import * as Y from "yjs";
import * as syncProtocol from "y-protocols/sync";
import { Awareness, encodeAwarenessUpdate, applyAwarenessUpdate, removeAwarenessStates } from "y-protocols/awareness";
import * as encoding from "lib0/encoding";
import * as decoding from "lib0/decoding";

// 와이어 프로토콜 메시지 타입. server.mts 와 값이 동일해야 한다(y-websocket 호환
// 프레이밍). stateless 상수라 각 번들에 인라인 — 자세한 이유는 server.mts 참고.
const MESSAGE_SYNC = 0;
const MESSAGE_AWARENESS = 1;

export type YdolabStatus = "connecting" | "connected" | "disconnected";

export interface YdolabProviderOptions {
  /** 협업 룸 WebSocket URL. 절대(`wss://host/editor/<room>`) 또는 same-origin
   *  상대 경로(`/editor/<room>`) 모두 허용 — 상대면 현재 origin 기준 ws(s)로 승격. */
  url: string;
  /** 동기화할 Y 문서. */
  doc: Y.Doc;
  /** 프레즌스용 awareness. 없으면 doc 으로 새로 생성. tiptap CollaborationCaret 과
   *  공유하려면 같은 인스턴스를 넘길 것. */
  awareness?: Awareness;
  /** URL 에 붙일 쿼리 파라미터(예: 단기 인증 토큰 `{ token }`). */
  params?: Record<string, string>;
  /** 생성 즉시 연결할지. 기본 true. */
  connect?: boolean;
  /** 비브라우저 환경(테스트 등)용 WebSocket 구현 주입. */
  WebSocketPolyfill?: typeof WebSocket;
  /** 초기 동기화(server step2 수신) 완료 시 1회. */
  onSynced?: () => void;
  /** 연결 상태 변화 콜백. */
  onStatus?: (status: YdolabStatus) => void;
}

const MAX_BACKOFF_MS = 30_000;

/**
 * 프레임워크·tiptap 무관 슬림 Y WebSocket provider. y-protocols(sync + awareness)
 * 로 ydolab/server(Durable Object)와 문서를 동기화한다. 지수 backoff 재연결 포함.
 *
 * tiptap 연결은 앱에서: `Collaboration.configure({ document: provider.doc })` +
 * `CollaborationCaret.configure({ provider })` 로 붙인다(ydolab 은 tiptap 을 모름).
 */
export class YdolabProvider {
  readonly doc: Y.Doc;
  readonly awareness: Awareness;
  synced = false;

  private readonly url: string;
  private readonly WS: typeof WebSocket;
  private readonly onSynced?: () => void;
  private readonly onStatus?: (status: YdolabStatus) => void;
  // 첫 sync 완료를 기다리는 소비자(예: 초기 시드)를 위한 대기열. markSynced 에서 flush.
  private syncedWaiters: Array<() => void> = [];

  private ws: WebSocket | null = null;
  private shouldConnect: boolean;
  private destroyed = false;
  private reconnectAttempts = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(options: YdolabProviderOptions) {
    this.doc = options.doc;
    this.awareness = options.awareness ?? new Awareness(this.doc);
    this.WS = options.WebSocketPolyfill ?? WebSocket;
    this.onSynced = options.onSynced;
    this.onStatus = options.onStatus;
    this.url = buildUrl(options.url, options.params);
    this.shouldConnect = options.connect ?? true;

    this.doc.on("update", this.handleDocUpdate);
    this.awareness.on("update", this.handleAwarenessUpdate);
    if (typeof window !== "undefined") {
      window.addEventListener("beforeunload", this.handleBeforeUnload);
    }

    if (this.shouldConnect) this.connect();
  }

  connect(): void {
    this.shouldConnect = true;
    if (this.ws || this.destroyed) return;
    this.openSocket();
  }

  disconnect(): void {
    this.shouldConnect = false;
    this.clearReconnect();
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }

  destroy(): void {
    if (this.destroyed) return;
    this.destroyed = true;
    this.shouldConnect = false;
    this.clearReconnect();

    // 내 프레즌스를 정리해 상대 화면에서 커서가 사라지게 한다.
    removeAwarenessStates(this.awareness, [this.doc.clientID], "destroy");
    this.doc.off("update", this.handleDocUpdate);
    this.awareness.off("update", this.handleAwarenessUpdate);
    if (typeof window !== "undefined") {
      window.removeEventListener("beforeunload", this.handleBeforeUnload);
    }
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }

  private openSocket(): void {
    this.onStatus?.("connecting");
    const ws = new this.WS(this.url);
    ws.binaryType = "arraybuffer";
    this.ws = ws;
    ws.addEventListener("open", this.handleOpen);
    ws.addEventListener("message", this.handleSocketMessage);
    ws.addEventListener("close", this.handleSocketClose);
    ws.addEventListener("error", () => ws.close());
  }

  private handleOpen = (): void => {
    this.reconnectAttempts = 0;
    this.onStatus?.("connected");

    // 1) 우리 state vector 전송(sync step1) → 서버가 부족분을 step2 로 채워준다.
    const syncEncoder = encoding.createEncoder();
    encoding.writeVarUint(syncEncoder, MESSAGE_SYNC);
    syncProtocol.writeSyncStep1(syncEncoder, this.doc);
    this.send(encoding.toUint8Array(syncEncoder));

    // 2) 로컬 프레즌스가 있으면 전송.
    if (this.awareness.getLocalState() !== null) {
      const awarenessEncoder = encoding.createEncoder();
      encoding.writeVarUint(awarenessEncoder, MESSAGE_AWARENESS);
      encoding.writeVarUint8Array(
        awarenessEncoder,
        encodeAwarenessUpdate(this.awareness, [this.doc.clientID])
      );
      this.send(encoding.toUint8Array(awarenessEncoder));
    }
  };

  private handleSocketMessage = (event: MessageEvent): void => {
    const message = new Uint8Array(event.data as ArrayBuffer);
    const decoder = decoding.createDecoder(message);
    const encoder = encoding.createEncoder();
    const messageType = decoding.readVarUint(decoder);

    switch (messageType) {
      case MESSAGE_SYNC: {
        encoding.writeVarUint(encoder, MESSAGE_SYNC);
        // origin=this → handleDocUpdate 가 서버발 업데이트를 되돌려보내지 않게 한다.
        const syncMessageType = syncProtocol.readSyncMessage(decoder, encoder, this.doc, this);
        if (encoding.length(encoder) > 1) {
          this.send(encoding.toUint8Array(encoder));
        }
        if (syncMessageType === syncProtocol.messageYjsSyncStep2 && !this.synced) {
          this.markSynced();
        }
        break;
      }
      case MESSAGE_AWARENESS: {
        applyAwarenessUpdate(this.awareness, decoding.readVarUint8Array(decoder), "remote");
        break;
      }
      default:
        break;
    }
  };

  private handleSocketClose = (): void => {
    this.ws = null;
    this.synced = false;
    this.onStatus?.("disconnected");

    // 내 것을 제외한 원격 프레즌스를 제거(끊긴 동안 stale 커서 방지). 재연결 시
    // 서버가 현재 상태를 다시 보내준다.
    const remoteIds = Array.from(this.awareness.getStates().keys()).filter(
      (id) => id !== this.doc.clientID
    );
    if (remoteIds.length > 0) {
      removeAwarenessStates(this.awareness, remoteIds, "disconnect");
    }

    if (!this.destroyed && this.shouldConnect) {
      this.scheduleReconnect();
    }
  };

  private handleDocUpdate = (update: Uint8Array, origin: unknown): void => {
    // 서버발 업데이트(origin===this)는 다시 서버로 보내지 않는다.
    if (origin === this) return;
    const encoder = encoding.createEncoder();
    encoding.writeVarUint(encoder, MESSAGE_SYNC);
    syncProtocol.writeUpdate(encoder, update);
    this.send(encoding.toUint8Array(encoder));
  };

  private handleAwarenessUpdate = (
    changes: { added: number[]; updated: number[]; removed: number[] },
    origin: unknown
  ): void => {
    // 원격발 프레즌스 변경은 에코하지 않는다 — 내 로컬 변경만 서버로 보낸다.
    if (origin === "remote") return;
    const changed = changes.added.concat(changes.updated, changes.removed);
    const encoder = encoding.createEncoder();
    encoding.writeVarUint(encoder, MESSAGE_AWARENESS);
    encoding.writeVarUint8Array(encoder, encodeAwarenessUpdate(this.awareness, changed));
    this.send(encoding.toUint8Array(encoder));
  };

  private handleBeforeUnload = (): void => {
    removeAwarenessStates(this.awareness, [this.doc.clientID], "unload");
  };

  /**
   * 첫 초기 동기화(server sync step2 수신) 완료를 기다린다. 이미 완료됐으면 즉시
   * resolve. 초기 콘텐츠 시드처럼 "서버가 보낼 기존 내용을 받은 뒤"에만 해야 하는
   * 작업에 쓴다 — sync 전에 문서를 건드리면 서버 상태를 덮어써 충돌한다.
   * (재연결로 synced 가 다시 false 가 돼도 이 Promise 는 최초 1회 완료로 resolve 된다.)
   */
  whenSynced(): Promise<void> {
    if (this.synced) return Promise.resolve();
    return new Promise((resolve) => {
      this.syncedWaiters.push(resolve);
    });
  }

  private markSynced(): void {
    this.synced = true;
    this.onSynced?.();
    const waiters = this.syncedWaiters;
    this.syncedWaiters = [];
    for (const resolve of waiters) resolve();
  }

  private scheduleReconnect(): void {
    const backoff = Math.min(MAX_BACKOFF_MS, 1000 * 2 ** this.reconnectAttempts);
    this.reconnectAttempts += 1;
    // 지터로 thundering herd 완화. Math.random 은 클라 런타임에서 정상 사용.
    const jitter = Math.floor(Math.random() * 500);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      if (!this.destroyed && this.shouldConnect) this.openSocket();
    }, backoff + jitter);
  }

  private clearReconnect(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }

  private send(data: Uint8Array): void {
    const ws = this.ws;
    if (ws && ws.readyState === ws.OPEN) {
      try {
        // 최신 lib.dom 은 send 에 `ArrayBufferView<ArrayBuffer>` 를 요구하는데 lib0
        // 는 `Uint8Array<ArrayBufferLike>` 를 반환한다. 런타임엔 항상 ArrayBuffer
        // 백킹이라 안전 — 타입만 좁혀 넘긴다.
        ws.send(data as unknown as ArrayBufferView<ArrayBuffer>);
      } catch {
        // 전송 실패 — close 핸들러가 재연결을 처리한다.
      }
    }
  }
}

/** 상대 경로면 현재 origin 기준으로 ws(s):// 절대 URL 로 승격하고 params 를 붙인다. */
function buildUrl(rawUrl: string, params?: Record<string, string>): string {
  let base: URL;
  if (/^wss?:\/\//i.test(rawUrl)) {
    base = new URL(rawUrl);
  } else if (/^https?:\/\//i.test(rawUrl)) {
    base = new URL(rawUrl);
    base.protocol = base.protocol === "https:" ? "wss:" : "ws:";
  } else {
    // 상대 경로 — same-origin 으로 해석.
    const origin =
      typeof location !== "undefined" ? location.origin : "http://localhost";
    base = new URL(rawUrl, origin);
    base.protocol = base.protocol === "https:" ? "wss:" : "ws:";
  }
  if (params) {
    for (const [key, value] of Object.entries(params)) {
      base.searchParams.set(key, value);
    }
  }
  return base.toString();
}
