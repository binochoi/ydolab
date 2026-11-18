import * as Y from "yjs";
import * as syncProtocol from "y-protocols/sync";
import * as awarenessProtocol from "y-protocols/awareness";
import * as encoding from "lib0/encoding";
import * as decoding from "lib0/decoding";

// 와이어 프로토콜 메시지 타입. y-websocket 과 동일한 프레이밍(각 메시지는 varUint
// 타입으로 시작하고 y-protocols sync/awareness 페이로드가 뒤따름)이라 표준 클라와도
// 상호운용된다. client.mts 와 값이 동일해야 한다(stateless 상수라 중복 무해 —
// 별도 모듈로 빼면 소비자 tsconfig 별 .mts 확장자 해석 문제가 생겨 인라인한다).
const MESSAGE_SYNC = 0;
const MESSAGE_AWARENESS = 1;

/**
 * Cloudflare Durable Object 런타임에서 우리가 실제로 쓰는 표면만 좁게 구조적으로
 * 선언한다. `@cloudflare/workers-types` 를 통째로 들이면 client.mts 의 DOM
 * WebSocket 전역과 충돌하므로, 필요한 메서드만 요구한다. 실제 DO 의
 * `DurableObjectState`/`WebSocket` 은 이 인터페이스를 구조적으로 만족한다.
 */
export interface YWebSocket {
  send(data: ArrayBuffer | ArrayBufferView | string): void;
  close(code?: number, reason?: string): void;
  /** 하이버네이션을 넘어 살아남는 per-connection 메타데이터 저장. */
  serializeAttachment(value: unknown): void;
  deserializeAttachment(): unknown;
  readonly readyState: number;
}

export interface YDurableObjectState {
  /** 하이버네이션 가능한 WebSocket 으로 승격. */
  acceptWebSocket(ws: YWebSocket, tags?: string[]): void;
  getWebSockets(tag?: string): YWebSocket[];
}

/**
 * 앱이 주입하는 영속 훅. ydolab 은 auth·task·HTML 을 모른다 — Y state 블롭만 다룬다.
 */
export interface YRoomHooks {
  /** 최초 연결/하이버네이션 웨이크 후 메모리 doc 이 비었을 때 1회. 영속된 full
   *  state(`Y.encodeStateAsUpdate`) 반환, 없으면 null. */
  load(): Promise<Uint8Array | null>;
  /** 디바운스 후 권위 상태를 영속. v1 은 full-state 블롭이라 항상 재구성 가능. */
  store(state: Uint8Array): Promise<void>;
  /** 선택. 2차 표현 파생용(예: 앱에서 Y→HTML→DB). doc 을 넘겨받아 앱이 직렬화. */
  snapshot?(doc: Y.Doc): Promise<void>;
}

export interface YRoomOptions {
  /** store() 디바운스(ms). 기본 3000. */
  persistDebounceMs?: number;
}

/** YRoom 생성 입력 — 영속 훅 + 선택 옵션. */
export type YRoomDeps = YRoomOptions & { hooks: YRoomHooks };

const SYNC_STEP1 = syncProtocol.messageYjsSyncStep1;

/** 하이버네이션을 넘겨 보존해야 하는 per-connection 메타. */
interface ConnAttachment {
  readOnly: boolean;
}

function isWebSocketOrigin(origin: unknown): origin is YWebSocket {
  return (
    typeof origin === "object" &&
    origin !== null &&
    typeof (origin as { send?: unknown }).send === "function"
  );
}

/**
 * Durable Object 안에서 단일 Y.Doc 을 소유하고, 접속한 모든 WebSocket 과
 * y-protocols(sync + awareness) 로 동기화하는 delegate 헬퍼.
 *
 * DO 를 상속하지 않는다 — DO 는 하이버네이션 핸들러(webSocketMessage/Close/Error)
 * 를 자체 소유해야 하고, 앱에 따라 Agents SDK 등 다른 베이스를 상속할 수 있기
 * 때문이다. 앱의 DO 가 이 클래스를 인스턴스로 들고 각 핸들러를 위임한다.
 */
export class YSyncRoom {
  readonly doc: Y.Doc;
  readonly awareness: awarenessProtocol.Awareness;

  private readonly state: YDurableObjectState;
  private readonly hooks: YRoomHooks;
  private readonly persistDebounceMs: number;

  /** 이번 wake 의 재수화 작업. 동시에 들어온 연결이 같은 작업을 함께 기다린다. */
  private loading: Promise<void> | null = null;
  private persistTimer: ReturnType<typeof setTimeout> | null = null;

  /** 각 연결이 소유(브로드캐스트)하는 awareness clientID 집합. 하이버네이션 시
   *  소실되지만, 클라가 주기적으로 재브로드캐스트하므로 자가 치유된다. */
  private readonly controlled = new Map<YWebSocket, Set<number>>();

  constructor(state: YDurableObjectState, deps: YRoomDeps) {
    const { hooks, persistDebounceMs = 3000 } = deps;
    this.state = state;
    this.hooks = hooks;
    this.persistDebounceMs = persistDebounceMs;

    this.doc = new Y.Doc();
    this.awareness = new awarenessProtocol.Awareness(this.doc);
    // 서버 자신은 프레즌스를 갖지 않는다 — 순수 릴레이.
    this.awareness.setLocalState(null);

    this.doc.on("update", this.handleDocUpdate);
    this.awareness.on("update", this.handleAwarenessUpdate);
  }

  /**
   * auth 통과 후 DO.fetch 에서 호출. 서버측 소켓을 하이버네이션으로 accept 하고
   * 초기 sync(step1) + 현재 awareness 상태를 클라에 보낸다.
   */
  async accept(server: YWebSocket, ctx: { readOnly: boolean }): Promise<void> {
    await this.ensureLoaded();
    this.state.acceptWebSocket(server);
    server.serializeAttachment({ readOnly: ctx.readOnly } satisfies ConnAttachment);

    // 1) sync step1 — 우리 state vector 를 보내 클라가 부족분(step2)을 채우게 한다.
    const syncEncoder = encoding.createEncoder();
    encoding.writeVarUint(syncEncoder, MESSAGE_SYNC);
    syncProtocol.writeSyncStep1(syncEncoder, this.doc);
    server.send(encoding.toUint8Array(syncEncoder));

    // 2) 현재 알려진 awareness 상태를 새 연결에 전달.
    const states = this.awareness.getStates();
    if (states.size > 0) {
      const awarenessEncoder = encoding.createEncoder();
      encoding.writeVarUint(awarenessEncoder, MESSAGE_AWARENESS);
      encoding.writeVarUint8Array(
        awarenessEncoder,
        awarenessProtocol.encodeAwarenessUpdate(this.awareness, Array.from(states.keys()))
      );
      server.send(encoding.toUint8Array(awarenessEncoder));
    }
  }

  /** DO.webSocketMessage 위임. */
  async onMessage(ws: YWebSocket, data: ArrayBuffer | string): Promise<void> {
    if (typeof data === "string") return; // 바이너리 프로토콜만 취급
    await this.ensureLoaded();

    const message = new Uint8Array(data);
    const decoder = decoding.createDecoder(message);
    const messageType = decoding.readVarUint(decoder);

    const attachment = ws.deserializeAttachment() as ConnAttachment | null;
    const readOnly = attachment?.readOnly ?? false;

    switch (messageType) {
      case MESSAGE_SYNC: {
        const encoder = encoding.createEncoder();
        encoding.writeVarUint(encoder, MESSAGE_SYNC);
        if (readOnly) {
          // read-only 연결: step1(state vector 요청)에만 step2 로 응답하고,
          // step2/update(문서 변경)는 무시한다 — 쓰기 강제는 ydolab 이 담당.
          const syncMessageType = decoding.readVarUint(decoder);
          if (syncMessageType === SYNC_STEP1) {
            const stateVector = decoding.readVarUint8Array(decoder);
            syncProtocol.writeSyncStep2(encoder, this.doc, stateVector);
          }
        } else {
          // origin=ws 로 적용 → handleDocUpdate 가 나머지 연결로만 브로드캐스트.
          syncProtocol.readSyncMessage(decoder, encoder, this.doc, ws);
        }
        // 헤더(varUint) 외 실제 페이로드가 있으면 응답.
        if (encoding.length(encoder) > 1) {
          ws.send(encoding.toUint8Array(encoder));
        }
        break;
      }
      case MESSAGE_AWARENESS: {
        awarenessProtocol.applyAwarenessUpdate(
          this.awareness,
          decoding.readVarUint8Array(decoder),
          ws // origin → handleAwarenessUpdate 가 소유 clientID 를 이 연결에 귀속
        );
        break;
      }
      default:
        break;
    }
  }

  /** DO.webSocketClose / webSocketError 위임. */
  async onClose(ws: YWebSocket): Promise<void> {
    const ids = this.controlled.get(ws);
    if (ids && ids.size > 0) {
      // 끊긴 연결이 들고 있던 프레즌스를 즉시 제거 → 나머지 연결에 브로드캐스트.
      awarenessProtocol.removeAwarenessStates(this.awareness, Array.from(ids), "conn-closed");
    }
    this.controlled.delete(ws);

    // 마지막 연결이 나가면 하이버네이션 전에 확정 플러시(디바운스 타이머 유실 대비).
    const remaining = this.state.getWebSockets().filter((s) => s !== ws);
    if (remaining.length === 0) {
      await this.flushPersist();
    }
  }

  private ensureLoaded(): Promise<void> {
    this.loading ??= this.load().catch((error) => {
      // 실패하면 다음 연결이 다시 읽게 한다.
      this.loading = null;
      throw error;
    });
    return this.loading;
  }

  private async load(): Promise<void> {
    const stored = await this.hooks.load();
    if (stored) {
      // origin="load" — handleDocUpdate 가 브로드캐스트/영속을 건너뛰게 한다.
      Y.applyUpdate(this.doc, stored, "load");
    }
  }

  private handleDocUpdate = (update: Uint8Array, origin: unknown): void => {
    // 재수화(load)는 브로드캐스트할 새 연결도 없고 다시 저장할 필요도 없다.
    if (origin === "load") return;

    const encoder = encoding.createEncoder();
    encoding.writeVarUint(encoder, MESSAGE_SYNC);
    syncProtocol.writeUpdate(encoder, update);
    this.broadcast(encoding.toUint8Array(encoder), origin);
    this.schedulePersist();
  };

  private handleAwarenessUpdate = (
    changes: { added: number[]; updated: number[]; removed: number[] },
    origin: unknown
  ): void => {
    const { added, updated, removed } = changes;

    // 이 변경을 유발한 연결에 소유 clientID 를 귀속(끊길 때 정리하려고).
    if (isWebSocketOrigin(origin)) {
      let set = this.controlled.get(origin);
      if (!set) {
        set = new Set<number>();
        this.controlled.set(origin, set);
      }
      for (const id of added) set.add(id);
      for (const id of updated) set.add(id);
      for (const id of removed) set.delete(id);
    }

    const changed = added.concat(updated, removed);
    const encoder = encoding.createEncoder();
    encoding.writeVarUint(encoder, MESSAGE_AWARENESS);
    encoding.writeVarUint8Array(
      encoder,
      awarenessProtocol.encodeAwarenessUpdate(this.awareness, changed)
    );
    this.broadcast(encoding.toUint8Array(encoder), origin);
  };

  private broadcast(payload: Uint8Array, exclude: unknown): void {
    for (const ws of this.state.getWebSockets()) {
      if (ws === exclude) continue;
      try {
        ws.send(payload);
      } catch {
        // 닫히는 중인 소켓 — 무시(close 핸들러가 정리한다).
      }
    }
  }

  private schedulePersist(): void {
    if (this.persistTimer) return; // 이미 예약됨(leading debounce)
    this.persistTimer = setTimeout(() => {
      this.persistTimer = null;
      void this.flushPersist();
    }, this.persistDebounceMs);
  }

  private async flushPersist(): Promise<void> {
    if (this.persistTimer) {
      clearTimeout(this.persistTimer);
      this.persistTimer = null;
    }
    const state = Y.encodeStateAsUpdate(this.doc);
    await this.hooks.store(state);
    if (this.hooks.snapshot) {
      await this.hooks.snapshot(this.doc);
    }
  }
}
