# ydolab

Y.js 문서를 Cloudflare **Durable Object 위에서** 실시간 동기화하는, **프레임워크·비즈니스로직 무관**
라이브러리. 서버(DO 위임 헬퍼)와 클라이언트(슬림 WS provider)를 제공한다.

## 왜 생겼나

협업 에디터는 여러 접속자의 편집을 CRDT(Y.js)로 합치고 프레즌스(커서)를 브로드캐스트해야 한다.
기성 `y-websocket` 서버는 Node 상주 프로세스를 전제해 **Cloudflare Workers/Durable Object 모델과 안 맞는다**
(DO 는 하이버네이션 가능한 WebSocket, per-room 단일 인스턴스, storage 훅이 핵심). 그래서:

- **서버**: 표준 `y-protocols`(sync + awareness) 프레이밍을 그대로 쓰되, **DO 를 상속하지 않고 위임**하는
  `YSyncRoom` 헬퍼. auth·HTML·DB 를 전혀 모르고 **Y state 블롭만** 다룬다 — 영속은 훅으로 주입받는다.
- **클라이언트**: tiptap·프레임워크 무관 **슬림 WS provider** `YdolabProvider`(지수 backoff 재연결 내장).

와이어 프레이밍이 y-websocket 호환이라 표준 클라와도 상호운용된다.

## 왜 DO 를 상속하지 않고 "위임"하나

DO 는 하이버네이션 핸들러(`webSocketMessage`/`Close`/`Error`)를 **자기가 소유**해야 하고, 앱에 따라
Agents SDK 등 다른 베이스를 상속할 수도 있다. 그래서 `YSyncRoom` 은 DO 가 **인스턴스로 들고** 각 핸들러를
위임하는 구조다. `@cloudflare/workers-types` 도 통째로 안 들인다(클라 DOM `WebSocket` 전역과 충돌) —
필요한 메서드만 구조적 인터페이스(`YWebSocket`/`YDurableObjectState`)로 요구한다.

## 진입점

| import | 런타임 | 내용 |
| --- | --- | --- |
| `ydolab/server` | Durable Object | `YSyncRoom` — 단일 `Y.Doc` 소유 + 접속 소켓 전부와 sync/awareness |
| `ydolab/client` | 브라우저 | `YdolabProvider` — 슬림 WS provider(재연결·프레즌스 정리) |

## 설치

```sh
pnpm add github:binochoi/ydolab#v0.0.1 yjs y-protocols
```

GitHub 에서 받으면 설치할 때 `prepare` 스크립트가 `dist` 를 빌드한다. pnpm 은 허락한 패키지만 빌드하므로
`pnpm-workspace.yaml` 에 다음을 넣어야 한다.

```yaml
onlyBuiltDependencies:
  - ydolab
```

## 사용법

### 서버 (Durable Object 안)

```ts
import { YSyncRoom } from "ydolab/server";

export class EditorRoom {
  private room: YSyncRoom;
  constructor(state: DurableObjectState, env: Env) {
    this.room = new YSyncRoom(
      state,
      {
        load: () => this.readState(),          // 영속된 full-state 블롭 or null
        store: (state) => this.writeState(state),
        snapshot: async (doc) => this.deriveHtml(doc), // 선택: Y→HTML→DB 등 2차 표현
      },
      { persistDebounceMs: 3000 },
    );
  }
  // 하이버네이션 핸들러를 room 에 위임
  async fetch(req: Request) { /* auth 후 */ await this.room.accept(server, { readOnly }); }
  webSocketMessage(ws, data) { return this.room.onMessage(ws, data); }
  webSocketClose(ws)         { return this.room.onClose(ws); }
  webSocketError(ws)         { return this.room.onClose(ws); }
}
```

`YRoomHooks`: `load()`(웨이크 후 최초 1회 재수화) / `store(state)`(디바운스 후 영속) / `snapshot?(doc)`(선택).
`readOnly` 연결은 문서 변경을 무시하고 step1 요청에만 응답한다 — 쓰기 권한 강제는 ydolab 이 담당.

### 클라이언트

```ts
import { YdolabProvider } from "ydolab/client";
import * as Y from "yjs";

const doc = new Y.Doc();
const provider = new YdolabProvider({
  url: "/editor/room-123",             // 상대 경로면 same-origin ws(s) 로 승격
  doc,
  params: { token },                   // 단기 인증 토큰 등
  onStatus: (s) => setStatus(s),
});

await provider.whenSynced();           // 서버의 기존 내용을 받은 뒤에만 초기 시드
// tiptap 연결(ydolab 은 tiptap 을 모름):
//   Collaboration.configure({ document: provider.doc })
//   CollaborationCaret.configure({ provider })

provider.destroy();                    // 내 커서 정리 + 소켓 종료
```

> **`whenSynced()` 전에 문서를 건드리지 말 것.** 초기 sync(server step2 수신) 전에 쓰면 서버 상태를
> 덮어써 충돌한다. 초기 콘텐츠 시드는 반드시 `await whenSynced()` 후에.

## 동작 메모

- **영속은 full-state 블롭**(v1) — `Y.encodeStateAsUpdate` 라 항상 재구성 가능. `store` 는 디바운스(기본 3s),
  마지막 연결이 나갈 때 확정 플러시.
- 프레즌스(awareness)는 연결별 소유 clientID 를 추적해, 끊기면 즉시 제거 → 상대 화면에서 stale 커서 방지.
  하이버네이션으로 소실돼도 클라가 주기적으로 재브로드캐스트해 자가 치유.
- **의존성**: `yjs`/`y-protocols` 는 peer(소비자와 단일 인스턴스 공유), `lib0` 는 direct.
- 와이어 상수(`MESSAGE_SYNC`/`MESSAGE_AWARENESS`)는 server/client 에 각각 인라인 — 별도 모듈로 빼면
  소비자 tsconfig 별 `.mts` 확장자 해석 문제가 생겨서(값은 동일해야 함).
