import { defineBuildConfig } from "obuild/config";

export default defineBuildConfig({
  // server(=DO 런타임) 와 client(=브라우저 provider) 를 각각 별도 엔트리로 번들한다.
  // yjs / y-protocols 는 peerDependencies 라 external 로 빠지고(단일 인스턴스 보장),
  // lib0(순수 stateless encoding) 만 각 번들에 인라인된다.
  entries: [{ type: "bundle", input: ["src/server.mts", "src/client.mts"] }],
});
