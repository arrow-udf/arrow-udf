import { createClient } from "graphql-sse";

export async function createAsyncIterable() {
  let url = "{{SERVER_URL}}";
  console.log("using url", url);
  const client = createClient({
    // singleConnection: true, preferred for HTTP/1 enabled servers and subscription heavy apps
    url,
  });

  return client.iterate({ query: "subscription { greetings }" });
}
