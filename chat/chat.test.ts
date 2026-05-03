import { describe, expect, test } from "vitest";
import { chat } from "~encore/clients";

async function recvChat(stream: Awaited<ReturnType<typeof chat.chat>>) {
  while (true) {
    const evt = await stream.recv();
    if (evt.type === "chat") return evt;
  }
}

async function recvPresence(stream: Awaited<ReturnType<typeof chat.chat>>) {
  while (true) {
    const evt = await stream.recv();
    if (evt.type === "presence") return evt;
  }
}

describe("chat", () => {
  test("should get back own message with correct parameters", async () => {
    const stream = await chat.chat({ id: "user-id", username: "foo" });

    await stream.send({ username: "foo", msg: "hello" });

    const { msg, userID, username } = await recvChat(stream);
    expect(userID).toBe("user-id");
    expect(msg).toBe("hello");
    expect(username).toBe("foo");
  });

  test("should get other users message", async () => {
    const stream1 = await chat.chat({ id: "user-1", username: "foo" });
    const stream2 = await chat.chat({ id: "user-2", username: "bar" });
    const stream3 = await chat.chat({ id: "user-3", username: "baz" });

    await stream1.send({ username: "foo", msg: "hello" });

    const stream2Results = await recvChat(stream2);
    const stream3Results = await recvChat(stream3);

    expect(stream2Results.userID).toBe("user-1");
    expect(stream3Results.userID).toBe("user-1");
  });

  test("should broadcast presence on connect and disconnect", async () => {
    const stream1 = await chat.chat({ id: "presence-1", username: "alice" });

    const initial = (await recvPresence(stream1)).users ?? [];
    expect(initial.find((u) => u.id === "presence-1")?.username).toBe("alice");

    const stream2 = await chat.chat({ id: "presence-2", username: "bob" });

    let latest = (await recvPresence(stream1)).users ?? [];
    while (!latest.some((u) => u.id === "presence-2")) {
      latest = (await recvPresence(stream1)).users ?? [];
    }
    expect(latest.find((u) => u.id === "presence-2")?.username).toBe("bob");

    await stream2.close();

    let afterLeave = (await recvPresence(stream1)).users ?? [];
    while (afterLeave.some((u) => u.id === "presence-2")) {
      afterLeave = (await recvPresence(stream1)).users ?? [];
    }
    expect(afterLeave.some((u) => u.id === "presence-1")).toBe(true);
    expect(afterLeave.some((u) => u.id === "presence-2")).toBe(false);
  });
});
