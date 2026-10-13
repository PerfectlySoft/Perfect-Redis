# Perfect-Redis

<p align="center">
    <a href="https://developer.apple.com/swift/" target="_blank">
        <img src="https://img.shields.io/badge/Swift-6.2%2B-orange.svg?style=flat" alt="Swift 6.2+">
    </a>
    <a href="https://developer.apple.com/swift/" target="_blank">
        <img src="https://img.shields.io/badge/Platforms-macOS%2012%2B%20%7C%20iOS%2015%2B%20%7C%20Linux-lightgray.svg?style=flat" alt="Platforms macOS 12+ | iOS 15+ | Linux">
    </a>
    <a href="http://perfect.org/licensing.html" target="_blank">
        <img src="https://img.shields.io/badge/License-Apache-lightgrey.svg?style=flat" alt="License Apache">
    </a>
</p>

An async/await Redis client for Perfect, built on [swift-server/RediStack](https://github.com/swift-server/RediStack).

`RedisClient` is an `actor` with an `async throws` API. Every command and argument goes to the server as a RESP bulk string, so keys and values can hold any bytes, CR and LF included. [Perfect-Session](https://github.com/PerfectlySoft/Perfect-Session)'s `PerfectSessionRedis` module stores its sessions with it.

## Requirements

- Swift 6.2 or newer (`swift-tools-version: 6.2`, Swift 6 language mode).
- macOS 12+, iOS 15+, or Linux. CI builds and tests on Ubuntu 24.04 with Swift 6.2 and 6.4, macOS with Xcode 26.0.1 and 26.6, and the iOS Simulator.
- A Redis server that speaks RESP2. The live tests run against Redis 7.
- One external dependency, RediStack (`from: "1.6.0"`), which brings in SwiftNIO; SwiftPM resolves both.

## Installation

```swift
dependencies: [
    .package(url: "https://github.com/PerfectlySoft/Perfect-Redis.git", from: "4.0.0"),
],
targets: [
    .target(
        name: "YourTarget",
        dependencies: [
            .product(name: "PerfectRedis", package: "Perfect-Redis"),
        ]
    ),
]
```

## Quick start

```swift
import PerfectRedis

// Defaults to 127.0.0.1:6379 with no password.
let client = try await RedisClient.connect(withIdentifier: RedisClientIdentifier())

let pong = try await client.ping()            // .simpleString("PONG")

_ = try await client.set(key: "greeting", value: .string("hello"), expires: 60)
let reply = try await client.get(key: "greeting")
print(reply.string ?? "missing")              // "hello"
print(try await client.get(key: "nope").isNil) // true

try await client.close()
```

Connect to another server, with a password and a per-command timeout:

```swift
let id = RedisClientIdentifier(withHost: "redis.internal", port: 6379,
                               password: "secret", commandTimeout: 5)
let client = try await RedisClient.connect(withIdentifier: id)
```

Replies come back as `RedisResponse`: `.simpleString`, `.bulkString([UInt8]?)` (nil for a missing value, `[]` for an empty one), `.integer`, `.array`, or `.error` for an error nested inside an array (as `EXEC` returns). The `string`, `integer`, `isNil` and `isSimpleOK` accessors cover the common cases. `RedisHash`, `RedisList` and `RedisSet` wrap a client and a key for the hash, list and set commands, and `sendCommandAsRESP(name:parameters:)` sends any other command.

### Errors

- An error reply to a command (such as `WRONGTYPE`) is thrown as RediStack's `RedisError`, whose `message` starts with `(Redis) `.
- Problems the client catches itself are thrown as `RedisClient.CommandError`: arguments it can't send (a non-finite duration, say), commands `sendCommandAsRESP` refuses, a reply with impossible framing, or a `commandTimeout` running out.
- A closed or dropped connection makes commands throw RediStack's `RedisClientError.connectionClosed`. A client doesn't reconnect; connect a new one.

### Pub/sub

Messages arrive on the subscribing client's `messages` stream:

```swift
let subscriber = try await RedisClient.connect()
let publisher = try await RedisClient.connect()

try await subscriber.subscribe(channels: ["news"])
try await subscriber.subscribe(patterns: ["weather.*"])

Task {
    for await message in subscriber.messages {
        print(message.channel, message.message.string ?? "")
    }
}

_ = try await publisher.publish(channel: "news", message: .string("Hello!"))
```

While a client has subscriptions, Redis only accepts subscription commands, `PING`, `QUIT` and `RESET` on it, so publish and run other commands from a second client. `unsubscribe(channels: [])` and `unsubscribe(patterns: [])` drop every subscription. The stream ends when the client is closed.

### Transactions

```swift
let results = try await client.multi {
    _ = try await client.set(key: "a", value: .string("1"))
    _ = try await client.increment(key: "a")
}   // .array([.simpleString("OK"), .integer(2)])
```

While `multi` runs, commands that other tasks send on the same client wait until `EXEC` or `DISCARD`, so they aren't queued into the transaction. If the body throws, the transaction is discarded and the error rethrown. `multiBegin()`, `multiExec()` and `multiDiscard()` are also available, but called by hand they have no such protection.

### Timeouts, cancellation and blocking commands

With `commandTimeout` set, a command that gets no reply in time throws `CommandError` and the connection is closed. The default is no timeout. Cancelling a task while its command is waiting for a reply also closes the connection, and the command throws `CancellationError`. The client closes the connection in both cases because the command has already gone to the server, and abandoning just its reply would hand the next reply to the wrong command.

Commands share one connection and are pipelined in order, so a blocking command such as `listPopFirstBlocking(keys:timeout:)` holds up everything sent after it on the same client. Give blocking commands their own client, and keep their server-side timeout shorter than `commandTimeout`.

## Limitations

- **RESP2 only.** `sendCommandAsRESP` refuses `HELLO` with any protocol version except 2. It also refuses commands that would break the one-reply-per-command pairing: the `SUBSCRIBE` family (use `subscribe`), `MONITOR`, `SYNC`, `PSYNC`, and `CLIENT REPLY OFF`/`SKIP`.
- **No TLS.** The connection is plain TCP. Use a TLS proxy or tunnel to reach a TLS-only server.
- **Password only.** `AUTH` is sent with the password alone, so it authenticates as the `default` ACL user; there is no username option.
- **No reconnection or pooling.** Each `RedisClient` is one connection.
- **Logging.** RediStack logs through swift-log under the label `RediStack.RedisConnection`. With the default handler, every failed command prints an `error` line; at `debug` level, every command and its arguments are logged, including `AUTH` and its password.
- **RediStack is deprecated upstream**: its maintainers point new users to valkey-swift. RediStack 1.6 still builds and works on Swift 6.2 and 6.4.

## Running the tests

```sh
swift test                      # reply, framing, argument and connection tests; live suites skip

# The live suites FLUSHALL, so point them at a throwaway server.
docker run -d --rm -p 6379:6379 redis:7
REDIS_TESTS=1 swift test
REDIS_TESTS=1 REDIS_HOST=redis.test REDIS_PORT=6380 REDIS_REQUIRE_LIVE=1 swift test
```

`REDIS_HOST` defaults to 127.0.0.1 and `REDIS_PORT` to 6379. With `REDIS_REQUIRE_LIVE=1`, a missing or unreachable server fails the run instead of skipping the live suites.

## Migrating from 3.x

4.0.0 is a rewrite. The API keeps the 3.x command names and `RedisResponse` shape, but everything is `async`:

- `RedisClient.getClient(withIdentifier:)` (callback or throwing form) and `releaseClient(_:)` → `try await RedisClient.connect(withIdentifier:)` and `try await client.close()`.
- Completion handlers and the blocking `throws` methods → `try await`. For example, `client.ping { response in … }` or `try client.ping()` becomes `try await client.ping()`.
- Error replies → thrown. 3.x returned them as `.error(type:msg:)`; 4.0 throws `RedisError` (see [Errors](#errors)). `.error` now appears only inside arrays, such as `EXEC` results.
- `sendCommand(name:parameters:callback:)` → `sendCommandAsRESP(name:parameters:)`.
- `readPublished(timeoutSeconds:callback:)` → `for await message in client.messages`. `subscribe` and `unsubscribe` now return nothing.
- `netTimeout` → `RedisClientIdentifier(…, commandTimeout:)`. `RedisClientIdentifier`'s `netGenerator` is gone.
- `clientReply(type:)` is removed. Turning replies off would break reply pairing.
- `[UInt8]` is a `RedisValueRepresentable`; other `ROctal` arrays aren't.
- `RedisClient` is an `actor`, so share one client between tasks freely. Commands from different tasks are pipelined on its single connection.

## Legacy

The pre-Swift-6 code is preserved on the [`legacy`](https://github.com/PerfectlySoft/Perfect-Redis/tree/legacy) branch, which points at the same commit as the 3.4.1 tag. Stay on 3.4.1 if you can't move to Swift 6.2 yet.

## Further information

For more information on the Perfect project, visit [perfect.org](http://perfect.org).
