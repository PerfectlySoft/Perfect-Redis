# Perfect-Redis [简体中文](README.zh_CN.md)

<p align="center">
    <a href="https://developer.apple.com/swift/" target="_blank">
        <img src="https://img.shields.io/badge/Swift-6.2-orange.svg?style=flat" alt="Swift 6.2">
    </a>
    <a href="https://developer.apple.com/swift/" target="_blank">
        <img src="https://img.shields.io/badge/Platforms-macOS%2012%2B-lightgray.svg?style=flat" alt="Platforms macOS 12+">
    </a>
    <a href="http://perfect.org/licensing.html" target="_blank">
        <img src="https://img.shields.io/badge/License-Apache-lightgrey.svg?style=flat" alt="License Apache">
    </a>
</p>

Async/await Redis client support for Perfect, built on [swift-server/RediStack](https://github.com/swift-server/RediStack).

## Status

This is a Swift 6 rewrite of the original PerfectlySoft `Perfect-Redis` package. The old synchronous/completion-handler API has been **deliberately removed** — `Sources/PerfectRedis/RedisClientSync.swift` is now a one-line stub confirming this. The public API is a Swift `actor` (`RedisClient`) with a fully `async throws` surface, layered on top of RediStack's NIO-based Redis client rather than a hand-rolled RESP implementation.

This package is a real, tested dependency: [Perfect-Session](https://github.com/PerfectlySoft/Perfect-Session)'s `PerfectSessionRedis` module imports it directly and implements a full `SessionDriver` backend with it.

The pre-Swift-6 version of this package is preserved on the [`legacy`](../../tree/legacy) branch.

`Package.swift` declares `swift-tools-version: 6.2` and `platforms: [.macOS(.v12)]`, with `.swiftLanguageMode(.v6)` (full Swift 6 strict concurrency) on both the library and test targets. **No Linux, iOS, tvOS, or watchOS platforms are declared** — this is macOS-only today.

## Quick Start

Connect a Redis client with defaults (localhost, default port):

```swift
import PerfectRedis

let client = try await RedisClient.connect(withIdentifier: RedisClientIdentifier())
```

Ping the server:

```swift
let response = try await client.ping()
guard case .simpleString(let s) = response else {
    return
}
assert(s == "PONG", "Unexpected response \(response)")
```

Set/get a value:

```swift
let (key, value) = ("mykey", "myvalue")
var response = try await client.set(key: key, value: .string(value))
guard case .simpleString = response else {
    // handle error
    return
}
response = try await client.get(key: key)
guard case .bulkString = response else {
    // handle error
    return
}
let s = response.string
assert(s == value, "Unexpected response \(response)")
```

Pub/sub with two clients (no callback nesting — every call is plain `async throws`):

```swift
let client1 = try await RedisClient.connect(withIdentifier: RedisClientIdentifier())
let client2 = try await RedisClient.connect(withIdentifier: RedisClientIdentifier())

try await client1.subscribe(channels: ["foo"])
try await client2.publish(channel: "foo", message: .string("Hello!"))
```

`RedisClient` is a Swift `actor`; `RedisResponse` and the other public value types (`RedisClientIdentifier`, `RedisValue`, and the `RedisHash`/`RedisList`/`RedisSet` wrappers) are `Sendable`. There is no synchronous or completion-handler API anywhere in the module.

## Building

Add it as a dependency in your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/PerfectlySoft/Perfect-Redis.git", branch: "main"),
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

`PerfectRedis` in turn depends on [`RediStack`](https://github.com/swift-server/RediStack) (`from: "1.6.0"`), resolved automatically from GitHub — no additional sibling checkout is required for that dependency.

## Further Information
For more information on the Perfect project, please visit [perfect.org](http://perfect.org).
