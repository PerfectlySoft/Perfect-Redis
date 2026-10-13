import Foundation
import Testing
@testable import PerfectRedis

/// Arguments the client must reject before anything reaches the server.
@Suite struct ArgumentTests {
    /// A client connected to a fake server that never answers, so any command that was
    /// actually sent would hang rather than pass.
    func withSilentServer(_ body: (RedisClient) async throws -> Void) async throws {
        let server = try await FakeRedisServer([])
        let client = try await RedisClient.connect(withIdentifier: server.identifier)
        try await body(client)
        try? await client.close()
        await server.shutdown()
    }

    static let unrepresentable: [Double] = [.nan, .infinity, -.infinity, 1e300, -1e300, 9.3e15]

    @Test(arguments: unrepresentable)
    func durationsThatDontFitThrowInsteadOfTrapping(_ seconds: Double) async throws {
        try await withSilentServer { client in
            await #expect(throws: RedisClient.CommandError.self) {
                try await client.set(key: "k", value: .string("v"), expires: seconds)
            }
            await #expect(throws: RedisClient.CommandError.self) {
                try await client.expire(key: "k", seconds: seconds)
            }
            await #expect(throws: RedisClient.CommandError.self) {
                try await client.expireAt(key: "k", seconds: seconds)
            }
            await #expect(throws: RedisClient.CommandError.self) {
                try await client.clientPause(timeoutSeconds: seconds)
            }
        }
    }

    static let unpairedCommands: [(String, [String])] = [
        ("HELLO", ["3"]), ("hello", ["3", "AUTH", "u", "p"]), ("HELLO", ["4"]),
        ("SUBSCRIBE", ["c"]), ("psubscribe", ["p*"]), ("SSUBSCRIBE", ["s"]),
        ("UNSUBSCRIBE", []), ("PUNSUBSCRIBE", []), ("SUNSUBSCRIBE", []),
        ("MONITOR", []), ("SYNC", []), ("PSYNC", ["?", "-1"]),
        ("CLIENT", ["REPLY", "OFF"]), ("client", ["reply", "skip"]), ("CLIENT", ["REPLY"]),
        ("RESET", []), ("reset", []),
    ]

    @Test(arguments: unpairedCommands)
    func commandsThatBreakReplyPairingAreRefused(_ name: String, _ parameters: [String]) async throws {
        try await withSilentServer { client in
            await #expect(throws: RedisClient.CommandError.self) {
                try await client.sendCommandAsRESP(name: name, parameters: parameters)
            }
        }
    }

    static let pairedCommands: [(String, [String])] = [
        ("HELLO", []), ("HELLO", ["2"]), ("CLIENT", ["REPLY", "ON"]), ("CLIENT", ["ID"]), ("PING", []),
    ]

    @Test(arguments: pairedCommands)
    func otherCommandsAreSent(_ name: String, _ parameters: [String]) async throws {
        let server = try await FakeRedisServer(replies: "+OK\r\n")
        let client = try await RedisClient.connect(withIdentifier: server.identifier)
        let reply = try await client.sendCommandAsRESP(name: name, parameters: parameters)
        #expect(reply.isSimpleOK)
        try? await client.close()
        await server.shutdown()
    }

    @Test func byteArraysAreBinaryValues() {
        guard case .binary(let bytes) = [UInt8]([0, 13, 10, 255]).redisValue else {
            Issue.record("Expected .binary"); return
        }
        #expect(bytes == [0, 13, 10, 255])
        #expect("text".redisValue.string == "text")
    }
}
