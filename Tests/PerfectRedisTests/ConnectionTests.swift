import Foundation
import Testing
@testable import PerfectRedis

/// Connection-level failures, against a scripted fake server.
@Suite struct ConnectionTests {
    func elapsed(_ body: () async throws -> Void) async rethrows -> Double {
        let start = Date()
        try await body()
        return Date().timeIntervalSince(start)
    }

    @Test func unansweredCommandTimesOut() async throws {
        let server = try await FakeRedisServer([.silence, .reply(Array("+PONG\r\n".utf8))])
        let id = RedisClientIdentifier(withHost: "127.0.0.1", port: server.port, commandTimeout: 0.3)
        let client = try await RedisClient.connect(withIdentifier: id)
        let seconds = await elapsed {
            await #expect(throws: RedisClient.CommandError.self) { try await client.ping() }
        }
        #expect(seconds < 5)
        // The connection is closed rather than left one reply out of step.
        await #expect(throws: (any Error).self) { try await client.ping() }
        await server.shutdown()
    }

    @Test func answeredCommandsAreUnaffectedByTheTimeout() async throws {
        let server = try await FakeRedisServer(replies: "+PONG\r\n", "+PONG\r\n")
        let id = RedisClientIdentifier(withHost: "127.0.0.1", port: server.port, commandTimeout: 0.2)
        let client = try await RedisClient.connect(withIdentifier: id)
        #expect(try await client.ping().string == "PONG")
        try await Task.sleep(nanoseconds: 400_000_000)  // past the first command's deadline
        #expect(try await client.ping().string == "PONG")
        try await client.close()
        await server.shutdown()
    }

    @Test func cancellingAWaitingCommandReturnsPromptly() async throws {
        let server = try await FakeRedisServer([.silence])
        let client = try await RedisClient.connect(withIdentifier: server.identifier)
        let waiting = Task { try await client.ping() }
        try await Task.sleep(nanoseconds: 200_000_000)
        let seconds = await elapsed {
            waiting.cancel()
            let result = await waiting.result
            #expect(throws: CancellationError.self) { try result.get() }
        }
        #expect(seconds < 5)
        await server.shutdown()
    }

    @Test(arguments: [0, -1, Double.nan, .infinity, 1e300])
    func invalidTimeoutIsRejected(_ seconds: Double) async throws {
        let server = try await FakeRedisServer([])
        let id = RedisClientIdentifier(withHost: "127.0.0.1", port: server.port, commandTimeout: seconds)
        await #expect(throws: RedisClient.CommandError.self) { try await RedisClient.connect(withIdentifier: id) }
        await server.shutdown()
    }

    @Test func serverClosingMidReplyFailsTheCommand() async throws {
        let server = try await FakeRedisServer([.replyThenClose(Array("$10\r\nabc".utf8))])
        let client = try await RedisClient.connect(withIdentifier: server.identifier)
        await #expect(throws: (any Error).self) { try await client.ping() }
        await #expect(throws: (any Error).self) { try await client.ping() }
        await server.shutdown()
    }

    @Test func commandsAfterCloseThrow() async throws {
        let server = try await FakeRedisServer([])
        let client = try await RedisClient.connect(withIdentifier: server.identifier)
        try await client.close()
        await #expect(throws: (any Error).self) { try await client.ping() }
        try await client.close()  // closing twice is harmless
        await server.shutdown()
    }

    @Test func failedAuthClosesTheSocket() async throws {
        let server = try await FakeRedisServer(replies: "-WRONGPASS invalid username-password pair\r\n")
        let id = RedisClientIdentifier(withHost: "127.0.0.1", port: server.port, password: "nope")
        await #expect(throws: (any Error).self) { try await RedisClient.connect(withIdentifier: id) }
        var waited = 0
        while server.closedConnections == 0, waited < 50 {
            try await Task.sleep(nanoseconds: 100_000_000)
            waited += 1
        }
        #expect(server.closedConnections == 1, "The rejected connection was left open")
        await server.shutdown()
    }

    /// RediStack's graceful close waits for every outstanding reply, so a pending BLPOP 0 or
    /// a silent server made close() hang forever.
    @Test func closeDoesNotWaitForeverForOutstandingReplies() async throws {
        let server = try await FakeRedisServer([.silence])
        let client = try await RedisClient.connect(withIdentifier: server.identifier)
        let pending = Task { try await client.ping() }
        try await Task.sleep(nanoseconds: 200_000_000)
        let seconds = try await elapsed { try await client.close() }
        #expect(seconds < 5)
        await #expect(throws: (any Error).self) { try await pending.value }
        await server.shutdown()
    }

    /// The stream used to end only on close() or deinit, so iterating it hung forever after
    /// the server dropped the connection or a timeout closed it.
    @Test(arguments: [false, true])
    func messagesEndWhenTheConnectionDrops(byTimeout: Bool) async throws {
        let server = try await FakeRedisServer([byTimeout ? .silence : .close])
        let id = RedisClientIdentifier(withHost: "127.0.0.1", port: server.port, commandTimeout: byTimeout ? 0.3 : nil)
        let client = try await RedisClient.connect(withIdentifier: id)
        let drained = Task { for await _ in client.messages {}; return true }
        await #expect(throws: (any Error).self) { try await client.ping() }
        let watchdog = Task {
            try await Task.sleep(nanoseconds: 5_000_000_000)
            drained.cancel()
        }
        let finished = await drained.value
        watchdog.cancel()
        #expect(finished)
        // drained.cancel() also ends the loop, so check it ended without the watchdog.
        #expect(!drained.isCancelled, "messages did not finish when the connection dropped")
        await server.shutdown()
    }
}
