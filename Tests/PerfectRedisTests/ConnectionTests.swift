import Foundation
import Testing
@testable import PerfectRedis

/// Connection-level failures, against a scripted fake server.
@Suite(.timeLimit(.minutes(1))) struct ConnectionTests {
    func elapsed(_ body: () async throws -> Void) async rethrows -> Double {
        let start = Date()
        try await body()
        return Date().timeIntervalSince(start)
    }

    @Test func unansweredCommandTimesOut() async throws {
        let server = try await FakeRedisServer([.silence, .reply(Array("+PONG\r\n".utf8))])
        let id = RedisClientIdentifier(withHost: "127.0.0.1", port: server.port, commandTimeout: 1)
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
        let id = RedisClientIdentifier(withHost: "127.0.0.1", port: server.port, commandTimeout: 1)
        let client = try await RedisClient.connect(withIdentifier: id)
        #expect(try await client.ping().string == "PONG")
        try await Task.sleep(nanoseconds: 1_500_000_000)  // past the first command's deadline
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
        let id = RedisClientIdentifier(withHost: "127.0.0.1", port: server.port, commandTimeout: byTimeout ? 1 : nil)
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

    @Test func subscribeHonoursTheTimeout() async throws {
        let server = try await FakeRedisServer([.silence])
        let id = RedisClientIdentifier(withHost: "127.0.0.1", port: server.port, commandTimeout: 1)
        let client = try await RedisClient.connect(withIdentifier: id)
        let seconds = await elapsed {
            await #expect(throws: RedisClient.CommandError.self) { try await client.subscribe(channels: ["c"]) }
        }
        #expect(seconds < 5)
        await server.shutdown()
    }

    @Test func subscribeHonoursCancellation() async throws {
        let server = try await FakeRedisServer([.silence])
        let client = try await RedisClient.connect(withIdentifier: server.identifier)
        let waiting = Task { try await client.subscribe(patterns: ["p*"]) }
        try await Task.sleep(nanoseconds: 200_000_000)
        let seconds = await elapsed {
            waiting.cancel()
            let result = await waiting.result
            #expect(throws: CancellationError.self) { try result.get() }
        }
        #expect(seconds < 5)
        await server.shutdown()
    }

    /// AUTH is sent while connecting; a server that never answers it hung connect().
    @Test func connectHonoursTheTimeoutDuringAuth() async throws {
        let server = try await FakeRedisServer([.silence])
        let id = RedisClientIdentifier(withHost: "127.0.0.1", port: server.port, password: "pw", commandTimeout: 1)
        let seconds = await elapsed {
            await #expect(throws: RedisClient.CommandError.self) { try await RedisClient.connect(withIdentifier: id) }
        }
        #expect(seconds < 5)
        await server.shutdown()
    }

    /// A task that is already cancelled used to send its command and then close the
    /// connection for everyone; now it throws without sending.
    @Test func alreadyCancelledTaskSendsNothing() async throws {
        let server = try await FakeRedisServer(replies: "+PONG\r\n")
        let client = try await RedisClient.connect(withIdentifier: server.identifier)
        let cancelled = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            return try await client.ping()
        }
        await #expect(throws: CancellationError.self) { try await cancelled.value }
        let pong = try await client.ping()
        #expect(pong.string == "PONG")
        try await client.close()
        await server.shutdown()
    }

    /// A client released without close() left its socket open.
    @Test func releasingAClientClosesItsConnection() async throws {
        let server = try await FakeRedisServer(replies: "+PONG\r\n")
        var client: RedisClient? = try await RedisClient.connect(withIdentifier: server.identifier)
        _ = try await client?.ping()
        client = nil
        var waited = 0
        while server.closedConnections == 0, waited < 50 {
            try await Task.sleep(nanoseconds: 100_000_000)
            waited += 1
        }
        #expect(server.closedConnections == 1, "The released client's connection was left open")
        await server.shutdown()
    }

    /// RediStack never fails a pending subscription change when the connection drops, so
    /// subscribe waited forever, and the client then refused commands as if subscribed.
    @Test func subscriptionChangeEndsWhenTheConnectionDrops() async throws {
        let server = try await FakeRedisServer([.close])
        let client = try await RedisClient.connect(withIdentifier: server.identifier)
        let outcome = await bounded(5, "subscribe on a dropped connection") { try await client.subscribe(channels: ["x"]) }
        if case .success? = outcome { Issue.record("subscribe succeeded on a closed connection") }
        let ping = await bounded(5, "ping after") { try await client.ping() }
        if case .failure(let error)? = ping {
            #expect(!(error is RedisClient.CommandError), "Expected a closed-connection error, got \(error)")
        } else if ping != nil {
            Issue.record("ping succeeded on a closed connection")
        }
        await server.shutdown()
    }

    /// Each command used to register its own closeFuture callback, which NIO keeps until the
    /// connection closes: about 200 bytes per command, for the life of the client. Waits now
    /// share one callback and deregister when they end.
    @Test func finishedCommandsLeaveNoCloseWatchers() async throws {
        let server = try await FakeRedisServer((0..<2_000).map { _ in .reply(Array("+PONG\r\n".utf8)) })
        let client = try await RedisClient.connect(withIdentifier: server.identifier)
        for _ in 0..<2_000 { _ = try await client.ping() }
        #expect(client.closeWatchers.count == 0)
        try await client.close()
        await server.shutdown()
    }

    @Test func aWaitInProgressIsWatchedUntilTheConnectionCloses() async throws {
        let server = try await FakeRedisServer([.silence])
        let client = try await RedisClient.connect(withIdentifier: server.identifier)
        let pending = Task { try await client.ping() }
        var waited = 0
        while client.closeWatchers.count == 0, waited < 50 {
            try await Task.sleep(nanoseconds: 20_000_000)
            waited += 1
        }
        #expect(client.closeWatchers.count == 1)
        try await client.close()
        _ = await bounded(5, "pending ping after close") { try await pending.value }
        #expect(client.closeWatchers.count == 0)
        await server.shutdown()
    }

    /// commandTimeout is one budget for the whole call: waiting behind another task's
    /// transaction and then for the reply used to get commandTimeout each.
    @Test func oneDeadlineCoversWaitingAndTheReply() async throws {
        let server = try await FakeRedisServer([
            .reply(Array("+OK\r\n".utf8)),  // MULTI
            .reply(Array("*0\r\n".utf8)),   // EXEC
            .silence,                         // the GET that waited
        ])
        let id = RedisClientIdentifier(withHost: "127.0.0.1", port: server.port, commandTimeout: 1)
        let client = try await RedisClient.connect(withIdentifier: id)
        let transaction = Task { try await client.multi { try await Task.sleep(nanoseconds: 700_000_000) } }
        try await Task.sleep(nanoseconds: 100_000_000)
        let seconds = await elapsed {
            _ = await bounded(5, "GET behind the transaction") {
                try await client.get(key: "k")
            }
        }
        #expect(seconds < 1.4, "Took \(seconds) s with a 1 s commandTimeout")
        _ = await bounded(5, "transaction") { try await transaction.value }
        await server.shutdown()
    }

    /// Each internal wait scheduled a deadline timer that was never cancelled, and the timer
    /// held the client until it fired, so a client released after waiting stayed connected
    /// for up to commandTimeout.
    @Test func finishedWaitsDontKeepTheClientAlive() async throws {
        let server = try await FakeRedisServer([
            .reply(Array("+OK\r\n".utf8)),     // MULTI
            .reply(Array("*0\r\n".utf8)),      // EXEC
            .reply(Array("$1\r\nv\r\n".utf8)), // the GET that waited
        ])
        let id = RedisClientIdentifier(withHost: "127.0.0.1", port: server.port, commandTimeout: 30)
        // Everything that touches the client happens in here, so nothing in this test holds
        // it afterwards.
        func waitBehindATransaction() async throws {
            let client = try await RedisClient.connect(withIdentifier: id)
            let transaction = Task { try await client.multi { try await Task.sleep(nanoseconds: 300_000_000) } }
            try await Task.sleep(nanoseconds: 50_000_000)
            let get = try await client.get(key: "k")
            #expect(get.string == "v")
            _ = try await transaction.value
        }
        try await waitBehindATransaction()
        var waited = 0
        while server.closedConnections == 0, waited < 30 {
            try await Task.sleep(nanoseconds: 100_000_000)
            waited += 1
        }
        #expect(server.closedConnections == 1, "The released client stayed connected")
        await server.shutdown()
    }
}
