import Foundation
import NIOCore
import NIOEmbedded
import Testing
@testable import PerfectRedis

/// Hostile reply framing must fail the connection, not crash the process.
@Suite struct ReplyGuardTests {
    /// Sends PING to a fake server that answers with `reply`, then a second PING.
    func exchange(_ reply: String) async throws -> (first: Result<RedisResponse, Error>, second: Result<RedisResponse, Error>) {
        let server = try await FakeRedisServer(replies: reply, "+PONG\r\n")
        let client = try await RedisClient.connect(withIdentifier: server.identifier)
        defer { Task { try? await client.close(); await server.shutdown() } }
        let first = await Result(catching: { try await client.ping() })
        let second = await Result(catching: { try await client.ping() })
        return (first, second)
    }

    func expectRejected(_ reply: String, sourceLocation: SourceLocation = #_sourceLocation) async throws {
        let (first, second) = try await exchange(reply)
        guard case .failure(let error) = first else {
            Issue.record("Expected the reply to be rejected, got \(first)", sourceLocation: sourceLocation)
            return
        }
        #expect(error is RedisClient.CommandError, "\(error)", sourceLocation: sourceLocation)
        // The connection is closed afterwards rather than left out of step.
        #expect(throws: (any Error).self, sourceLocation: sourceLocation) { try second.get() }
    }

    @Test func bulkLengthThatOverflows() async throws {
        try await expectRejected("$9223372036854775807\r\n")
    }

    @Test func negativeBulkLength() async throws {
        try await expectRejected("$-2\r\n")
    }

    @Test func arrayCountThatOverflows() async throws {
        try await expectRejected("*9223372036854775807\r\n")
    }

    @Test func negativeArrayCount() async throws {
        try await expectRejected("*-2\r\n")
    }

    @Test func deeplyNestedArrays() async throws {
        try await expectRejected(String(repeating: "*1\r\n", count: 100_000) + ":1\r\n")
    }

    @Test func nestingJustPastTheLimit() async throws {
        try await expectRejected(String(repeating: "*1\r\n", count: RESPReplyGuard.maxDepth + 1) + ":1\r\n")
    }

    @Test func nonNumericLength() async throws {
        try await expectRejected("$abc\r\n")
    }

    @Test func overlongLengthHeader() async throws {
        try await expectRejected("$" + String(repeating: "0", count: 100) + "1\r\n")
    }

    @Test func resp3TypeByte() async throws {
        try await expectRejected("%1\r\n+a\r\n+b\r\n")
    }

    // RediStack drops whatever byte precedes LF, so a header or line ending in a bare LF is
    // read differently by the guard and the parser. It must be rejected, not reinterpreted.
    @Test(arguments: ["$5\n", "*1\n", "+\n", "+OK\n", ":1\n", "-ERR\n", "$-1\n", "*0\n"])
    func lineEndingWithoutCarriageReturn(_ reply: String) async throws {
        try await expectRejected(reply)
    }

    /// "$55\n" is 55 to a guard that tolerates a bare LF but 5 to RediStack, which then
    /// parses the "body" as new values: here an overflowing bulk length.
    @Test func bareLineFeedCannotSmuggleAnOverflow() async throws {
        var body = "abcde\r\n$9223372036854775807\r\n"
        body += String(repeating: "x", count: 57 - body.utf8.count)
        try await expectRejected("$55\n" + body)
    }

    @Test func bareLineFeedCannotSmuggleDeepNesting() async throws {
        let nested = String(repeating: "*1\r\n", count: 200_000)
        try await expectRejected("$1000009\n" + String(repeating: "x", count: 100_000) + "\r\n" + nested)
    }

    @Test func carriageReturnSplitFromLineFeedAcrossReads() throws {
        let channel = EmbeddedChannel(handler: RESPReplyGuard())
        for chunk in ["+OK\r", "\n", "$3\r", "\nabc\r\n", "*1\r", "\n:1\r", "\n"] {
            try channel.writeInbound(channel.allocator.buffer(string: chunk))
        }
        // A line split after a byte that isn't CR is still caught.
        #expect(throws: RedisClient.CommandError.self) {
            try channel.writeInbound(channel.allocator.buffer(string: "+OK"))
            try channel.writeInbound(channel.allocator.buffer(string: "\n"))
        }
        _ = try? channel.finish()
    }

    /// RediStack reserves an array's capacity from its declared count before any element
    /// arrives: 24 bytes each, so 2^32 - 1 asked Linux for ~100 GB and aborted the process.
    @Test(arguments: [4_294_967_295, 200_000_000, RESPReplyGuard.maxPendingElements + 1])
    func arrayCountTooLargeToReserve(_ count: Int) async throws {
        try await expectRejected("*\(count)\r\n:1\r\n")
    }

    @Test func bulkLengthBeyondRedisMaximum() async throws {
        try await expectRejected("$\(512 * 1024 * 1024 + 1)\r\n")
    }

    /// Nested arrays each reserve their own count, so the limit is on the total still to come.
    @Test func pendingElementsAreCountedAcrossNestedArrays() throws {
        let half = RESPReplyGuard.maxPendingElements / 2
        let channel = EmbeddedChannel(handler: RESPReplyGuard())
        try channel.writeInbound(channel.allocator.buffer(string: "*\(half)\r\n"))
        // All half of the outer elements are still pending while this one is open.
        #expect(throws: RedisClient.CommandError.self) {
            try channel.writeInbound(channel.allocator.buffer(string: "*\(half + 2)\r\n"))
        }
        _ = try? channel.finish()
    }

    @Test func countsAtTheLimitPassThrough() throws {
        let channel = EmbeddedChannel(handler: RESPReplyGuard())
        try channel.writeInbound(channel.allocator.buffer(string: "*\(RESPReplyGuard.maxPendingElements)\r\n"))
        try channel.writeInbound(channel.allocator.buffer(string: "$\(RESPReplyGuard.maxBulkLength)\r\n"))
        _ = try? channel.finish()
    }

    @Test func nestingAtTheLimitIsAccepted() async throws {
        let depth = RESPReplyGuard.maxDepth
        let (first, second) = try await exchange(String(repeating: "*1\r\n", count: depth) + ":7\r\n")
        var value = try first.get()
        for _ in 0..<depth {
            guard case .array(let elements) = value, elements.count == 1 else {
                Issue.record("Expected a 1-element array, got \(value)"); return
            }
            value = elements[0]
        }
        #expect(value.integer == 7)
        #expect(try second.get().string == "PONG")
    }

    @Test func largeBulkStringIsAccepted() async throws {
        let size = 4 << 20
        let (first, second) = try await exchange("$\(size)\r\n" + String(repeating: "x", count: size) + "\r\n")
        guard case .bulkString(let bytes?) = try first.get() else {
            Issue.record("Expected a bulk string"); return
        }
        #expect(bytes.count == size)
        #expect(try second.get().string == "PONG")
    }

    /// Feeds a mix of every reply type a byte at a time, so each header, line and body
    /// straddles reads, and checks every byte is passed on untouched.
    @Test func framingSurvivesArbitraryReadBoundaries() throws {
        let stream = "+OK\r\n-ERR bad\r\n:-42\r\n$5\r\nhe\r\no\r\n$0\r\n\r\n$-1\r\n*-1\r\n*0\r\n"
            + "*3\r\n$1\r\na\r\n*2\r\n:1\r\n*1\r\n+x\r\n$-1\r\n"
            + "*2\r\n*2\r\n:1\r\n:2\r\n*2\r\n:3\r\n:4\r\n"
        let channel = EmbeddedChannel(handler: RESPReplyGuard())
        var passed: [UInt8] = []
        for byte in Array(stream.utf8) {
            try channel.writeInbound(channel.allocator.buffer(bytes: [byte]))
            while var out: ByteBuffer = try channel.readInbound() {
                passed += out.readBytes(length: out.readableBytes) ?? []
            }
        }
        #expect(passed == Array(stream.utf8))
        // Back at a value boundary: a further reply is still accepted.
        try channel.writeInbound(channel.allocator.buffer(string: "+PONG\r\n"))
        #expect(try channel.readInbound(as: ByteBuffer.self) != nil)
        _ = try channel.finish()
    }

    @Test func violationIsRaisedAsAnError() throws {
        let channel = EmbeddedChannel(handler: RESPReplyGuard())
        #expect(throws: RedisClient.CommandError.self) {
            try channel.writeInbound(channel.allocator.buffer(string: "$9223372036854775807\r\n"))
        }
        // Anything after the violation is dropped.
        try channel.writeInbound(channel.allocator.buffer(string: "+OK\r\n"))
        #expect(try channel.readInbound(as: ByteBuffer.self) == nil)
        _ = try? channel.finish()
    }
}

extension Result where Failure == Error {
    init(catching body: () async throws -> Success) async {
        do { self = .success(try await body()) } catch { self = .failure(error) }
    }
}
