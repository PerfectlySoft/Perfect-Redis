import Foundation
import Testing
@testable import PerfectRedis

/// Replies fed from a scripted fake server, so no Redis is needed.
@Suite struct ReplyTests {
    /// Connects to a fake server that answers the first request with `reply`, sends PING,
    /// and returns the reply as the client decoded it.
    func decode(_ reply: String) async throws -> RedisResponse {
        let server = try await FakeRedisServer(replies: reply)
        let client = try await RedisClient.connect(withIdentifier: server.identifier)
        defer { Task { try? await client.close(); await server.shutdown() } }
        return try await client.ping()
    }

    @Test func emptyBulkStringIsEmptyNotNil() async throws {
        let response = try await decode("$0\r\n\r\n")
        guard case .bulkString(let bytes) = response else {
            Issue.record("Expected a bulk string, got \(response)"); return
        }
        #expect(bytes == [])
        #expect(!response.isNil)
        #expect(response.string == "")
    }

    @Test func nullBulkStringIsNil() async throws {
        let response = try await decode("$-1\r\n")
        #expect(response.isNil)
    }

    @Test func emptyStringsInsideArrays() async throws {
        let response = try await decode("*2\r\n$0\r\n\r\n$-1\r\n")
        guard case .array(let elements) = response, elements.count == 2 else {
            Issue.record("Expected a 2-element array, got \(response)"); return
        }
        #expect(!elements[0].isNil)
        #expect(elements[0].string == "")
        #expect(elements[1].isNil)
    }
}
