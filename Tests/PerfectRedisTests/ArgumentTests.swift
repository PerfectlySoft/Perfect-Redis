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
}
