import Foundation
import Testing
import NIOConcurrencyHelpers
@testable import PerfectRedis

/// Where the live tests find a server. The live suites FLUSHALL, so only point them at a
/// throwaway server.
///
/// - `REDIS_TESTS=1` turns the live suites on.
/// - `REDIS_HOST` (default 127.0.0.1) and `REDIS_PORT` (default 6379) name the server.
/// - `REDIS_REQUIRE_LIVE=1` makes a missing or unreachable server a failure instead of a skip.
enum RedisTestEnvironment {
    static let environment = ProcessInfo.processInfo.environment
    static let isEnabled = environment["REDIS_TESTS"] == "1"
    static let required = environment["REDIS_REQUIRE_LIVE"] == "1"
    static let host = environment["REDIS_HOST"].flatMap { $0.isEmpty ? nil : $0 } ?? "127.0.0.1"
    static let port = environment["REDIS_PORT"].flatMap { Int($0) } ?? redisDefaultPort

    static var identifier: RedisClientIdentifier {
        RedisClientIdentifier(withHost: host, port: port)
    }

    /// Connects to the test server and empties it.
    static func connect() async throws -> RedisClient {
        let client = try await RedisClient.connect(withIdentifier: identifier)
        _ = try await client.flushAll()
        return client
    }

    /// Runs `body` with a fresh, emptied connection, then closes it.
    static func withClient(_ body: (RedisClient) async throws -> Void) async throws {
        let client = try await connect()
        do {
            try await body(client)
        } catch {
            try? await client.close()
            throw error
        }
        try await client.close()
    }
}

/// Live suites skip when they have no server, so a CI misconfiguration would pass silently.
/// With REDIS_REQUIRE_LIVE=1 (set in CI wherever a server runs) this fails instead.
@Suite(.timeLimit(.minutes(1))) struct LiveServerRequirementTests {
    @Test(.enabled(if: RedisTestEnvironment.required))
    func liveServerIsReachableWhenRequired() async throws {
        try #require(RedisTestEnvironment.isEnabled, "REDIS_REQUIRE_LIVE=1 but REDIS_TESTS is not 1")
        let client = try await RedisClient.connect(withIdentifier: RedisTestEnvironment.identifier)
        let pong = try await client.ping()
        try await client.close()
        guard case .simpleString("PONG") = pong else {
            Issue.record("Unexpected PING reply \(pong)")
            return
        }
    }
}

/// Every live suite nests in here. They share one server and empty it, so they must not
/// run in parallel with each other.
@Suite(
    .serialized, .timeLimit(.minutes(1)),
    .enabled(if: RedisTestEnvironment.isEnabled, "Set REDIS_TESTS=1 to run against a server")
)
enum LiveRedisTests {}

/// Runs `body`, but stops waiting after `seconds`: returns its result, or nil after
/// recording an issue if it hasn't finished (the task is cancelled and left behind), so a
/// regression fails the test instead of hanging the run.
func bounded<T: Sendable>(
    _ seconds: Double, _ what: String,
    sourceLocation: SourceLocation = #_sourceLocation,
    _ body: @escaping @Sendable () async throws -> T
) async -> Result<T, Error>? {
    let slot = NIOLockedValueBox<CheckedContinuation<Result<T, Error>?, Never>?>(nil)
    let result = await withCheckedContinuation { continuation in
        slot.withLockedValue { $0 = continuation }
        let work = Task<Void, Never> {
            let result: Result<T, Error>
            do { result = .success(try await body()) } catch { result = .failure(error) }
            slot.withLockedValue { $0?.resume(returning: result); $0 = nil }
        }
        Task<Void, Never> {
            try? await Task.sleep(nanoseconds: UInt64(seconds * 1_000_000_000))
            let timedOut = slot.withLockedValue { slot -> Bool in
                guard let pending = slot else { return false }
                pending.resume(returning: nil)
                slot = nil
                return true
            }
            if timedOut { work.cancel() }
        }
    }
    if result == nil {
        Issue.record("\(what) didn't finish within \(seconds) s", sourceLocation: sourceLocation)
    }
    return result
}
