import Foundation
import Testing
@testable import PerfectRedis

extension LiveRedisTests {
    /// Waits for another task's transaction, or for commands in flight before subscribing,
    /// must honour cancellation and commandTimeout.
    @Suite struct WaitTests {
        @Test func subscribeWaitingBehindABlockingPopCanBeCancelled() async throws {
            let client = try await RedisTestEnvironment.connect()
            let blpop = Task { try await client.listPopFirstBlocking(keys: "nothing", timeout: 3) }
            try await Task.sleep(nanoseconds: 100_000_000)
            let subscribe = Task { try await client.subscribe(channels: ["x"]) }
            try await Task.sleep(nanoseconds: 200_000_000)
            subscribe.cancel()
            let outcome = await bounded(1.5, "cancelled subscribe") { try await subscribe.value }
            if case .success? = outcome { Issue.record("The cancelled subscribe went ahead") }
            _ = await bounded(5, "BLPOP") { try await blpop.value }
            try? await client.close()
        }

        @Test func commandWaitingForAnotherTasksTransactionTimesOut() async throws {
            let id = RedisClientIdentifier(
                withHost: RedisTestEnvironment.host, port: RedisTestEnvironment.port, commandTimeout: 1
            )
            let client = try await RedisClient.connect(withIdentifier: id)
            let transaction = Task {
                try await client.multi { try await Task.sleep(nanoseconds: 3_000_000_000) }
            }
            try await Task.sleep(nanoseconds: 100_000_000)
            let outcome = await bounded(2.5, "GET behind a 3 s transaction") { try await client.get(key: "k") }
            if case .failure(let error)? = outcome {
                #expect(error is RedisClient.CommandError, "\(error)")
            } else if outcome != nil {
                Issue.record("The GET went ahead inside another task's transaction")
            }
            _ = await bounded(5, "transaction") { try await transaction.value }
            try? await client.close()
        }

        @Test func commandWaitingForAnotherTasksTransactionCanBeCancelled() async throws {
            let client = try await RedisTestEnvironment.connect()
            let transaction = Task {
                try await client.multi { try await Task.sleep(nanoseconds: 1_500_000_000) }
            }
            try await Task.sleep(nanoseconds: 100_000_000)
            let waiting = Task { try await client.get(key: "k") }
            try await Task.sleep(nanoseconds: 100_000_000)
            waiting.cancel()
            let outcome = await bounded(0.8, "cancelled GET behind a transaction") { try await waiting.value }
            if case .failure(let error)? = outcome {
                #expect(error is CancellationError, "\(error)")
            } else if outcome != nil {
                Issue.record("The cancelled GET went ahead")
            }
            _ = await bounded(5, "transaction") { try await transaction.value }
            try? await client.close()
        }
    }
}
