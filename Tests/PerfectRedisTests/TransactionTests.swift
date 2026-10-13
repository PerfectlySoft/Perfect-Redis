import Foundation
import Testing
@testable import PerfectRedis

extension LiveRedisTests {
    @Suite struct TransactionTests {
        @Test func execReturnsQueuedResults() async throws {
            try await RedisTestEnvironment.withClient { client in
                let exec = try await client.multi {
                    _ = try await client.set(key: "t", value: .string("1"))
                    _ = try await client.increment(key: "t")
                }
                guard case .array(let results) = exec, results.count == 2 else {
                    Issue.record("Expected two results, got \(exec)"); return
                }
                #expect(results[0].isSimpleOK)
                #expect(results[1].integer == 2)
            }
        }

        /// Another task's command must not land inside someone else's MULTI…EXEC.
        @Test func otherTasksWaitForTheTransaction() async throws {
            try await RedisTestEnvironment.withClient { client in
                _ = try await client.set(key: "mk", value: .string("before"))
                let outsider = Task {
                    try await Task.sleep(nanoseconds: 100_000_000)
                    return try await client.get(key: "mk")
                }
                let exec = try await client.multi {
                    _ = try await client.set(key: "mk", value: .string("inside"))
                    try await Task.sleep(nanoseconds: 400_000_000)
                }
                let seen = try await outsider.value
                #expect(seen.string == "inside", "The outsider's GET was \(seen)")
                guard case .array(let results) = exec else {
                    Issue.record("Expected EXEC results, got \(exec)"); return
                }
                #expect(results.count == 1)
            }
        }

        @Test func concurrentTransactionsDontInterleave() async throws {
            try await RedisTestEnvironment.withClient { client in
                let results = try await withThrowingTaskGroup(of: RedisResponse.self) { group in
                    for i in 0..<20 {
                        group.addTask {
                            try await client.multi {
                                _ = try await client.set(key: "tx\(i)", value: .string("\(i)"))
                                await Task.yield()
                                _ = try await client.get(key: "tx\(i)")
                            }
                        }
                    }
                    var all: [RedisResponse] = []
                    for try await r in group { all.append(r) }
                    return all
                }
                #expect(results.count == 20)
                for r in results {
                    guard case .array(let pair) = r, pair.count == 2 else {
                        Issue.record("Expected [OK, value], got \(r)"); continue
                    }
                    #expect(pair[0].isSimpleOK)
                }
            }
        }

        @Test func failingBodyDiscardsAndReleases() async throws {
            struct Abandon: Error {}
            try await RedisTestEnvironment.withClient { client in
                await #expect(throws: Abandon.self) {
                    try await client.multi {
                        _ = try await client.set(key: "never", value: .string("x"))
                        throw Abandon()
                    }
                }
                let never = try await client.get(key: "never")
                let pong = try await client.ping()
                #expect(never.isNil)
                #expect(pong.string == "PONG")
            }
        }

        @Test func nestedTransactionIsRefused() async throws {
            try await RedisTestEnvironment.withClient { client in
                await #expect(throws: RedisClient.CommandError.self) {
                    try await client.multi {
                        _ = try await client.set(key: "outer", value: .string("x"))
                        _ = try await client.multi {}
                    }
                }
                let outer = try await client.get(key: "outer")
                let pong = try await client.ping()
                #expect(outer.isNil)
                #expect(pong.string == "PONG")
            }
        }
    }
}
