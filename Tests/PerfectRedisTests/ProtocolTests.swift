import Foundation
import Testing
@testable import PerfectRedis

extension LiveRedisTests {
    /// Framing and pipelining against a real server.
    @Suite struct ProtocolTests {
        /// Every argument goes out as a length-prefixed bulk string, so CR, LF, `$` and `*`
        /// in keys, values and command names can't inject a second command.
        @Test func protocolCharactersInArgumentsStayInsideThem() async throws {
            try await RedisTestEnvironment.withClient { client in
                let key = "a\r\nSET injected yes\r\n"
                let value = "*3\r\n$3\r\nSET\r\n$8\r\ninjected\r\n$3\r\nyes\r\n"
                _ = try await client.set(key: key, value: .string(value))
                let readBack = try await client.get(key: key)
                #expect(readBack.string == value)
                let injected = try await client.get(key: "injected")
                #expect(injected.isNil)

                await #expect(throws: (any Error).self) {
                    try await client.sendCommandAsRESP(name: "PING\r\nSET injected yes", parameters: [])
                }
                let afterName = try await client.get(key: "injected")
                #expect(afterName.isNil)
                let field = try await client.hashSet(key: "h", field: "f\r\n*1\r\n", value: .string("$-1\r\n"))
                #expect(field.integer == 1)
                let fieldValue = try await client.hashGet(key: "h", field: "f\r\n*1\r\n")
                #expect(fieldValue.string == "$-1\r\n")
                #expect(try await client.dbSize().integer == 2)
            }
        }

        @Test func everyByteValueRoundTrips() async throws {
            try await RedisTestEnvironment.withClient { client in
                let bytes = (0...255).map { UInt8($0) }
                _ = try await client.set(key: "bytes", value: .binary(bytes))
                guard case .bulkString(let back?) = try await client.get(key: "bytes") else {
                    Issue.record("Expected a bulk string"); return
                }
                #expect(back == bytes)
            }
        }

        /// Thousands of commands from concurrent tasks share one pipelined connection; each
        /// must get its own reply.
        @Test func concurrentCommandsGetTheirOwnReplies() async throws {
            try await RedisTestEnvironment.withClient { client in
                let mismatches = try await withThrowingTaskGroup(of: Bool.self) { group in
                    for i in 0..<2000 {
                        group.addTask {
                            _ = try await client.set(key: "c\(i)", value: .string("v\(i)"))
                            return try await client.get(key: "c\(i)").string == "v\(i)"
                        }
                    }
                    var mismatches = 0
                    for try await matched in group where !matched { mismatches += 1 }
                    return mismatches
                }
                #expect(mismatches == 0)
            }
        }

        @Test func blockingPopTimesOutWithNil() async throws {
            try await RedisTestEnvironment.withClient { client in
                let reply = try await client.listPopFirstBlocking(keys: "empty-list", timeout: 1)
                #expect(reply.isNil)
                #expect(try await client.ping().string == "PONG")
            }
        }

        @Test func blockingPopReturnsPushedValue() async throws {
            try await RedisTestEnvironment.withClient { client in
                let pusher = try await RedisClient.connect(withIdentifier: RedisTestEnvironment.identifier)
                let popped = Task { try await client.listPopFirstBlocking(keys: "jobs", timeout: 5) }
                try await Task.sleep(nanoseconds: 200_000_000)
                _ = try await pusher.listAppend(key: "jobs", values: [.string("job1")])
                let reply = try await popped.value
                guard case .array(let pair) = reply, pair.count == 2 else {
                    Issue.record("Expected [key, value], got \(reply)"); return
                }
                #expect(pair.map { $0.string } == ["jobs", "job1"])
                try await pusher.close()
            }
        }

        @Test func commandTimeoutCutsOffALongerBlockingCommand() async throws {
            let id = RedisClientIdentifier(
                withHost: RedisTestEnvironment.host, port: RedisTestEnvironment.port, commandTimeout: 1
            )
            let client = try await RedisClient.connect(withIdentifier: id)
            await #expect(throws: RedisClient.CommandError.self) {
                try await client.listPopFirstBlocking(keys: "never-filled", timeout: 3)
            }
        }

        @Test func emptyElementsInsideArrayReplies() async throws {
            try await RedisTestEnvironment.withClient { client in
                _ = try await client.listAppend(key: "l", values: [.string(""), .string("x"), .string("")])
                guard case .array(let items) = try await client.listRange(key: "l", start: 0, stop: -1) else {
                    Issue.record("Expected an array"); return
                }
                #expect(items.map { $0.string } == ["", "x", ""])
                #expect(items.allSatisfy { !$0.isNil })
            }
        }

        @Test func execReportsPerCommandErrors() async throws {
            try await RedisTestEnvironment.withClient { client in
                _ = try await client.listAppend(key: "list", values: [.string("x")])
                let exec = try await client.multi {
                    _ = try await client.increment(key: "list")  // WRONGTYPE at EXEC time
                    _ = try await client.set(key: "ok", value: .string("1"))
                }
                guard case .array(let results) = exec, results.count == 2,
                      case .error(let type, _) = results[0] else {
                    Issue.record("Expected [error, OK], got \(exec)"); return
                }
                #expect(type == "WRONGTYPE")
                #expect(results[1].isSimpleOK)
            }
        }
    }
}
