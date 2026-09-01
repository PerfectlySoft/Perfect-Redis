import Foundation
import Testing
@testable import PerfectRedis

// Run with: REDIS_TESTS=1 swift test
// Requires a Redis server on 127.0.0.1:6379

private var redisEnabled: Bool { ProcessInfo.processInfo.environment["REDIS_TESTS"] == "1" }

private func withRedis(_ body: (RedisClient) async throws -> Void) async throws {
    guard redisEnabled else { return }
    let client = try await RedisClient.connect()
    _ = try await client.flushAll()
    defer { Task { try? await client.close() } }
    try await body(client)
}

@Suite(.serialized)
struct PerfectRedisTests {

    @Test func connect() async throws {
        try await withRedis { _ in }
    }

    @Test func ping() async throws {
        try await withRedis { client in
            let r = try await client.ping()
            guard case .simpleString(let s) = r else {
                Issue.record("Expected simpleString, got \(r)")
                return
            }
            #expect(s == "PONG")
        }
    }

    @Test func flushAll() async throws {
        try await withRedis { client in
            let r = try await client.flushAll()
            #expect(r.isSimpleOK)
        }
    }

    @Test func setGet() async throws {
        try await withRedis { client in
            let (key, value) = ("my key", "myvalue")
            let setR = try await client.set(key: key, value: .string(value))
            #expect(setR.isSimpleOK)
            let getR = try await client.get(key: key)
            #expect(getR.string == value)
        }
    }

    @Test func setGetXX() async throws {
        try await withRedis { client in
            let (key, value) = ("mykey", "myvalue")
            // XX = set only if exists; key doesn't exist yet → nil response
            let r = try await client.set(key: key, value: .string(value), ifExists: true)
            #expect(r.isNil)
            let getR = try await client.get(key: key)
            #expect(getR.isNil)
        }
    }

    @Test func setGetNX() async throws {
        try await withRedis { client in
            let (key, value, value2) = ("mykey", "myvalue", "other")
            _ = try await client.set(key: key, value: .string(value))
            // NX = set only if not exists; key already exists → nil response
            let r = try await client.set(key: key, value: .string(value2), ifNotExists: true)
            #expect(r.isNil)
            let getR = try await client.get(key: key)
            #expect(getR.string == value)
        }
    }

    @Test func setGetExpiry() async throws {
        try await withRedis { client in
            let (key, value) = ("mykey", "myvalue")
            _ = try await client.set(key: key, value: .string(value), expires: 1.0)
            let before = try await client.get(key: key)
            #expect(before.string == value)
            try await Task.sleep(for: .seconds(2))
            let after = try await client.get(key: key)
            #expect(after.isNil)
        }
    }

    @Test func deleteKey() async throws {
        try await withRedis { client in
            _ = try await client.set(key: "dk", value: .string("v"))
            try await client.delete(keys: "dk")
            let r = try await client.get(key: "dk")
            #expect(r.isNil)
        }
    }

    @Test func increment() async throws {
        try await withRedis { client in
            let r1 = try await client.increment(key: "counter")
            #expect(r1.integer == 1)
            let r2 = try await client.increment(key: "counter", by: 4)
            #expect(r2.integer == 5)
            let r3 = try await client.decrement(key: "counter")
            #expect(r3.integer == 4)
            let r4 = try await client.decrement(key: "counter", by: 2)
            #expect(r4.integer == 2)
        }
    }

    @Test func getMultiple() async throws {
        try await withRedis { client in
            _ = try await client.set(keysValues: [("k1", .string("v1")), ("k2", .string("v2"))])
            let r = try await client.get(keys: ["k1", "k2", "k3"])
            guard case .array(let a) = r else {
                Issue.record("Expected array, got \(r)")
                return
            }
            #expect(a.count == 3)
            #expect(a[0].string == "v1")
            #expect(a[1].string == "v2")
            #expect(a[2].isNil)
        }
    }

    @Test func rename() async throws {
        try await withRedis { client in
            _ = try await client.set(key: "orig", value: .string("hello"))
            let r = try await client.rename(key: "orig", newKey: "renamed")
            #expect(r.isSimpleOK)
            let getR = try await client.get(key: "renamed")
            #expect(getR.string == "hello")
        }
    }

    @Test func exists() async throws {
        try await withRedis { client in
            _ = try await client.set(key: "ek", value: .string("v"))
            let r = try await client.exists(keys: "ek")
            #expect(r.integer == 1)
            let r2 = try await client.exists(keys: "nonexistent")
            #expect(r2.integer == 0)
        }
    }

    @Test func expireAndPersist() async throws {
        try await withRedis { client in
            _ = try await client.set(key: "ep", value: .string("v"))
            _ = try await client.expire(key: "ep", seconds: 60)
            let ttl = try await client.timeToExpire(key: "ep")
            #expect(ttl.integer > 0)
            _ = try await client.persist(key: "ep")
            let ttl2 = try await client.timeToExpire(key: "ep")
            #expect(ttl2.integer == -1)
        }
    }

    @Test func listOperations() async throws {
        try await withRedis { client in
            _ = try await client.listAppend(key: "mylist", values: [.string("a"), .string("b"), .string("c")])
            let len = try await client.listLength(key: "mylist")
            #expect(len.integer == 3)
            let rangeR = try await client.listRange(key: "mylist", start: 0, stop: -1)
            guard case .array(let a) = rangeR else {
                Issue.record("Expected array, got \(rangeR)")
                return
            }
            #expect(a.map { $0.string } == ["a", "b", "c"])
            let pop = try await client.listPopFirst(key: "mylist")
            #expect(pop.string == "a")
        }
    }

    @Test func setOperations() async throws {
        try await withRedis { client in
            _ = try await client.setAdd(key: "myset", elements: [.string("x"), .string("y"), .string("z")])
            let cnt = try await client.setCount(key: "myset")
            #expect(cnt.integer == 3)
            let has = try await client.setContains(key: "myset", value: .string("x"))
            #expect(has.integer == 1)
            let missing = try await client.setContains(key: "myset", value: .string("nope"))
            #expect(missing.integer == 0)
            _ = try await client.setRemove(key: "myset", value: .string("x"))
            let cnt2 = try await client.setCount(key: "myset")
            #expect(cnt2.integer == 2)
        }
    }

    @Test func keys() async throws {
        try await withRedis { client in
            _ = try await client.set(keysValues: [("kk:1", .string("a")), ("kk:2", .string("b"))])
            let r = try await client.keys(pattern: "kk:*")
            guard case .array(let a) = r else {
                Issue.record("Expected array, got \(r)")
                return
            }
            #expect(a.count == 2)
        }
    }

    @Test func dbSize() async throws {
        try await withRedis { client in
            _ = try await client.set(key: "sz", value: .string("v"))
            let r = try await client.dbSize()
            #expect(r.integer >= 1)
        }
    }
}
