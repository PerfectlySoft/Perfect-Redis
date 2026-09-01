import Foundation
import Testing
@testable import PerfectRedis

@Suite(.serialized)
struct HashTests {

    @Test func hashSetGet() async throws {
        guard ProcessInfo.processInfo.environment["REDIS_TESTS"] == "1" else { return }
        let client = try await RedisClient.connect()
        _ = try await client.flushAll()
        defer { Task { try? await client.close() } }
        let (key, field, value) = ("mykey", "myfield", "myvalue")
        let setR = try await client.hashSet(key: key, field: field, value: .string(value))
        guard case .integer = setR else {
            Issue.record("Expected integer response from HSET, got \(setR)"); return
        }
        let getR = try await client.hashGet(key: key, field: field)
        #expect(getR.string == value)
    }

    @Test func hashDelAndHashExist() async throws {
        guard ProcessInfo.processInfo.environment["REDIS_TESTS"] == "1" else { return }
        let client = try await RedisClient.connect()
        _ = try await client.flushAll()
        defer { Task { try? await client.close() } }
        let (key, field, value) = ("mykey", "myfield", "myvalue")
        _ = try await client.hashSet(key: key, field: field, value: .string(value))
        let exists1 = try await client.hashExists(key: key, field: field)
        #expect(exists1.integer == 1)
        let del1 = try await client.hashDel(key: key, fields: field)
        #expect(del1.integer == 1)
        let exists2 = try await client.hashExists(key: key, field: field)
        #expect(exists2.integer == 0)
        let del2 = try await client.hashDel(key: key, fields: field)
        #expect(del2.integer == 0)
    }

    @Test func hashMultiSetAndGetAll() async throws {
        guard ProcessInfo.processInfo.environment["REDIS_TESTS"] == "1" else { return }
        let client = try await RedisClient.connect()
        _ = try await client.flushAll()
        defer { Task { try? await client.close() } }
        let key = "mykey"
        let fv: [(String, RedisClient.RedisValue)] = [("myfield", .string("myvalue")), ("myfield2", .string("myvalue2"))]
        _ = try await client.hashSet(key: key, fieldsValues: fv)
        let r = try await client.hashGetAll(key: key)
        guard case .array(var a) = r else {
            Issue.record("Expected array, got \(r)"); return
        }
        #expect(a.count == 4)
        var dict: [String: String] = [:]
        while a.count >= 2 {
            let v = a.popLast()!.string!
            let k = a.popLast()!.string!
            dict[k] = v
        }
        #expect(dict == ["myfield": "myvalue", "myfield2": "myvalue2"])
    }

    @Test func hashMultiGet() async throws {
        guard ProcessInfo.processInfo.environment["REDIS_TESTS"] == "1" else { return }
        let client = try await RedisClient.connect()
        _ = try await client.flushAll()
        defer { Task { try? await client.close() } }
        let key = "mykey"
        let fv: [(String, RedisClient.RedisValue)] = [
            ("myfield", .string("myvalue")), ("myfield2", .string("myvalue2")), ("myfield3", .string("myvalue3"))
        ]
        _ = try await client.hashSet(key: key, fieldsValues: fv)
        let r = try await client.hashGet(key: key, fields: ["myfield3", "myfield2"])
        guard case .array(let a) = r else {
            Issue.record("Expected array, got \(r)"); return
        }
        #expect(a.map { $0.string } == ["myvalue3", "myvalue2"])
    }

    @Test func hashKeysValuesLen() async throws {
        guard ProcessInfo.processInfo.environment["REDIS_TESTS"] == "1" else { return }
        let client = try await RedisClient.connect()
        _ = try await client.flushAll()
        defer { Task { try? await client.close() } }
        let key = "mykey"
        let fv: [(String, RedisClient.RedisValue)] = [
            ("myfield", .string("myvalue")), ("myfield2", .string("myvalue2")), ("myfield3", .string("myvalue3"))
        ]
        _ = try await client.hashSet(key: key, fieldsValues: fv)
        let keysR = try await client.hashKeys(key: key)
        guard case .array(let ka) = keysR else {
            Issue.record("Expected array for HKEYS, got \(keysR)"); return
        }
        #expect(Set(ka.compactMap { $0.string }) == Set(fv.map { $0.0 }))
        let valsR = try await client.hashValues(key: key)
        guard case .array(let va) = valsR else {
            Issue.record("Expected array for HVALS, got \(valsR)"); return
        }
        #expect(Set(va.compactMap { $0.string }) == Set(["myvalue", "myvalue2", "myvalue3"]))
        let lenR = try await client.hashLength(key: key)
        #expect(lenR.integer == 3)
    }

    @Test func hashSetNX() async throws {
        guard ProcessInfo.processInfo.environment["REDIS_TESTS"] == "1" else { return }
        let client = try await RedisClient.connect()
        _ = try await client.flushAll()
        defer { Task { try? await client.close() } }
        let (key, field, value) = ("mykey", "myfield", "myvalue")
        let r1 = try await client.hashSetIfNonExists(key: key, field: field, value: .string(value))
        #expect(r1.integer == 1)
        let r2 = try await client.hashSetIfNonExists(key: key, field: field, value: .string(value))
        #expect(r2.integer == 0)
    }

    @Test func hashStrlen() async throws {
        guard ProcessInfo.processInfo.environment["REDIS_TESTS"] == "1" else { return }
        let client = try await RedisClient.connect()
        _ = try await client.flushAll()
        defer { Task { try? await client.close() } }
        let (key, field, value) = ("mykey", "myfield", "myvalue")
        _ = try await client.hashSet(key: key, field: field, value: .string(value))
        let r1 = try await client.hashStringLength(key: key, field: field)
        #expect(r1.integer == 7)
        let r2 = try await client.hashStringLength(key: key, field: "nonexisting")
        #expect(r2.integer == 0)
        let r3 = try await client.hashStringLength(key: "nonexisting", field: "nonexisting")
        #expect(r3.integer == 0)
    }

    @Test func hashIncrementBy() async throws {
        guard ProcessInfo.processInfo.environment["REDIS_TESTS"] == "1" else { return }
        let client = try await RedisClient.connect()
        _ = try await client.flushAll()
        defer { Task { try? await client.close() } }
        let (key, field) = ("mykey", "myfield")
        let r1 = try await client.hashIncrementBy(key: key, field: field, by: 1)
        #expect(r1.integer == 1)
        let r2 = try await client.hashIncrementBy(key: key, field: field, by: -1)
        #expect(r2.integer == 0)
        let r3 = try await client.hashIncrementBy(key: key, field: field, by: 1.5)
        guard case .bulkString = r3 else {
            Issue.record("Expected bulkString for HINCRBYFLOAT, got \(r3)"); return
        }
        let resultDouble = Double(r3.string ?? "0") ?? 0
        #expect(abs(resultDouble - 1.5) < 0.001)
    }

    @Test func hashScan() async throws {
        guard ProcessInfo.processInfo.environment["REDIS_TESTS"] == "1" else { return }
        let client = try await RedisClient.connect()
        _ = try await client.flushAll()
        defer { Task { try? await client.close() } }
        let key = "mykey"
        let fv: [(String, RedisClient.RedisValue)] = [("myfield", .string("myvalue")), ("myfield2", .string("myvalue2"))]
        _ = try await client.hashSet(key: key, fieldsValues: fv)
        let r = try await client.hashScan(key: key, cursor: 0)
        guard case .array(let a) = r, a.count == 2 else {
            Issue.record("Expected array[2] from HSCAN, got \(r)"); return
        }
        #expect(a[0].string == "0")
        guard case .array(var pairs) = a[1] else {
            Issue.record("Expected inner array, got \(a[1])"); return
        }
        var dict: [String: String] = [:]
        while pairs.count >= 2 {
            let v = pairs.popLast()!.string!
            let k = pairs.popLast()!.string!
            dict[k] = v
        }
        #expect(dict == ["myfield": "myvalue", "myfield2": "myvalue2"])
    }
}
