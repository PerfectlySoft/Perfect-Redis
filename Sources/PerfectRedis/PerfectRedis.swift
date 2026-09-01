import Foundation
import RediStack
import NIOCore
import NIOPosix

public let redisDefaultPort = 6379

// MARK: - RedisResponse

public enum RedisResponse: Sendable {
    case error(type: String, msg: String)
    case simpleString(String)
    case bulkString([UInt8]?)
    case integer(Int)
    case array([RedisResponse])

    public var isSimpleOK: Bool {
        guard case .simpleString(let s) = self, s == "OK" else { return false }
        return true
    }

    public var isNil: Bool {
        if case .bulkString(let b) = self, b == nil { return true }
        return false
    }

    public var value: RedisClient.RedisValue? {
        switch self {
        case .simpleString(let s): return .string(s)
        case .bulkString(let b):
            guard let b else { return nil }
            return .binary(b)
        default: return nil
        }
    }

    public var string: String? {
        switch self {
        case .error(let type, let msg): return "\(type) \(msg)"
        case .simpleString(let s): return s
        case .bulkString(let b):
            guard let b else { return nil }
            return String(bytes: b, encoding: .utf8)
        case .integer(let i): return "\(i)"
        case .array(let a):
            return "[" + a.map { $0.string ?? "nil" }.joined(separator: ", ") + "]"
        }
    }

    public var integer: Int {
        guard case .integer(let i) = self else { return 0 }
        return i
    }
}

// MARK: - RESPValue → RedisResponse

extension RedisResponse {
    init(_ value: RESPValue) {
        switch value {
        case .simpleString:
            self = .simpleString(value.string ?? "")
        case .bulkString(let buf):
            if let buf {
                self = .bulkString(buf.getBytes(at: buf.readerIndex, length: buf.readableBytes))
            } else {
                self = .bulkString(nil)
            }
        case .integer(let i):
            self = .integer(i)
        case .array(let arr):
            self = .array(arr.map { RedisResponse($0) })
        case .error(let e):
            let parts = e.message.split(separator: " ", maxSplits: 1)
            self = .error(
                type: parts.first.map(String.init) ?? "ERR",
                msg: parts.dropFirst().first.map(String.init) ?? e.message
            )
        case .null:
            self = .bulkString(nil)
        }
    }
}

// MARK: - RedisClientIdentifier

public struct RedisClientIdentifier: Sendable {
    public let host: String
    public let port: Int
    public let password: String

    public init() {
        host = "127.0.0.1"
        port = redisDefaultPort
        password = ""
    }

    public init(withHost host: String, port: Int, password: String = "") {
        self.host = host
        self.port = port
        self.password = password
    }
}

// MARK: - RedisValueRepresentable

public protocol RedisValueRepresentable: Sendable {
    var redisValue: RedisClient.RedisValue { get }
}

extension String: RedisValueRepresentable {
    public var redisValue: RedisClient.RedisValue { .string(self) }
}

public protocol ROctal: Sendable {}
extension UInt8: ROctal {}

extension Array: RedisValueRepresentable where Element: ROctal {
    public var redisValue: RedisClient.RedisValue { .binary(self as! [UInt8]) }
}

// MARK: - RedisClient

public actor RedisClient {

    public struct CommandError: Error, CustomStringConvertible, Sendable {
        public let description: String
        init(_ msg: String) { description = msg }
    }

    public enum RedisValue: RedisValueRepresentable, Sendable {
        case string(String)
        case binary([UInt8])

        public var redisValue: RedisClient.RedisValue { self }

        public var string: String? {
            switch self {
            case .string(let s): return s
            case .binary(let b): return String(bytes: b, encoding: .utf8)
            }
        }

        // RESPValue representation for sending to Redis.
        var resp: RESPValue {
            switch self {
            case .string(let s): return RESPValue(from: s)
            case .binary(let b): return RESPValue(from: Foundation.Data(b))
            }
        }
    }

    private let connection: RedisConnection

    public static func connect(
        withIdentifier id: RedisClientIdentifier = RedisClientIdentifier()
    ) async throws -> RedisClient {
        let eventLoop = MultiThreadedEventLoopGroup.singleton.next()
        let address = try SocketAddress.makeAddressResolvingHost(id.host, port: id.port)
        let password: String? = id.password.isEmpty ? nil : id.password
        let config = try RedisConnection.Configuration(address: address, password: password)
        let connection = try await RedisConnection.make(
            configuration: config,
            boundEventLoop: eventLoop
        ).get()
        return RedisClient(connection: connection)
    }

    private init(connection: RedisConnection) {
        self.connection = connection
    }

    public func close() async throws {
        _ = try await connection.close().get()
    }

    // All public commands route through here.
    func send(_ command: String, _ args: [RESPValue] = []) async throws -> RedisResponse {
        let result = try await connection.send(command: command, with: args).get()
        return RedisResponse(result)
    }
}

// Helper: convert String to RESPValue argument.
@inline(__always)
private func arg(_ s: String) -> RESPValue { RESPValue(from: s) }

// MARK: - sendCommandAsRESP

public extension RedisClient {
    func sendCommandAsRESP(name: String, parameters: [String]) async throws -> RedisResponse {
        try await send(name, parameters.map { arg($0) })
    }
}

// MARK: - Connection

public extension RedisClient {
    func auth(withPassword password: String) async throws -> RedisResponse {
        try await send("AUTH", [arg(password)])
    }
    func ping() async throws -> RedisResponse {
        try await send("PING")
    }
}

// MARK: - Database meta

public extension RedisClient {
    func flushAll() async throws -> RedisResponse { try await send("FLUSHALL") }
    func save() async throws -> RedisResponse { try await send("SAVE") }
    func backgroundSave() async throws -> RedisResponse { try await send("BGSAVE") }
    func lastSave() async throws -> RedisResponse { try await send("LASTSAVE") }
    func rewriteAppendOnlyFile() async throws -> RedisResponse { try await send("BGREWRITEAOF") }
    func dbSize() async throws -> RedisResponse { try await send("DBSIZE") }
    func keys(pattern: String) async throws -> RedisResponse { try await send("KEYS", [arg(pattern)]) }
    func randomKey() async throws -> RedisResponse { try await send("RANDOMKEY") }
    func select(index: Int) async throws -> RedisResponse { try await send("SELECT", [arg(String(index))]) }
}

// MARK: - Client operations

public extension RedisClient {

    enum KillFilter: Sendable {
        case addr(ip: String, port: Int)
        case id(String)
        case typeNormal, typeMaster, typeSlave, typePubSub

        var args: [RESPValue] {
            switch self {
            case .addr(let ip, let port): return [arg("ADDR"), arg("\(ip):\(port)")]
            case .id(let id):             return [arg("ID"), arg(id)]
            case .typeNormal:             return [arg("TYPE"), arg("normal")]
            case .typeMaster:             return [arg("TYPE"), arg("master")]
            case .typeSlave:              return [arg("TYPE"), arg("slave")]
            case .typePubSub:             return [arg("TYPE"), arg("pubsub")]
            }
        }
    }

    func clientList() async throws -> RedisResponse {
        try await send("CLIENT", [arg("LIST")])
    }
    func clientGetName() async throws -> RedisResponse {
        try await send("CLIENT", [arg("GETNAME")])
    }
    func clientSetName(to name: String) async throws -> RedisResponse {
        try await send("CLIENT", [arg("SETNAME"), arg(name)])
    }
    func clientKill(filters: [KillFilter], skipMe: Bool = true) async throws -> RedisResponse {
        var args: [RESPValue] = [arg("KILL")]
        for f in filters { args += f.args }
        args += [arg("SKIPME"), arg(skipMe ? "yes" : "no")]
        return try await send("CLIENT", args)
    }
    func clientPause(timeoutSeconds: Double) async throws -> RedisResponse {
        try await send("CLIENT", [arg("PAUSE"), arg(String(Int(timeoutSeconds * 1000)))])
    }
}

// MARK: - Key/value

public extension RedisClient {

    func set(
        key: String, value: RedisValue,
        expires: Double = 0, ifNotExists: Bool = false, ifExists: Bool = false
    ) async throws -> RedisResponse {
        var args: [RESPValue] = [arg(key), value.resp]
        if expires != 0 { args += [arg("PX"), arg(String(Int(expires * 1000)))] }
        if ifNotExists { args.append(arg("NX")) }
        else if ifExists { args.append(arg("XX")) }
        return try await send("SET", args)
    }

    func set(keysValues: [(String, RedisValue)]) async throws -> RedisResponse {
        let args = keysValues.flatMap { [arg($0.0), $0.1.resp] }
        return try await send("MSET", args)
    }

    func setIfNonExists(keysValues: [(String, RedisValue)]) async throws -> RedisResponse {
        let args = keysValues.flatMap { [arg($0.0), $0.1.resp] }
        return try await send("MSETNX", args)
    }

    func get(key: String) async throws -> RedisResponse {
        try await send("GET", [arg(key)])
    }

    func get(keys: [String]) async throws -> RedisResponse {
        try await send("MGET", keys.map { arg($0) })
    }

    func getSet(key: String, newValue: RedisValue) async throws -> RedisResponse {
        try await send("GETSET", [arg(key), newValue.resp])
    }

    @discardableResult
    func delete(keys: String...) async throws -> RedisResponse {
        try await send("DEL", keys.map { arg($0) })
    }

    func increment(key: String) async throws -> RedisResponse { try await send("INCR", [arg(key)]) }
    func increment(key: String, by: Int) async throws -> RedisResponse { try await send("INCRBY", [arg(key), arg(String(by))]) }
    func increment(key: String, by: Double) async throws -> RedisResponse { try await send("INCRBYFLOAT", [arg(key), arg(String(by))]) }
    func decrement(key: String) async throws -> RedisResponse { try await send("DECR", [arg(key)]) }
    func decrement(key: String, by: Int) async throws -> RedisResponse { try await send("DECRBY", [arg(key), arg(String(by))]) }

    func rename(key: String, newKey: String) async throws -> RedisResponse {
        try await send("RENAME", [arg(key), arg(newKey)])
    }
    func renameIfnotExists(key: String, newKey: String) async throws -> RedisResponse {
        try await send("RENAMENX", [arg(key), arg(newKey)])
    }

    func exists(keys: String...) async throws -> RedisResponse {
        guard !keys.isEmpty else { return .array([]) }
        return try await send("EXISTS", keys.map { arg($0) })
    }

    func append(key: String, value: RedisValue) async throws -> RedisResponse {
        try await send("APPEND", [arg(key), value.resp])
    }

    func expire(key: String, seconds: Double) async throws -> RedisResponse {
        try await send("PEXPIRE", [arg(key), arg(String(Int(seconds * 1000)))])
    }
    func expireAt(key: String, seconds: Double) async throws -> RedisResponse {
        try await send("PEXPIREAT", [arg(key), arg(String(Int(seconds * 1000)))])
    }
    func timeToExpire(key: String) async throws -> RedisResponse {
        try await send("PTTL", [arg(key)])
    }
    func persist(key: String) async throws -> RedisResponse {
        try await send("PERSIST", [arg(key)])
    }
}

// MARK: - Bit operations

public extension RedisClient {

    enum IntegerType: Sendable {
        case signed(Int), unsigned(Int)
        var arg: String { switch self { case .signed(let i): return "i\(i)"; case .unsigned(let i): return "u\(i)" } }
    }

    enum SubCommand: Sendable {
        case get(type: IntegerType, offset: Int)
        case set(type: IntegerType, offset: Int, value: Int)
        case setMul(type: IntegerType, offset: String, value: Int)
        case incrby(type: IntegerType, offset: Int, increment: Int)
        case overflowWrap, overflowSat, overflowFail

        var args: [RESPValue] {
            switch self {
            case .get(let t, let o):
                return [RediStack.RESPValue(from: "GET"), RediStack.RESPValue(from: t.arg), RediStack.RESPValue(from: String(o))]
            case .set(let t, let o, let v):
                return [RediStack.RESPValue(from: "SET"), RediStack.RESPValue(from: t.arg), RediStack.RESPValue(from: String(o)), RediStack.RESPValue(from: String(v))]
            case .setMul(let t, let o, let v):
                return [RediStack.RESPValue(from: "SET"), RediStack.RESPValue(from: t.arg), RediStack.RESPValue(from: "#\(o)"), RediStack.RESPValue(from: String(v))]
            case .incrby(let t, let o, let inc):
                return [RediStack.RESPValue(from: "INCRBY"), RediStack.RESPValue(from: t.arg), RediStack.RESPValue(from: String(o)), RediStack.RESPValue(from: String(inc))]
            case .overflowWrap: return [RediStack.RESPValue(from: "OVERFLOW"), RediStack.RESPValue(from: "WRAP")]
            case .overflowSat:  return [RediStack.RESPValue(from: "OVERFLOW"), RediStack.RESPValue(from: "SAT")]
            case .overflowFail: return [RediStack.RESPValue(from: "OVERFLOW"), RediStack.RESPValue(from: "FAIL")]
            }
        }
    }

    enum BitOperation: Sendable {
        case and, or, xor, not
        var arg: String { switch self { case .and: return "AND"; case .or: return "OR"; case .xor: return "XOR"; case .not: return "NOT" } }
    }

    func bitCount(key: String) async throws -> RedisResponse {
        try await send("BITCOUNT", [arg(key)])
    }
    func bitCount(key: String, start: Int, end: Int) async throws -> RedisResponse {
        try await send("BITCOUNT", [arg(key), arg(String(start)), arg(String(end))])
    }
    func bitField(key: String, commands: [SubCommand]) async throws -> RedisResponse {
        let args = [arg(key)] + commands.flatMap { $0.args }
        return try await send("BITFIELD", args)
    }
    func bitOp(_ op: BitOperation, destKey: String, srcKeys: String...) async throws -> RedisResponse {
        let args = [arg(op.arg), arg(destKey)] + srcKeys.map { arg($0) }
        return try await send("BITOP", args)
    }
    func bitPos(key: String, position: Int) async throws -> RedisResponse {
        try await send("BITPOS", [arg(key), arg(String(position))])
    }
    func bitPos(key: String, position: Int, start: Int) async throws -> RedisResponse {
        try await send("BITPOS", [arg(key), arg(String(position)), arg(String(start))])
    }
    func bitPos(key: String, position: Int, start: Int, end: Int) async throws -> RedisResponse {
        try await send("BITPOS", [arg(key), arg(String(position)), arg(String(start)), arg(String(end))])
    }
    func bitGet(key: String, offset: Int) async throws -> RedisResponse {
        try await send("GETBIT", [arg(key), arg(String(offset))])
    }
    func bitSet(key: String, offset: Int, value: Bool) async throws -> RedisResponse {
        try await send("SETBIT", [arg(key), arg(String(offset)), arg(value ? "1" : "0")])
    }
}

// MARK: - List operations

public extension RedisClient {
    func listPrepend(key: String, values: [RedisValue]) async throws -> RedisResponse {
        try await send("LPUSH", [arg(key)] + values.map { $0.resp })
    }
    func listAppend(key: String, values: [RedisValue]) async throws -> RedisResponse {
        try await send("RPUSH", [arg(key)] + values.map { $0.resp })
    }
    func listPrependX(key: String, value: RedisValue) async throws -> RedisResponse {
        try await send("LPUSHX", [arg(key), value.resp])
    }
    func listAppendX(key: String, value: RedisValue) async throws -> RedisResponse {
        try await send("RPUSHX", [arg(key), value.resp])
    }
    func listPopFirst(key: String) async throws -> RedisResponse { try await send("LPOP", [arg(key)]) }
    func listPopLast(key: String) async throws -> RedisResponse  { try await send("RPOP", [arg(key)]) }
    func listPopLastAppend(sourceKey: String, destKey: String) async throws -> RedisResponse {
        try await send("RPOPLPUSH", [arg(sourceKey), arg(destKey)])
    }
    func listPopLastAppendBlocking(sourceKey: String, destKey: String, timeout: Int = 0) async throws -> RedisResponse {
        try await send("BRPOPLPUSH", [arg(sourceKey), arg(destKey), arg(String(timeout))])
    }
    func listPopFirstBlocking(keys: String..., timeout: Int) async throws -> RedisResponse {
        try await send("BLPOP", keys.map { arg($0) } + [arg(String(timeout))])
    }
    func listPopLastBlocking(keys: String..., timeout: Int) async throws -> RedisResponse {
        try await send("BRPOP", keys.map { arg($0) } + [arg(String(timeout))])
    }
    func listLength(key: String) async throws -> RedisResponse { try await send("LLEN", [arg(key)]) }
    func listTrim(key: String, start: Int, stop: Int) async throws -> RedisResponse {
        try await send("LTRIM", [arg(key), arg(String(start)), arg(String(stop))])
    }
    func listRange(key: String, start: Int, stop: Int) async throws -> RedisResponse {
        try await send("LRANGE", [arg(key), arg(String(start)), arg(String(stop))])
    }
    func listGetElement(key: String, index: Int) async throws -> RedisResponse {
        try await send("LINDEX", [arg(key), arg(String(index))])
    }
    func listInsert(key: String, element: RedisValue, before: RedisValue) async throws -> RedisResponse {
        try await send("LINSERT", [arg(key), arg("BEFORE"), before.resp, element.resp])
    }
    func listInsert(key: String, element: RedisValue, after: RedisValue) async throws -> RedisResponse {
        try await send("LINSERT", [arg(key), arg("AFTER"), after.resp, element.resp])
    }
    func listSet(key: String, index: Int, value: RedisValue) async throws -> RedisResponse {
        try await send("LSET", [arg(key), arg(String(index)), value.resp])
    }
    func listRemoveMatching(key: String, value: RedisValue, count: Int) async throws -> RedisResponse {
        try await send("LREM", [arg(key), arg(String(count)), value.resp])
    }
}

// MARK: - Transaction (multi)

public extension RedisClient {
    func multiBegin() async throws -> RedisResponse   { try await send("MULTI") }
    func multiExec() async throws -> RedisResponse    { try await send("EXEC") }
    func multiDiscard() async throws -> RedisResponse { try await send("DISCARD") }
    func multiWatch(keys: [String]) async throws -> RedisResponse {
        try await send("WATCH", keys.map { arg($0) })
    }
    func multiUnwatch() async throws -> RedisResponse { try await send("UNWATCH") }

    func multi(body: () async throws -> ()) async throws -> RedisResponse {
        _ = try await multiBegin()
        do {
            try await body()
            return try await multiExec()
        } catch {
            _ = try await multiDiscard()
            throw error
        }
    }
}

// MARK: - Pub/sub

public extension RedisClient {
    func subscribe(patterns: [String]) async throws -> RedisResponse {
        try await send("PSUBSCRIBE", patterns.map { arg($0) })
    }
    func subscribe(channels: [String]) async throws -> RedisResponse {
        try await send("SUBSCRIBE", channels.map { arg($0) })
    }
    func unsubscribe(patterns: [String]) async throws -> RedisResponse {
        try await send("PUNSUBSCRIBE", patterns.map { arg($0) })
    }
    func unsubscribe(channels: [String]) async throws -> RedisResponse {
        try await send("UNSUBSCRIBE", channels.map { arg($0) })
    }
    func publish(channel: String, message: RedisValue) async throws -> RedisResponse {
        try await send("PUBLISH", [arg(channel), message.resp])
    }
}

// MARK: - Set operations

public extension RedisClient {
    func setAdd(key: String, elements: [RedisValue]) async throws -> RedisResponse {
        try await send("SADD", [arg(key)] + elements.map { $0.resp })
    }
    func setCount(key: String) async throws -> RedisResponse { try await send("SCARD", [arg(key)]) }
    func setDifference(key: String, againstKeys: [String]) async throws -> RedisResponse {
        try await send("SDIFF", [arg(key)] + againstKeys.map { arg($0) })
    }
    func setStoreDifference(into: String, ofKey: String, againstKeys: [String]) async throws -> RedisResponse {
        try await send("SDIFFSTORE", [arg(into), arg(ofKey)] + againstKeys.map { arg($0) })
    }
    func setIntersection(key: String, againstKeys: [String]) async throws -> RedisResponse {
        try await send("SINTER", [arg(key)] + againstKeys.map { arg($0) })
    }
    func setStoreIntersection(into: String, ofKey: String, againstKeys: [String]) async throws -> RedisResponse {
        try await send("SINTERSTORE", [arg(into), arg(ofKey)] + againstKeys.map { arg($0) })
    }
    func setUnion(key: String, againstKeys: [String]) async throws -> RedisResponse {
        try await send("SUNION", [arg(key)] + againstKeys.map { arg($0) })
    }
    func setStoreUnion(into: String, ofKey: String, againstKeys: [String]) async throws -> RedisResponse {
        try await send("SUNIONSTORE", [arg(into), arg(ofKey)] + againstKeys.map { arg($0) })
    }
    func setContains(key: String, value: RedisValue) async throws -> RedisResponse {
        try await send("SISMEMBER", [arg(key), value.resp])
    }
    func setMembers(key: String) async throws -> RedisResponse { try await send("SMEMBERS", [arg(key)]) }
    func setMove(fromKey: String, toKey: String, value: RedisValue) async throws -> RedisResponse {
        try await send("SMOVE", [arg(fromKey), arg(toKey), value.resp])
    }
    func setRandomPop(key: String, count: Int) async throws -> RedisResponse {
        try await send("SPOP", [arg(key), arg(String(count))])
    }
    func setRandomPop(key: String) async throws -> RedisResponse {
        try await send("SPOP", [arg(key)])
    }
    func setRandomGet(key: String, count: Int) async throws -> RedisResponse {
        try await send("SRANDMEMBER", [arg(key), arg(String(count))])
    }
    func setRandomGet(key: String) async throws -> RedisResponse {
        try await send("SRANDMEMBER", [arg(key)])
    }
    func setRemove(key: String, value: RedisValue) async throws -> RedisResponse {
        try await send("SREM", [arg(key), value.resp])
    }
    func setRemove(key: String, values: [RedisValue]) async throws -> RedisResponse {
        try await send("SREM", [arg(key)] + values.map { $0.resp })
    }
    func setScan(key: String, cursor: Int = 0, pattern: String? = nil, count: Int? = nil) async throws -> RedisResponse {
        var args: [RESPValue] = [arg(key), arg(String(cursor))]
        if let p = pattern { args += [arg("MATCH"), arg(p)] }
        if let c = count   { args += [arg("COUNT"), arg(String(c))] }
        return try await send("SSCAN", args)
    }
}

// MARK: - Hash operations

public extension RedisClient {
    func hashSet(key: String, field: String, value: RedisValue) async throws -> RedisResponse {
        try await send("HSET", [arg(key), arg(field), value.resp])
    }
    func hashSet(key: String, fieldsValues: [(String, RedisValue)]) async throws -> RedisResponse {
        let args = [arg(key)] + fieldsValues.flatMap { [arg($0.0), $0.1.resp] }
        return try await send("HMSET", args)
    }
    func hashSetIfNonExists(key: String, field: String, value: RedisValue) async throws -> RedisResponse {
        try await send("HSETNX", [arg(key), arg(field), value.resp])
    }
    func hashGet(key: String, field: String) async throws -> RedisResponse {
        try await send("HGET", [arg(key), arg(field)])
    }
    func hashGet(key: String, fields: [String]) async throws -> RedisResponse {
        try await send("HMGET", [arg(key)] + fields.map { arg($0) })
    }
    func hashExists(key: String, field: String) async throws -> RedisResponse {
        try await send("HEXISTS", [arg(key), arg(field)])
    }
    func hashDel(key: String, fields: String...) async throws -> RedisResponse {
        try await send("HDEL", [arg(key)] + fields.map { arg($0) })
    }
    func hashDel(key: String, fields: [String]) async throws -> RedisResponse {
        try await send("HDEL", [arg(key)] + fields.map { arg($0) })
    }
    func hashGetAll(key: String) async throws -> RedisResponse { try await send("HGETALL", [arg(key)]) }
    func hashKeys(key: String) async throws -> RedisResponse   { try await send("HKEYS", [arg(key)]) }
    func hashValues(key: String) async throws -> RedisResponse { try await send("HVALS", [arg(key)]) }
    func hashLength(key: String) async throws -> RedisResponse { try await send("HLEN", [arg(key)]) }
    func hashStringLength(key: String, field: String) async throws -> RedisResponse {
        try await send("HSTRLEN", [arg(key), arg(field)])
    }
    func hashIncrementBy(key: String, field: String, by: Int) async throws -> RedisResponse {
        try await send("HINCRBY", [arg(key), arg(field), arg(String(by))])
    }
    func hashIncrementBy(key: String, field: String, by: Double) async throws -> RedisResponse {
        try await send("HINCRBYFLOAT", [arg(key), arg(field), arg(String(by))])
    }
    func hashScan(key: String, cursor: Int = 0, pattern: String? = nil, count: Int? = nil) async throws -> RedisResponse {
        var args: [RESPValue] = [arg(key), arg(String(cursor))]
        if let p = pattern { args += [arg("MATCH"), arg(p)] }
        if let c = count   { args += [arg("COUNT"), arg(String(c))] }
        return try await send("HSCAN", args)
    }
}
