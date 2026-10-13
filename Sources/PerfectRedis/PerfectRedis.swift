import Foundation
// RediStack 1.6 predates Sendable annotations. RESPValue is a value enum over ByteBuffer,
// and RedisConnection hops every call onto its event loop, so both are safe to pass between
// tasks; the actor below is the only holder of the connection.
@preconcurrency import RediStack
import NIOCore
import NIOPosix
import NIOConcurrencyHelpers
import Logging

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

private let redisErrorPrefix = "(Redis) "

extension RedisResponse {
    init(_ value: RESPValue) {
        switch value {
        case .simpleString:
            self = .simpleString(value.string ?? "")
        case .bulkString(let buf):
            // RediStack decodes an empty bulk string ("$0") as .bulkString(nil) and a null
            // one ("$-1") as .null, so nil here is an empty value, not a missing one.
            if let buf {
                self = .bulkString(buf.getBytes(at: buf.readerIndex, length: buf.readableBytes) ?? [])
            } else {
                self = .bulkString([])
            }
        case .integer(let i):
            self = .integer(i)
        case .array(let arr):
            self = .array(arr.map { RedisResponse($0) })
        case .error(let e):
            // RedisError prefixes the server's text with "(Redis) ".
            var text = Substring(e.message)
            if text.hasPrefix(redisErrorPrefix) { text = text.dropFirst(redisErrorPrefix.count) }
            let parts = text.split(separator: " ", maxSplits: 1, omittingEmptySubsequences: false)
            self = .error(
                type: parts.first.map(String.init).flatMap { $0.isEmpty ? nil : $0 } ?? "ERR",
                msg: parts.dropFirst().first.map(String.init) ?? ""
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
    /// Seconds to wait for each command's reply before closing the connection and throwing
    /// `CommandError`, or nil (the default) to wait indefinitely. It also bounds blocking
    /// commands such as BLPOP, so give those a server-side timeout shorter than this.
    public let commandTimeout: Double?
    /// Where RediStack logs this connection's activity, or nil (the default) for nowhere.
    ///
    /// RediStack logs every command with its arguments at `debug` level (including the
    /// password sent with AUTH, and every value), every reply at `debug`, and every failed
    /// command at `error`. Pass a logger only where that is acceptable, and keep its level
    /// above `debug` in production.
    public let logger: Logger?

    public init() {
        host = "127.0.0.1"
        port = redisDefaultPort
        password = ""
        commandTimeout = nil
        logger = nil
    }

    public init(
        withHost host: String, port: Int, password: String = "",
        commandTimeout: Double? = nil, logger: Logger? = nil
    ) {
        self.host = host
        self.port = port
        self.password = password
        self.commandTimeout = commandTimeout
        self.logger = logger
    }

    /// The logger handed to RediStack.
    var connectionLogger: Logger {
        logger ?? Logger(label: "PerfectRedis", factory: { _ in SwiftLogNoOpLogHandler() })
    }
}

// MARK: - RedisValueRepresentable

public protocol RedisValueRepresentable: Sendable {
    var redisValue: RedisClient.RedisValue { get }
}

extension String: RedisValueRepresentable {
    public var redisValue: RedisClient.RedisValue { .string(self) }
}

/// Kept for source compatibility. Only `[UInt8]` is `RedisValueRepresentable`; conforming
/// another type to `ROctal` no longer makes its arrays representable.
public protocol ROctal: Sendable {}
extension UInt8: ROctal {}

extension Array: RedisValueRepresentable where Element == UInt8 {
    public var redisValue: RedisClient.RedisValue { .binary(self) }
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
    /// The connection's channel, for closing it outright: RediStack's own close() waits for
    /// outstanding replies, which never come from a server that has stopped answering.
    private nonisolated let channel: Channel
    private let commandTimeout: TimeAmount?

    /// Messages published to the channels and patterns this client subscribes to, in arrival
    /// order. The stream is unbounded and ends when the connection closes for any reason.
    /// It is shared by every subscription on this client, so iterate it from one task.
    public nonisolated let messages: AsyncStream<PublishedMessage>
    private nonisolated let messageSink: AsyncStream<PublishedMessage>.Continuation

    public static func connect(
        withIdentifier id: RedisClientIdentifier = RedisClientIdentifier()
    ) async throws -> RedisClient {
        var commandTimeout: TimeAmount?
        if let seconds = id.commandTimeout {
            guard seconds.isFinite, seconds > 0, seconds < 1e9 else {
                throw CommandError("commandTimeout must be a positive number of seconds, not \(seconds)")
            }
            commandTimeout = .nanoseconds(Int64(seconds * 1e9))
        }
        let eventLoop = MultiThreadedEventLoopGroup.singleton.next()
        let address = try SocketAddress.makeAddressResolvingHost(id.host, port: id.port)
        let password: String? = id.password.isEmpty ? nil : id.password
        let config = try RedisConnection.Configuration(
            address: address, password: password, defaultLogger: id.connectionLogger
        )
        let opened = NIOLockedValueBox<Channel?>(nil)
        // RediStack's own pipeline, with RESPReplyGuard in front of its reply decoder.
        let bootstrap = ClientBootstrap(group: eventLoop)
            .channelOption(.socketOption(.so_reuseaddr), value: 1)
            .channelInitializer { channel in
                opened.withLockedValue { $0 = channel }
                return channel.pipeline.addBaseRedisHandlers().flatMapThrowing {
                    try channel.pipeline.syncOperations.addHandler(
                        RESPReplyGuard(), name: "PerfectRedis.ReplyGuard", position: .first
                    )
                }
            }
        // commandTimeout also bounds connecting: the TCP connect, then AUTH and anything else
        // RediStack sends before handing over the connection.
        let timedOut = NIOLockedValueBox(false)
        var deadline: Scheduled<Void>?
        if let commandTimeout {
            _ = bootstrap.connectTimeout(commandTimeout)
            deadline = eventLoop.scheduleTask(in: commandTimeout) {
                timedOut.withLockedValue { $0 = true }
                opened.withLockedValue { $0 }?.close(promise: nil)
            }
        }
        let connection: RedisConnection
        do {
            connection = try await RedisConnection.make(
                configuration: config,
                boundEventLoop: eventLoop,
                configuredTCPClient: bootstrap
            ).get()
            deadline?.cancel()
            if timedOut.withLockedValue({ $0 }) {
                // The deadline closed the channel just as RediStack finished.
                throw CommandError("Connection closed by commandTimeout while connecting")
            }
        } catch {
            deadline?.cancel()
            // A failed AUTH leaves the socket open; RediStack doesn't close it.
            try? await opened.withLockedValue { $0 }?.close()
            if timedOut.withLockedValue({ $0 }) {
                let ms = (commandTimeout?.nanoseconds ?? 0) / 1_000_000
                throw CommandError("Couldn't connect and authenticate within \(ms) ms")
            }
            throw error
        }
        guard let channel = opened.withLockedValue({ $0 }) else {
            throw CommandError("Connected without a channel")
        }
        return RedisClient(connection: connection, channel: channel, commandTimeout: commandTimeout)
    }

    private init(connection: RedisConnection, channel: Channel, commandTimeout: TimeAmount?) {
        self.connection = connection
        self.channel = channel
        self.commandTimeout = commandTimeout
        let (stream, sink) = AsyncStream.makeStream(of: PublishedMessage.self)
        (messages, messageSink) = (stream, sink)
        // However the connection ends (close(), a timeout, the server dropping it), so does
        // the stream.
        channel.closeFuture.whenComplete { _ in sink.finish() }
    }

    deinit {
        messageSink.finish()
    }

    /// Closes the connection. Replies still outstanding get up to a second to arrive (a
    /// blocking command or a silent server would otherwise hold close() forever); then the
    /// connection is closed outright and those commands throw.
    public func close() async throws {
        messageSink.finish()
        let channel = self.channel
        connection.close()
        let deadline = channel.eventLoop.scheduleTask(in: .seconds(1)) { channel.close(promise: nil) }
        defer { deadline.cancel() }
        try await channel.closeFuture.get()
    }

    // All public commands route through here.
    func send(_ command: String, _ args: [RESPValue] = []) async throws -> RedisResponse {
        await waitForTransaction()
        guard !subscriptionMode else {
            throw CommandError(
                "\(command) can't be sent while this client has subscriptions; use another client, or unsubscribe from everything first"
            )
        }
        inFlight += 1
        defer { commandFinished() }
        return try await awaitReply(connection.send(command: command, with: args).map { RedisResponse($0) })
    }

    // MARK: Subscription mode

    /// Commands sent whose replies haven't arrived.
    private var inFlight = 0
    private var idleWaiters: [CheckedContinuation<Void, Never>] = []
    /// From the start of the first subscribe until the last subscription ends. Ordinary
    /// commands are refused meanwhile: RediStack's pub/sub handler inspects every reply, so a
    /// reply shaped like a subscription message (ordinary data can be) would be misread.
    private var subscriptionMode = false

    private func commandFinished() {
        inFlight -= 1
        guard inFlight == 0 else { return }
        let waiters = idleWaiters
        idleWaiters.removeAll()
        for waiter in waiters { waiter.resume() }
    }

    /// Enters subscription mode once every command already sent has its reply, so none of
    /// those replies pass through RediStack's pub/sub handler.
    func enterSubscriptionMode() async throws {
        if let current = Self.currentTransaction, current == transactionOwner {
            throw CommandError("subscribe can't be used inside multi(body:)")
        }
        await waitForTransaction()
        subscriptionMode = true
        while inFlight > 0 {
            await withCheckedContinuation { idleWaiters.append($0) }
        }
    }

    private var pendingSubscriptionChanges = 0

    func changeSubscriptions(entering: Bool, _ change: () -> EventLoopFuture<Void>) async throws {
        if entering {
            try await enterSubscriptionMode()
        } else {
            await waitForTransaction()
        }
        pendingSubscriptionChanges += 1
        defer {
            pendingSubscriptionChanges -= 1
            // Leave subscription mode once RediStack has no subscriptions left.
            subscriptionMode = connection.isSubscribed || pendingSubscriptionChanges > 0
        }
        try await awaitReply(change())
    }

    /// Awaits a reply, honouring commandTimeout and task cancellation. Either one closes the
    /// connection: the command has been sent, so abandoning only its reply would leave the
    /// server's next reply to be paired with whatever is sent next. The wait ends on the
    /// timeout or cancellation itself rather than on the future failing, because RediStack
    /// doesn't fail every future (pending subscription changes) when its channel closes.
    func awaitReply<Value: Sendable>(_ future: EventLoopFuture<Value>) async throws -> Value {
        let waiter = ReplyWaiter<Value>()
        let channel = self.channel
        let timeout = commandTimeout
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                guard waiter.install(continuation) else { return }
                future.whenComplete { waiter.finish($0) }
                if let timeout {
                    let timer = future.eventLoop.scheduleTask(in: timeout) {
                        let ms = timeout.nanoseconds / 1_000_000
                        waiter.finish(.failure(CommandError("No reply within \(ms) ms; the connection was closed")))
                        channel.close(promise: nil)
                    }
                    future.whenComplete { _ in timer.cancel() }
                }
            }
        } onCancel: {
            waiter.finish(.failure(CancellationError()))
            channel.close(promise: nil)
        }
    }

    // MARK: Transaction ownership

    /// The `multi(body:)` call the current task is running inside, if any. Child tasks
    /// inherit it, so structured concurrency inside the body still reaches the server.
    @TaskLocal static var currentTransaction: UUID?
    /// The transaction whose MULTI is open on the connection.
    private var transactionOwner: UUID?
    private var transactionWaiters: [CheckedContinuation<Void, Never>] = []

    /// Suspends while a `multi(body:)` call that the current task isn't part of is open, so
    /// other callers' commands don't get queued into it.
    func waitForTransaction() async {
        while let owner = transactionOwner, owner != Self.currentTransaction {
            await withCheckedContinuation { transactionWaiters.append($0) }
        }
    }

    func beginTransaction() async throws -> UUID {
        if let current = Self.currentTransaction, current == transactionOwner {
            throw CommandError("multi(body:) calls cannot be nested")
        }
        await waitForTransaction()
        let id = UUID()
        transactionOwner = id
        return id
    }

    func endTransaction() {
        transactionOwner = nil
        let waiters = transactionWaiters
        transactionWaiters.removeAll()
        for waiter in waiters { waiter.resume() }
    }
}

// Helper: convert String to RESPValue argument.
@inline(__always)
private func arg(_ s: String) -> RESPValue { RESPValue(from: s) }

/// Seconds as a whole number of milliseconds, the unit the P* commands take. Throws
/// rather than trapping when the value is NaN, infinite or beyond Int's range.
private func milliseconds(_ seconds: Double) throws -> String {
    let ms = (seconds * 1000).rounded(.towardZero)
    guard ms.isFinite, ms >= Double(Int.min), ms < Double(Int.max) else {
        throw RedisClient.CommandError("\(seconds) seconds is not a representable number of milliseconds")
    }
    return String(Int(ms))
}

// MARK: - sendCommandAsRESP

public extension RedisClient {
    /// Sends any command, with its name and every parameter as a RESP bulk string.
    ///
    /// The connection pairs each command with exactly one RESP2 reply, so this throws
    /// `CommandError`, without sending anything, for commands that would break that pairing
    /// and leave later replies matched to the wrong commands: the (P/S)SUBSCRIBE family (use
    /// `subscribe` and `messages`), MONITOR, SYNC, PSYNC, CLIENT REPLY OFF or SKIP, HELLO
    /// asking for any protocol other than 2, and RESET.
    func sendCommandAsRESP(name: String, parameters: [String]) async throws -> RedisResponse {
        if let reason = Self.breaksReplyPairing(name: name, parameters: parameters) {
            throw CommandError("\(name) is not supported through sendCommandAsRESP: \(reason)")
        }
        return try await send(name, parameters.map { arg($0) })
    }
}

extension RedisClient {
    static func breaksReplyPairing(name: String, parameters: [String]) -> String? {
        switch name.uppercased() {
        case "SUBSCRIBE", "PSUBSCRIBE", "SSUBSCRIBE", "UNSUBSCRIBE", "PUNSUBSCRIBE", "SUNSUBSCRIBE":
            return "subscription replies arrive unrequested; use subscribe(channels:) or subscribe(patterns:)"
        case "MONITOR", "SYNC", "PSYNC":
            return "the server streams replies that no command asked for"
        case "RESET":
            return "it silently ends subscriptions and transactions the client is tracking; use close() and reconnect"
        case "CLIENT":
            guard parameters.first?.uppercased() == "REPLY",
                  parameters.dropFirst().first?.uppercased() != "ON" else { return nil }
            return "commands whose replies are suppressed would be paired with later replies"
        case "HELLO":
            guard let version = parameters.first, version != "2" else { return nil }
            return "only RESP2 is supported, so HELLO may only ask for protocol 2"
        default:
            return nil
        }
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
        try await send("CLIENT", [arg("PAUSE"), arg(try milliseconds(timeoutSeconds))])
    }
}

// MARK: - Key/value

public extension RedisClient {

    func set(
        key: String, value: RedisValue,
        expires: Double = 0, ifNotExists: Bool = false, ifExists: Bool = false
    ) async throws -> RedisResponse {
        var args: [RESPValue] = [arg(key), value.resp]
        if expires != 0 { args += [arg("PX"), arg(try milliseconds(expires))] }
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
        try await send("PEXPIRE", [arg(key), arg(try milliseconds(seconds))])
    }
    func expireAt(key: String, seconds: Double) async throws -> RedisResponse {
        try await send("PEXPIREAT", [arg(key), arg(try milliseconds(seconds))])
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

    /// Runs `body` inside MULTI…EXEC and returns EXEC's reply, or sends DISCARD and rethrows
    /// if `body` throws.
    ///
    /// Until it returns, commands this client receives from other tasks wait rather than
    /// being queued into the transaction. Commands from `body` and from child tasks it
    /// creates go through. Calls nested inside `body` throw `CommandError`. Calling
    /// `multiBegin()` and `multiExec()` yourself has no such protection.
    func multi(body: () async throws -> ()) async throws -> RedisResponse {
        let id = try await beginTransaction()
        defer { endTransaction() }
        return try await Self.$currentTransaction.withValue(id) {
            _ = try await multiBegin()
            do {
                try await body()
                return try await multiExec()
            } catch {
                _ = try? await multiDiscard()
                throw error
            }
        }
    }
}

// MARK: - Pub/sub

public extension RedisClient {
    /// A message delivered to one of this client's subscriptions.
    struct PublishedMessage: Sendable {
        /// The channel it was published to (for a pattern subscription, the matching channel).
        public let channel: String
        /// The published payload, normally a `.bulkString`.
        public let message: RedisResponse
    }

    /// Subscribes to channels matching each glob-style pattern. Messages arrive on `messages`.
    ///
    /// Commands this client has already sent finish first. From then until the last
    /// subscription ends, the client refuses every other command with `CommandError`, so
    /// publish and do other work from a second client.
    func subscribe(patterns: [String]) async throws {
        guard !patterns.isEmpty else { throw CommandError("subscribe(patterns:) needs at least one pattern") }
        let sink = messageSink
        try await changeSubscriptions(entering: true) {
            connection.psubscribe(
                to: patterns,
                messageReceiver: { channel, value in
                    sink.yield(PublishedMessage(channel: channel.rawValue, message: RedisResponse(value)))
                },
                onSubscribe: nil,
                onUnsubscribe: nil
            )
        }
    }

    /// Subscribes to channels. Messages arrive on `messages`.
    ///
    /// Commands this client has already sent finish first. From then until the last
    /// subscription ends, the client refuses every other command with `CommandError`, so
    /// publish and do other work from a second client.
    func subscribe(channels: [String]) async throws {
        guard !channels.isEmpty else { throw CommandError("subscribe(channels:) needs at least one channel") }
        let sink = messageSink
        try await changeSubscriptions(entering: true) {
            connection.subscribe(
                to: channels.map { RedisChannelName($0) },
                messageReceiver: { channel, value in
                    sink.yield(PublishedMessage(channel: channel.rawValue, message: RedisResponse(value)))
                },
                onSubscribe: nil,
                onUnsubscribe: nil
            )
        }
    }

    /// Unsubscribes from the given patterns, or from every pattern when the list is empty.
    func unsubscribe(patterns: [String]) async throws {
        try await changeSubscriptions(entering: false) { connection.punsubscribe(from: patterns) }
    }

    /// Unsubscribes from the given channels, or from every channel when the list is empty.
    func unsubscribe(channels: [String]) async throws {
        try await changeSubscriptions(entering: false) {
            connection.unsubscribe(from: channels.map { RedisChannelName($0) })
        }
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

/// Resumes a continuation exactly once, with whichever of a reply, a timeout or a
/// cancellation comes first. A result that arrives before the continuation is installed
/// is kept and delivered when it is.
final class ReplyWaiter<Value: Sendable>: Sendable {
    private enum State {
        case idle
        case waiting(CheckedContinuation<Value, Error>)
        case finishedEarly(Result<Value, Error>)
        case done
    }
    private let state = NIOLockedValueBox(State.idle)

    /// Returns false if the waiter already finished; the continuation has then been resumed.
    func install(_ continuation: CheckedContinuation<Value, Error>) -> Bool {
        let early: Result<Value, Error>? = state.withLockedValue { state in
            if case .finishedEarly(let result) = state {
                state = .done
                return result
            }
            state = .waiting(continuation)
            return nil
        }
        guard let early else { return true }
        continuation.resume(with: early)
        return false
    }

    func finish(_ result: Result<Value, Error>) {
        let continuation: CheckedContinuation<Value, Error>? = state.withLockedValue { state in
            switch state {
            case .idle:
                state = .finishedEarly(result)
                return nil
            case .waiting(let continuation):
                state = .done
                return continuation
            case .finishedEarly, .done:
                return nil
            }
        }
        continuation?.resume(with: result)
    }
}
