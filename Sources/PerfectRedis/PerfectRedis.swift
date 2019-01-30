//
//  PerfectRedis.swift
//  Perfect-Redis
//
//  Created by Kyle Jessup on 2016-06-03.
//	Copyright (C) 2016 PerfectlySoft, Inc.
//
//===----------------------------------------------------------------------===//
//
// This source file is part of the Perfect.org open source project
//
// Copyright (c) 2015 - 2016 PerfectlySoft Inc. and the Perfect project authors
// Licensed under Apache License v2.0
//
// See http://perfect.org/licensing.html for license information
//
//===----------------------------------------------------------------------===//
//

import PerfectNet
import PerfectCrypto
import PerfectThread
import Dispatch

public let redisDefaultPort = 6379
let redisDefaultNetTimeout = 5.0
let redisDefaultReadSize = 2048

let cr: UInt8 = 13
let lf: UInt8 = 10
let sp: UInt8 = 32

extension String {
	var bytes: [UInt8] { return Array(utf8) }
}

extension UInt8 {
	var hexString: String {
		var s = ""
		let b = self >> 4
		s.append(String(Character(UnicodeScalar(b > 9 ? b - 10 + 65 : b + 48))))
		let b2 = self & 0x0F
		s.append(String(Character(UnicodeScalar(b2 > 9 ? b2 - 10 + 65 : b2 + 48))))
		return s
	}
}

private func s(_ a: [UInt8]) -> String {
	return String(validatingUTF8: a) ?? ""
}

private func hexString(fromArray: [UInt8]) -> String {
	var s = ""
	for v in fromArray {
		s.append("\\x")
		let b = v >> 4
		s.append(String(Character(UnicodeScalar(b > 9 ? b - 10 + 65 : b + 48))))
		let b2 = v & 0x0F
		s.append(String(Character(UnicodeScalar(b2 > 9 ? b2 - 10 + 65 : b2 + 48))))
	}
	return s
}

public enum RedisResponse {
	case error(type: String, msg: String)
	case simpleString(String)
	case bulkString([UInt8]?)
	case integer(Int)
	case array([RedisResponse])

	/// Returns true if the response is the Redis standard +OK
	public var isSimpleOK: Bool {
		guard case .simpleString(let s) = self, s == "OK" else {
			return false
		}
		return true
	}

	public var isNil: Bool {
		if case .bulkString(let b) = self, b == nil {
			return true
		}
		return false
	}
	// .string or .binary, else nil
	public var value: RedisClient.RedisValue? {
		switch self {
		case .error:
			return nil
		case .simpleString(let s):
			return .string(s)
		case .bulkString(let b):
			guard let b = b else {
				return nil
			}
			return .binary(b)
		case .integer:
			return nil
		case .array:
			return nil
		}
	}

	@available(*, deprecated, message: "Use `string` property.")
	public func toString() -> String? {
		return string
	}

	public var string: String? {
		switch self {
		case .error(let type, let msg):
			return "\(type) \(msg)"
		case .simpleString(let str):
			return str
		case .bulkString(let bytes):
			if let b = bytes {
				return s(b)
			} else {
				return nil
			}
		case .integer(let int):
			return "\(int)"
		case .array(let array):
			var ary = "["
			ary.append(array.map { $0.string ?? "nil" }.joined(separator: ", "))
			ary.append("]")
			return ary
		}
	}

	public var integer: Int {
		guard case .integer(let i) = self else {
			return 0
		}
		return i
	}

	// to a not null terminated char array
	var bytes: [UInt8] {
		switch self {
		case .error(let type, let msg):
			return "-\(type) \(msg)\r\n".bytes
		case .simpleString(let str):
			return "+\(str)\r\n".bytes
		case .bulkString(let bytes):
			if let b = bytes {
				var ary = "$\(b.count)\r\n".bytes
				ary.append(contentsOf: b)
				ary.append(contentsOf: "\r\n".bytes)
				return ary
			} else {
				return "$-1\r\n".bytes
			}
		case .integer(let int):
			return ":\(int)\r\n".bytes
		case.array(let array):

			var ary = "*\(array.count)\r\n".bytes
			for elem in array {
				ary.append(contentsOf: elem.bytes)
			}
			return ary
		}
	}

	static func readResponse(client: RedisClient, timeoutSeconds: Double, callback: @escaping (RedisResponse) -> ()) {
		if let line = client.pullLineFromBuffer() {
			let endIndex = line.endIndex
			let id = line[0]
			switch Character(UnicodeScalar(id)) {
			case "-": // error
				let split = s(Array(line[1...])).split(separator: " ", maxSplits: 1).map { String($0) }
				let type = split.first ?? "ERR"
				let msg = split.last ?? "Unknown"
				return callback(.error(type: type, msg: msg))
			case "+": // string
				return callback(.simpleString(s(Array(line[1..<endIndex]))))
			case "$": // bulk string
				if let i = Int(s(Array(line[1..<endIndex]))) {
					if i == -1 {
						return callback(.bulkString(nil))
					}
					return client.extractBytesFromBuffer(size: i + 2) { // we are also reading the trailing crlf
						bytes in
						guard let b = bytes else {
							return callback(.error(type: "NET", msg: "Invalid response from server"))
						}
						let subB = Array(b[b.startIndex..<b.endIndex-2])
						return callback(.bulkString(subB))
					}
				}
			case ":": // integer
				if let i = Int(s(Array(line[1..<endIndex]))) {
					return callback(.integer(i))
				}
			case "*": // array
				if let i = Int(s(Array(line[1..<endIndex]))) {
					return readElements(client: client, count: i, into: [RedisResponse](), arrayCallback: callback)
				}
			default:
				()
			}
			return callback(.error(type: "NET", msg: "Invalid response from server"))
		} else {
			client.fillBuffer(timeoutSeconds: timeoutSeconds) {
				ok in

				guard ok else {
					return callback(.error(type: "NET", msg: "Failed to read response from server"))
				}
				RedisResponse.readResponse(client: client, timeoutSeconds: timeoutSeconds, callback: callback)
			}
		}
	}

	static func readElements(client: RedisClient, count: Int, recurseCheck: Int = 5, into: [RedisResponse], arrayCallback: @escaping (RedisResponse) -> ()) {
		if count == -1 {
			return arrayCallback(.array([]))
		}
		if count == 0 {
			return arrayCallback(.array(into))
		}
		RedisResponse.readResponse(client: client, timeoutSeconds: client.netTimeout) {
			element in

			if case .error = element {
				return arrayCallback(element)
			}

			var newAry = into
			newAry.append(element)
			if recurseCheck == 0 { // suboptimal
				DispatchQueue.global().async {
					RedisResponse.readElements(client: client, count: count - 1, into: newAry, arrayCallback: arrayCallback)
				}
			} else {
				RedisResponse.readElements(client: client, count: count - 1, recurseCheck: recurseCheck - 1, into: newAry, arrayCallback: arrayCallback)
			}
		}
	}
}

public struct RedisClientIdentifier {
	let host: String
	let port: Int
	let password: String
	let netGenerator: () -> NetTCP

	public init() {
		host = "127.0.0.1"
		port = redisDefaultPort
		password = ""
		netGenerator = { return NetTCP() }
	}

	public init(withHost: String, port: Int, password: String = "", netGenerator: @escaping () -> NetTCP = { return NetTCP() }) {
		host = withHost
		self.port = port
		self.password = password
		self.netGenerator = netGenerator
	}
}

public extension RedisClientIdentifier {
	func client() throws -> RedisClient {
		return try RedisClient.getClient(withIdentifier: self)
	}
}

public protocol RedisValueRepresentable {
	var redisValue: RedisClient.RedisValue { get }
}

extension String: RedisValueRepresentable {
	public var redisValue: RedisClient.RedisValue {
		return .string(self)
	}
}

public protocol ROctal {}
extension UInt8: ROctal {}

extension Array: RedisValueRepresentable where Element: ROctal {
	public var redisValue: RedisClient.RedisValue {
		return .binary(self as! [UInt8])
	}
}

public class RedisClient {
	public struct CommandError: Error, CustomStringConvertible {
		public let description: String
		init(_ msg: String) {
			description = msg
		}
	}
	public static let invalidResponseError = CommandError("Invalid response type for command.")

	public typealias redisResponseCallback = (RedisResponse) -> ()

	public enum RedisValue: RedisValueRepresentable {
		case string(String)
		case binary([UInt8])

		public var redisValue: RedisClient.RedisValue { return self }

		@available(*, deprecated, message: "Use `string` property.")
		public func toString() -> String? {
			return string
		}

		public var string: String? {
			switch self {
			case .string(let s): return s
			case .binary(let b) : return String(validatingUTF8: b)
			}
		}

		public var commandString: String {
			switch self {
			case .string(let s): return "\"\(s)\""
			case .binary(let b) : return "\"\(hexString(fromArray: b))\""
			}
		}
	}

	public static func getClient(withIdentifier: RedisClientIdentifier, callback: @escaping (() throws -> RedisClient) -> ()) {
		// !FIX! would look in cache here
		let net = withIdentifier.netGenerator()
		do {
			try net.connect(address: withIdentifier.host, port: UInt16(withIdentifier.port), timeoutSeconds: redisDefaultNetTimeout) {
				net in
				if let n = net {
					let client = RedisClient(net: n)
					if !withIdentifier.password.isEmpty {
						client.auth(withPassword: withIdentifier.password) {
							response in
							guard response.isSimpleOK else {
								return callback({ throw PerfectNetError.networkError(401, "Not authorized") })
							}
							callback({ return client })
						}
					} else {
						callback({ return client })
					}
				} else {
					callback({ throw PerfectNetError.networkError(404, "Server was not available") })
				}
			}
		} catch let e {
			callback({ throw e })
		}
	}

	public static func releaseClient(_ client: RedisClient) {
		// !FIX! would put back in cache here - check if connection is open
		client.close()
	}

	let net: NetTCP
	var readBuffer = [UInt8]()
	var readBufferOffset = 0
	var availableBufferedBytes: Int {
		return readBuffer.count - readBufferOffset
	}

	public var netTimeout = redisDefaultNetTimeout

	public init(net: NetTCP) {
		self.net = net
	}

	func close() {
		net.close()
	}

	func appendCRLF(to: [UInt8]) -> [UInt8] {
		var a = to
		a.append(cr)
		a.append(lf)
		return a
	}

	func commandBytes(name: String, parameters: [RedisResponse]) -> [UInt8] {
		var a = name.bytes

		for param in parameters {
			a.append(sp)
			a.append(contentsOf: param.bytes)
		}

		return appendCRLF(to: a)
	}

	func commandBytes(name: String) -> [UInt8] {
		return appendCRLF(to: name.bytes)
	}

	public func sendCommand(name: String, parameters: [RedisResponse], callback: @escaping redisResponseCallback) {
		let a = commandBytes(name: name, parameters: parameters)
		sendRawCommand(bytes: a, callback: callback)
	}

	public func sendCommand(name: String, callback: @escaping redisResponseCallback) {
		let a = commandBytes(name: name)
		sendRawCommand(bytes: a, callback: callback)
	}

	// Send command as serialized in RESP format: See https://redis.io/topics/protocol
	public func sendCommandAsRESP(name: String, parameters: [String], callback: @escaping redisResponseCallback) {

		var array = [RedisResponse.bulkString(name.bytes)]
		#if swift(>=4.1)
			array.append(contentsOf: parameters.compactMap({ RedisResponse.bulkString($0.bytes) }))
		#else
			array.append(contentsOf: parameters.flatMap({ RedisResponse.bulkString($0.bytes) }))
		#endif

		sendRawCommand(bytes: RedisResponse.array(array).bytes, callback: callback)
	}

	// sends the bytes to trhe client
	// reads response when the bytes have been sent
	func sendRawCommand(bytes: [UInt8], callback: @escaping redisResponseCallback) {
		net.write(bytes: bytes) {
			wrote in

			guard wrote == bytes.count else {
				return callback(.error(type: "NET", msg: "Failed to write all bytes"))
			}

			self.readResponse(callback: callback)
		}
	}

	func readResponse(callback: @escaping redisResponseCallback) {
		RedisResponse.readResponse(client: self, timeoutSeconds: netTimeout, callback: callback)
	}

	// pull the request number of bytes from the buffer
	func extractBytesFromBuffer(size: Int, callback: @escaping ([UInt8]?) -> ()) {
		if availableBufferedBytes >= size {
			let ary = Array(readBuffer[readBufferOffset..<readBufferOffset+size])
			readBufferOffset += size
			trimReadBuffer()
			callback(ary)
		} else {
			fillBuffer(timeoutSeconds: netTimeout) {
				ok in
				if ok {
					self.extractBytesFromBuffer(size: size, callback: callback)
				} else {
					callback(nil)
				}
			}
		}
	}

	// returns nil if there is not a complete line to read
	func pullLineFromBuffer() -> [UInt8]? {

		var startOffset = readBufferOffset
		let endCount = readBuffer.count - 1 // so we can always include the lf

		if endCount <= 0 {
			return nil
		}

		while startOffset < endCount {
			if readBuffer[startOffset] == cr && readBuffer[1 + startOffset] == lf {
				let ret = readBuffer[readBufferOffset..<startOffset]
				readBufferOffset = startOffset + 2
				return Array(ret)
			} else {
				startOffset += 1
			}
		}
		return nil
	}

	func trimReadBuffer() {
		if readBufferOffset > 0 {
			readBuffer.removeFirst(readBufferOffset)
			readBufferOffset = 0
		}
	}

	func appendToReadBuffer(bytes: [UInt8]) {
		trimReadBuffer()
		readBuffer.append(contentsOf: bytes)
	}

	// bool indicates that at least one byte was read before timing out
	func fillBuffer(timeoutSeconds: Double, callback: @escaping (Bool) -> ()) {
		net.readSomeBytes(count: redisDefaultReadSize) {
			readBytes in
			guard let readBytes = readBytes else {
				return callback(false)
			}
			if readBytes.count == 0 {
				// no data was available now. try with timeout
				self.net.readBytesFully(count: 1, timeoutSeconds: timeoutSeconds) {
					readBytes in
					guard let readBytes = readBytes, !readBytes.isEmpty else {
						return callback(false)
					}
					self.appendToReadBuffer(bytes: readBytes)
					callback(true)
				}
			} else {
				self.appendToReadBuffer(bytes: readBytes)
				callback(true)
			}
		}
	}
}

/// Connection related operations
public extension RedisClient {
	/// Authorize with password
	func auth(withPassword: String, callback: @escaping redisResponseCallback) {
		sendCommand(name: "AUTH \(withPassword)", callback: callback)
	}
	/// Authorize with password
	func auth(withPassword: String) throws -> RedisResponse {
		return try sendCommand(name: "AUTH \(withPassword)")
	}
	/// Ping the server
	func ping(callback: @escaping redisResponseCallback) {
		sendCommand(name: "PING", callback: callback)
	}
	/// Ping the server
	func ping() throws -> RedisResponse {
		return try sendCommand(name: "PING")
	}
}

/// Database meta-operations
public extension RedisClient {

	/// Flush all keys.
	func flushAll(callback: @escaping redisResponseCallback) {
		sendCommand(name: "FLUSHALL", callback: callback)
	}
	/// Flush all keys.
	func flushAll() throws -> RedisResponse {
		return try sendCommand(name: "FLUSHALL")
	}

	/// Save the database sync.
	func save(callback: @escaping redisResponseCallback) {
		sendCommand(name: "SAVE", callback: callback)
	}
	/// Save the database sync.
	func save() throws -> RedisResponse {
		return try sendCommand(name: "SAVE")
	}

	/// Save the database async.
	func backgroundSave(callback: @escaping redisResponseCallback) {
		sendCommand(name: "BGSAVE", callback: callback)
	}
	/// Save the database async.
	func backgroundSave() throws -> RedisResponse {
		return try sendCommand(name: "BGSAVE")
	}

	/// Timestamp of the last save.
	func lastSave(callback: @escaping redisResponseCallback) {
		sendCommand(name: "LASTSAVE", callback: callback)
	}
	/// Timestamp of the last save.
	func lastSave() throws -> RedisResponse {
		return try sendCommand(name: "LASTSAVE")
	}

	/// Timestamp of the last save.
	func rewriteAppendOnlyFile(callback: @escaping redisResponseCallback) {
		sendCommand(name: "BGREWRITEAOF", callback: callback)
	}
	/// Timestamp of the last save.
	func rewriteAppendOnlyFile() throws -> RedisResponse {
		return try sendCommand(name: "BGREWRITEAOF")
	}

	/// Number of keys in the database.
	func dbSize(callback: @escaping redisResponseCallback) {
		sendCommand(name: "DBSIZE", callback: callback)
	}
	/// Number of keys in the database.
	func dbSize() throws -> RedisResponse {
		return try sendCommand(name: "DBSIZE")
	}

	/// Returns the keys matching pattern.
	func keys(pattern: String, callback: @escaping redisResponseCallback) {
		sendCommand(name: "KEYS \(pattern)", callback: callback)
	}
	/// Returns the keys matching pattern.
	func keys(pattern: String) throws -> RedisResponse {
		return try sendCommand(name: "KEYS \(pattern)")
	}

	/// Returns a random key from the database.
	func randomKey(callback: @escaping redisResponseCallback) {
		sendCommand(name: "RANDOMKEY", callback: callback)
	}
	/// Returns a random key from the database.
	func randomKey() throws -> RedisResponse {
		return try sendCommand(name: "RANDOMKEY")
	}

	/// Select the database at index.
	func select(index: Int, callback: @escaping redisResponseCallback) {
		sendCommand(name: "SELECT \(index)", callback: callback)
	}
	/// Select the database at index.
	func select(index: Int) throws -> RedisResponse {
		return try sendCommand(name: "SELECT \(index)")
	}
}

/// Client operations
public extension RedisClient {

	/// Indicates the type of clients to kill.
	enum KillFilter {
		case addr(ip: String, port: Int)
		case id(String)
		case typeNormal, typeMaster, typeSlave, typePubSub

		func toString() -> String {
			switch self {
			case .addr(let ip, let port):
				return "ADDR \(ip):\(port)"
			case .id(let id):
				return "ID \(id)"
			case .typeNormal:
				return "TYPE normal"
			case .typeMaster:
				return "TYPE master"
			case .typeSlave:
				return "TYPE slave"
			case .typePubSub:
				return "TYPE pubsub"

			}
		}
	}

	/// Client reply setting.
	enum ReplyType {
		case on, off, skip

		func toString() -> String {
			switch self {
			case .on: return "ON"
			case .off: return "OFF"
			case .skip: return "SKIP"
			}
		}
	}

	/// List connected clients.
	func clientList(callback: @escaping redisResponseCallback) {
		sendCommand(name: "CLIENT LIST", callback: callback)
	}
	/// List connected clients.
	func clientList() throws -> RedisResponse {
		return try sendCommand(name: "CLIENT LIST")
	}

	/// Get the name of the connected client.
	func clientGetName(callback: @escaping redisResponseCallback) {
		sendCommand(name: "CLIENT GETNAME", callback: callback)
	}
	/// Get the name of the connected client.
	func clientGetName() throws -> RedisResponse {
		return try sendCommand(name: "CLIENT GETNAME")
	}

	/// Set the name of the connected client.
	func clientSetName(to: String, callback: @escaping redisResponseCallback) {
		sendCommand(name: "CLIENT SETNAME \(to)", callback: callback)
	}
	/// Set the name of the connected client.
	func clientSetName(to: String) throws -> RedisResponse {
		return try sendCommand(name: "CLIENT SETNAME \(to)")
	}

	/// Kill the indicated client connection(s).
	func clientKill(filters: [KillFilter], skipMe: Bool = true, callback: @escaping redisResponseCallback) {
		sendCommand(name: "CLIENT KILL \(filters.map { $0.toString() }.joined(separator: " ")) SKIPME \(skipMe ? "yes" : "no")", callback: callback)
	}
	/// Kill the indicated client connection(s).
	func clientKill(filters: [KillFilter], skipMe: Bool = true) throws -> RedisResponse {
		return try sendCommand(name: "CLIENT KILL \(filters.map { $0.toString() }.joined(separator: " ")) SKIPME \(skipMe ? "yes" : "no")")
	}

	/// Pause all client activity for the indicated timeout.
	func clientPause(timeoutSeconds: Double, callback: @escaping redisResponseCallback) {
		sendCommand(name: "CLIENT PAUSE \(Int(timeoutSeconds * 1000))", callback: callback)
	}
	/// Pause all client activity for the indicated timeout.
	func clientPause(timeoutSeconds: Double) throws -> RedisResponse {
		return try sendCommand(name: "CLIENT PAUSE \(Int(timeoutSeconds * 1000))")
	}

	/// Adjust client replies.
	func clientReply(type: ReplyType, callback: @escaping redisResponseCallback) {
		sendCommand(name: "CLIENT REPLY \(type.toString())", callback: callback)
	}
	/// Adjust client replies.
	func clientReply(type: ReplyType) throws -> RedisResponse {
		return try sendCommand(name: "CLIENT REPLY \(type.toString())")
	}
}

/// Key/value get/set operations.
public extension RedisClient {

	/// Set the key to the String value with an optional expiration.
	func set(key: String, value: RedisValue, expires: Double = 0.0, ifNotExists: Bool = false, ifExists: Bool = false, callback: @escaping redisResponseCallback) {
		var options = ""
		if expires != 0.0 {
			options += " PX \(Int(expires * 1000))"
		}
		if ifNotExists {
			options += " NX"
		} else if ifExists {
			options += " XX"
		}
		sendCommand(name: "SET \(key) \(value.commandString)\(options)", callback: callback)
	}
	/// Set the key to the String value with an optional expiration.
	func set(key: String, value: RedisValue, expires: Double = 0.0, ifNotExists: Bool = false, ifExists: Bool = false) throws -> RedisResponse {
		let p = Promise<RedisResponse> {
			p in
			self.set(key: key, value: value, expires: expires, ifNotExists: ifNotExists, ifExists: ifExists) {
				p.set($0)
			}
		}
		return try syncResponse(p)
	}

	/// Set the keys/values.
	func set(keysValues: [(String, RedisValue)], callback: @escaping redisResponseCallback) {
		sendCommand(name: "MSET \(keysValues.map { "\($0.0) \($0.1.commandString)" }.joined(separator: " "))", callback: callback)
	}
	/// Set the keys/values.
	func set(keysValues: [(String, RedisValue)]) throws -> RedisResponse {
		return try sendCommand(name: "MSET \(keysValues.map { "\($0.0) \($0.1.commandString)" }.joined(separator: " "))")
	}

	/// Set the keys/values.
	func setIfNonExists(keysValues: [(String, RedisValue)], callback: @escaping redisResponseCallback) {
		sendCommand(name: "MSETNX \(keysValues.map { "\($0.0) \($0.1.commandString)" }.joined(separator: " "))", callback: callback)
	}
	/// Set the keys/values.
	func setIfNonExists(keysValues: [(String, RedisValue)]) throws -> RedisResponse {
		return try sendCommand(name: "MSETNX \(keysValues.map { "\($0.0) \($0.1.commandString)" }.joined(separator: " "))")
	}

	/// Get the key value.
	func get(key: String, callback: @escaping redisResponseCallback) {
		sendCommand(name: "GET \(key)", callback: callback)
	}
	/// Get the key value.
	func get(key: String) throws -> RedisResponse {
		return try sendCommand(name: "GET \(key)")
	}

	/// Get the keys values.
	func get(keys: [String], callback: @escaping redisResponseCallback) {
		sendCommand(name: "MGET \(keys.joined(separator: " "))", callback: callback)
	}
	/// Get the keys values.
	func get(keys: [String]) throws -> RedisResponse {
		return try sendCommand(name: "MGET \(keys.joined(separator: " "))")
	}

	/// Get the key value and set to new value.
	func getSet(key: String, newValue: RedisValue, callback: @escaping redisResponseCallback) {
		sendCommand(name: "GETSET \(key) \(newValue.commandString)", callback: callback)
	}
	/// Get the key value and set to new value.
	func getSet(key: String, newValue: RedisValue) throws -> RedisResponse {
		return try sendCommand(name: "GETSET \(key) \(newValue.commandString)")
	}

	/// Get the key value.
	func delete(keys: String..., callback: @escaping redisResponseCallback) {
		sendCommand(name: "DEL \(keys.joined(separator: " "))", callback: callback)
	}
	/// Get the key value.
	@discardableResult
	func delete(keys: String...) throws -> RedisResponse {
		return try sendCommand(name: "DEL \(keys.joined(separator: " "))")
	}

	/// Increment the key value.
	func increment(key: String, callback: @escaping redisResponseCallback) {
		sendCommand(name: "INCR \(key)", callback: callback)
	}
	/// Increment the key value.
	func increment(key: String) throws -> RedisResponse {
		return try sendCommand(name: "INCR \(key)")
	}

	/// Increment the key value.
	func increment(key: String, by: Int, callback: @escaping redisResponseCallback) {
		sendCommand(name: "INCRBY \(key) \(by)", callback: callback)
	}
	/// Increment the key value.
	func increment(key: String, by: Int) throws -> RedisResponse {
		return try sendCommand(name: "INCRBY \(key) \(by)")
	}

	/// Increment the key value.
	func increment(key: String, by: Double, callback: @escaping redisResponseCallback) {
		sendCommand(name: "INCRBYFLOAT \(key) \(by)", callback: callback)
	}
	/// Increment the key value.
	func increment(key: String, by: Double) throws -> RedisResponse {
		return try sendCommand(name: "INCRBYFLOAT \(key) \(by)")
	}

	/// Decrement the key value.
	func decrement(key: String, callback: @escaping redisResponseCallback) {
		sendCommand(name: "DECR \(key)", callback: callback)
	}
	/// Decrement the key value.
	func decrement(key: String) throws -> RedisResponse {
		return try sendCommand(name: "DECR \(key)")
	}

	/// Increment the key value.
	func decrement(key: String, by: Int, callback: @escaping redisResponseCallback) {
		sendCommand(name: "DECRBY \(key) \(by)", callback: callback)
	}
	/// Increment the key value.
	func decrement(key: String, by: Int) throws -> RedisResponse {
		return try sendCommand(name: "DECRBY \(key) \(by)")
	}

	/// Rename a key.
	func rename(key: String, newKey: String, callback: @escaping redisResponseCallback) {
		sendCommand(name: "RENAME \(key) \(newKey)", callback: callback)
	}
	/// Rename a key.
	func rename(key: String, newKey: String) throws -> RedisResponse {
		return try sendCommand(name: "RENAME \(key) \(newKey)")
	}

	/// Rename a key if new name does not exist.
	func renameIfnotExists(key: String, newKey: String, callback: @escaping redisResponseCallback) {
		sendCommand(name: "RENAMENX \(key) \(newKey)", callback: callback)
	}
	/// Rename a key if new name does not exist.
	func renameIfnotExists(key: String, newKey: String) throws -> RedisResponse {
		return try sendCommand(name: "RENAMENX \(key) \(newKey)")
	}

	/// Check if the indicated keys exist.
	func exists(keys: String..., callback: @escaping redisResponseCallback) {
		guard keys.count > 0 else {
			return callback(.array([RedisResponse]()))
		}
		sendCommand(name: "EXISTS \(keys.joined(separator: " "))", callback: callback)
	}
	/// Check if the indicated keys exist.
	func exists(keys: String...) throws -> RedisResponse {
		guard keys.count > 0 else {
			return .array([RedisResponse]())
		}
		return try sendCommand(name: "EXISTS \(keys.joined(separator: " "))")
	}

	/// Append a value to the key.
	func append(key: String, value: RedisValue, callback: @escaping redisResponseCallback) {
		sendCommand(name: "APPEND \(key) \(value.commandString)", callback: callback)
	}
	/// Append a value to the key.
	func append(key: String, value: RedisValue) throws -> RedisResponse {
		return try sendCommand(name: "APPEND \(key) \(value.commandString)")
	}

	/// Set the expiration for the indicated key.
	func expire(key: String, seconds: Double, callback: @escaping redisResponseCallback) {
		sendCommand(name: "PEXPIRE \(key) \(Int(seconds * 1000))", callback: callback)
	}
	/// Set the expiration for the indicated key.
	func expire(key: String, seconds: Double) throws -> RedisResponse {
		return try sendCommand(name: "PEXPIRE \(key) \(Int(seconds * 1000))")
	}

	/// Set the expiration for the indicated key.
	func expireAt(key: String, seconds: Double, callback: @escaping redisResponseCallback) {
		sendCommand(name: "PEXPIREAT \(key) \(Int(seconds * 1000))", callback: callback)
	}
	/// Set the expiration for the indicated key.
	func expireAt(key: String, seconds: Double) throws -> RedisResponse {
		return try sendCommand(name: "PEXPIREAT \(key) \(Int(seconds * 1000))")
	}

	/// Returns the expiration in milliseconds for the indicated key.
	func timeToExpire(key: String, callback: @escaping redisResponseCallback) {
		sendCommand(name: "PTTL \(key)", callback: callback)
	}
	/// Returns the expiration in milliseconds for the indicated key.
	func timeToExpire(key: String) throws -> RedisResponse {
		return try sendCommand(name: "PTTL \(key)")
	}

	/// Remove the expiration for the indicated key.
	func persist(key: String, callback: @escaping redisResponseCallback) {
		sendCommand(name: "PERSIST \(key)", callback: callback)
	}
	/// Remove the expiration for the indicated key.
	func persist(key: String) throws -> RedisResponse {
		return try sendCommand(name: "PERSIST \(key)")
	}
}

/// Bit related operations.
public extension RedisClient {

	/// BITFIELD operation integer types.
	enum IntegerType {
		case signed(Int)
		case unsigned(Int)

		func toString() -> String {
			switch self {
			case .signed(let i):
				return "i\(i)"
			case .unsigned(let i):
				return "u\(i)"
			}
		}
	}

	/// BITFIELD operation commands.
	enum SubCommand {
		case get(type: IntegerType, offset: Int)
		case set(type: IntegerType, offset: Int, value: Int)
		case setMul(type: IntegerType, offset: String, value: Int)
		case incrby(type: IntegerType, offset: Int, increment: Int)
		case overflowWrap
		case overflowSat
		case overflowFail

		func toString() -> String {
			switch self {
			case .get(let type, let offset):
				return "GET \(type.toString()) \(offset)"
			case .set(let type, let offset, let value):
				return "SET \(type.toString()) \(offset) \(value)"
			case .setMul(let type, let offset, let value):
				return "SET \(type.toString()) #\(offset) \(value)"
			case .incrby(let type, let offset, let increment):
				return "INCRBY \(type.toString()) \(offset) \(increment)"
			case .overflowWrap:
				return "OVERFLOW WRAP"
			case .overflowSat:
				return "OVERFLOW SAT"
			case .overflowFail:
				return "OVERFLOW FAIL"
			}
		}
	}

	/// BITOP bit operations.
	enum BitOperation {
		case and, or, xor, not
		func toString() -> String {
			switch self {
			case .and: return "AND"
			case .or: return "OR"
			case .xor: return "XOR"
			case .not: return "NOT"
			}
		}
	}

	/// Count the set bits in a value.
	func bitCount(key: String, callback: @escaping redisResponseCallback) {
		sendCommand(name: "BITCOUNT \(key)", callback: callback)
	}

	/// Count the set bits in a value.
	func bitCount(key: String, start: Int, end: Int, callback: @escaping redisResponseCallback) {
		sendCommand(name: "BITCOUNT \(key) \(start) \(end)", callback: callback)
	}

	/// Perform a bitfield operation on a value
	func bitField(key: String, commands: [SubCommand], callback: @escaping redisResponseCallback) {
		sendCommand(name: "BITFIELD \(key) \(commands.map { $0.toString() }.joined(separator: " "))", callback: callback)
	}

	/// Perform a bitfield operation on a value.
	func bitOp(_ op: BitOperation, destKey: String, srcKeys: String..., callback: @escaping redisResponseCallback) {
		sendCommand(name: "BITOP \(op.toString()) \(destKey) \(srcKeys.joined(separator: " "))", callback: callback)
	}

	/// Perform a bitpos operation.
	func bitPos(key: String, position: Int, callback: @escaping redisResponseCallback) {
		sendCommand(name: "BITPOS \(key) \(position))", callback: callback)
	}

	/// Perform a bitpos operation on a range.
	func bitPos(key: String, position: Int, start: Int, callback: @escaping redisResponseCallback) {
		sendCommand(name: "BITPOS \(key) \(position) \(start))", callback: callback)
	}

	/// Perform a bitpos operation on a range.
	func bitPos(key: String, position: Int, start: Int, end: Int, callback: @escaping redisResponseCallback) {
		sendCommand(name: "BITPOS \(key) \(position) \(start) \(end))", callback: callback)
	}

	/// Get the bit at the indicated offset.
	func bitGet(key: String, offset: Int, callback: @escaping redisResponseCallback) {
		sendCommand(name: "GETBIT \(key) \(offset))", callback: callback)
	}

	/// Set the bit at the indicated offset.
	func bitSet(key: String, offset: Int, value: Bool, callback: @escaping redisResponseCallback) {
		sendCommand(name: "SETBIT \(key) \(offset) \(value ? 1 : 0))", callback: callback)
	}

	/// Count the set bits in a value.
	func bitCount(key: String) throws -> RedisResponse {
		return try sendCommand(name: "BITCOUNT \(key)")
	}

	/// Count the set bits in a value.
	func bitCount(key: String, start: Int, end: Int) throws -> RedisResponse {
		return try sendCommand(name: "BITCOUNT \(key) \(start) \(end)")
	}

	/// Perform a bitfield operation on a value
	func bitField(key: String, commands: [SubCommand]) throws -> RedisResponse {
		return try sendCommand(name: "BITFIELD \(key) \(commands.map { $0.toString() }.joined(separator: " "))")
	}

	/// Perform a bitfield operation on a value.
	func bitOp(_ op: BitOperation, destKey: String, srcKeys: String...) throws -> RedisResponse {
		return try sendCommand(name: "BITOP \(op.toString()) \(destKey) \(srcKeys.joined(separator: " "))")
	}

	/// Perform a bitpos operation.
	func bitPos(key: String, position: Int) throws -> RedisResponse {
		return try sendCommand(name: "BITPOS \(key) \(position))")
	}

	/// Perform a bitpos operation on a range.
	func bitPos(key: String, position: Int, start: Int) throws -> RedisResponse {
		return try sendCommand(name: "BITPOS \(key) \(position) \(start))")
	}

	/// Perform a bitpos operation on a range.
	func bitPos(key: String, position: Int, start: Int, end: Int) throws -> RedisResponse {
		return try sendCommand(name: "BITPOS \(key) \(position) \(start) \(end))")
	}

	/// Get the bit at the indicated offset.
	func bitGet(key: String, offset: Int) throws -> RedisResponse {
		return try sendCommand(name: "GETBIT \(key) \(offset))")
	}

	/// Set the bit at the indicated offset.
	func bitSet(key: String, offset: Int, value: Bool) throws -> RedisResponse {
		return try sendCommand(name: "SETBIT \(key) \(offset) \(value ? 1 : 0))")
	}
}

/// List related operations
public extension RedisClient {

	/// Push values to the beginning of the list
	func listPrepend(key: String, values: [RedisValue], callback: @escaping redisResponseCallback) {
		sendCommand(name: "LPUSH \(key) \(values.map { $0.commandString }.joined(separator: " "))", callback: callback)
	}

	/// Push values to the end of the list
	func listAppend(key: String, values: [RedisValue], callback: @escaping redisResponseCallback) {
		sendCommand(name: "RPUSH \(key) \(values.map { $0.commandString }.joined(separator: " "))", callback: callback)
	}

	/// Push value to the beginning of the list. LPUSHX
	func listPrependX(key: String, value: RedisValue, callback: @escaping redisResponseCallback) {
		sendCommand(name: "LPUSHX \(key) \(value.commandString)", callback: callback)
	}

	/// Push value to the end of the list. RPUSHX
	func listAppendX(key: String, value: RedisValue, callback: @escaping redisResponseCallback) {
		sendCommand(name: "RPUSHX \(key) \(value.commandString)", callback: callback)
	}

	/// Pop and return the first element from the list
	func listPopFirst(key: String, callback: @escaping redisResponseCallback) {
		sendCommand(name: "LPOP \(key)", callback: callback)
	}

	/// Pop and return the last element from the list
	func listPopLast(key: String, callback: @escaping redisResponseCallback) {
		sendCommand(name: "RPOP \(key)", callback: callback)
	}

	/// Pop and return the last element from the list. Append the element to the destination list.
	func listPopLastAppend(sourceKey: String, destKey: String, callback: @escaping redisResponseCallback) {
		sendCommand(name: "RPOPLPUSH \(sourceKey) \(destKey)", callback: callback)
	}

	/// Pop and return the last element from the list. Append the element to the destination list.
	func listPopLastAppendBlocking(sourceKey: String, destKey: String, timeout: Int = 0, callback: @escaping redisResponseCallback) {
		sendCommand(name: "BRPOPLPUSH \(sourceKey) \(destKey) \(timeout)", callback: callback)
	}

	/// Pop and return the first element from the list
	func listPopFirstBlocking(keys: String..., timeout: Int, callback: @escaping redisResponseCallback) {
		sendCommand(name: "BLPOP \(keys.joined(separator: " ")) \(timeout)", callback: callback)
	}

	/// Pop and return the last element from the list
	func listPopLastBlocking(keys: String..., timeout: Int, callback: @escaping redisResponseCallback) {
		sendCommand(name: "BRPOP \(keys.joined(separator: " ")) \(timeout)", callback: callback)
	}

	/// The length of the list
	func listLength(key: String, callback: @escaping redisResponseCallback) {
		sendCommand(name: "LLEN \(key)", callback: callback)
	}

	/// Remove the items in the range
	func listTrim(key: String, start: Int, stop: Int, callback: @escaping redisResponseCallback) {
		sendCommand(name: "LTRIM \(key) \(start) \(stop)", callback: callback)
	}

	/// Returns the list items in the indicated range
	func listRange(key: String, start: Int, stop: Int, callback: @escaping redisResponseCallback) {
		sendCommand(name: "LRANGE \(key) \(start) \(stop)", callback: callback)
	}

	/// Returns the list item at the indicated offset.
	func listGetElement(key: String, index: Int, callback: @escaping redisResponseCallback) {
		sendCommand(name: "LINDEX \(key) \(index)", callback: callback)
	}

	/// Inserts the new item before the indicated value.
	func listInsert(key: String, element: RedisValue, before: RedisValue, callback: @escaping redisResponseCallback) {
		sendCommand(name: "LINSERT \(key) BEFORE \(before.commandString)", callback: callback)
	}

	/// Inserts the new item after the indicated value.
	func listInsert(key: String, element: RedisValue, after: RedisValue, callback: @escaping redisResponseCallback) {
		sendCommand(name: "LINSERT \(key) AFTER \(after.commandString)", callback: callback)
	}

	/// Set the item at index to value.
	func listSet(key: String, index: Int, value: RedisValue, callback: @escaping redisResponseCallback) {
		sendCommand(name: "LSET \(key) \(index) \(value.commandString)", callback: callback)
	}

	/// Remove the first N elements matching value.
	func listRemoveMatching(key: String, value: RedisValue, count: Int, callback: @escaping redisResponseCallback) {
		sendCommand(name: "LREM \(key) \(count) \(value.commandString)", callback: callback)
	}
}

/// List related operations sync
public extension RedisClient {
	/// Push values to the beginning of the list
	func listPrepend(key: String, values: [RedisValue]) throws -> RedisResponse {
		return try sendCommand(name: "LPUSH \(key) \(values.map { $0.commandString }.joined(separator: " "))")
	}

	/// Push values to the end of the list
	func listAppend(key: String, values: [RedisValue]) throws -> RedisResponse {
		return try sendCommand(name: "RPUSH \(key) \(values.map { $0.commandString }.joined(separator: " "))")
	}

	/// Push value to the beginning of the list. LPUSHX
	func listPrependX(key: String, value: RedisValue) throws -> RedisResponse {
		return try sendCommand(name: "LPUSHX \(key) \(value.commandString)")
	}

	/// Push value to the end of the list. RPUSHX
	func listAppendX(key: String, value: RedisValue) throws -> RedisResponse {
		return try sendCommand(name: "RPUSHX \(key) \(value.commandString)")
	}

	/// Pop and return the first element from the list
	func listPopFirst(key: String) throws -> RedisResponse {
		return try sendCommand(name: "LPOP \(key)")
	}

	/// Pop and return the last element from the list
	func listPopLast(key: String) throws -> RedisResponse {
		return try sendCommand(name: "RPOP \(key)")
	}

	/// Pop and return the last element from the list. Append the element to the destination list.
	func listPopLastAppend(sourceKey: String, destKey: String) throws -> RedisResponse {
		return try sendCommand(name: "RPOPLPUSH \(sourceKey) \(destKey)")
	}

	/// Pop and return the last element from the list. Append the element to the destination list.
	func listPopLastAppendBlocking(sourceKey: String, destKey: String, timeout: Int) throws -> RedisResponse {
		return try sendCommand(name: "BRPOPLPUSH \(sourceKey) \(destKey) \(timeout)")
	}

	/// Pop and return the first element from the list
	func listPopFirstBlocking(keys: String..., timeout: Int) throws -> RedisResponse {
		return try sendCommand(name: "BLPOP \(keys.joined(separator: " ")) \(timeout)")
	}

	/// Pop and return the last element from the list
	func listPopLastBlocking(keys: String..., timeout: Int) throws -> RedisResponse {
		return try sendCommand(name: "BRPOP \(keys.joined(separator: " ")) \(timeout)")
	}

	/// The length of the list
	func listLength(key: String) throws -> RedisResponse {
		return try sendCommand(name: "LLEN \(key)")
	}

	/// Remove the items in the range
	func listTrim(key: String, start: Int, stop: Int) throws -> RedisResponse {
		return try sendCommand(name: "LTRIM \(key) \(start) \(stop)")
	}

	/// Returns the list items in the indicated range
	func listRange(key: String, start: Int, stop: Int) throws -> RedisResponse {
		return try sendCommand(name: "LRANGE \(key) \(start) \(stop)")
	}

	/// Returns the list item at the indicated offset.
	func listGetElement(key: String, index: Int) throws -> RedisResponse {
		return try sendCommand(name: "LINDEX \(key) \(index)")
	}

	/// Inserts the new item before the indicated value.
	func listInsert(key: String, element: RedisValue, before: RedisValue) throws -> RedisResponse {
		return try sendCommand(name: "LINSERT \(key) BEFORE \(before.commandString)")
	}

	/// Inserts the new item after the indicated value.
	func listInsert(key: String, element: RedisValue, after: RedisValue) throws -> RedisResponse {
		return try sendCommand(name: "LINSERT \(key) AFTER \(after.commandString)")
	}

	/// Set the item at index to value.
	func listSet(key: String, index: Int, value: RedisValue) throws -> RedisResponse {
		return try sendCommand(name: "LSET \(key) \(index) \(value.commandString)")
	}

	/// Remove the first N elements matching value.
	func listRemoveMatching(key: String, value: RedisValue, count: Int) throws -> RedisResponse {
		return try sendCommand(name: "LREM \(key) \(count) \(value.commandString)")
	}
}

/// Multi (transaction) related operations.
public extension RedisClient {

	/// Begin a transaction.
	func multiBegin(callback: @escaping redisResponseCallback) {
		sendCommand(name: "MULTI", callback: callback)
	}

	/// Execute a transation.
	func multiExec(callback: @escaping redisResponseCallback) {
		sendCommand(name: "EXEC", callback: callback)
	}

	/// Discard a transaction.
	func multiDiscard(callback: @escaping redisResponseCallback) {
		sendCommand(name: "DISCARD", callback: callback)
	}

	/// Watch keys for modification during a transaction.
	func multiWatch(keys: [String], callback: @escaping redisResponseCallback) {
		sendCommand(name: "WATCH \(keys.joined(separator: " "))", callback: callback)
	}

	/// Unwatch keys for modification during a transaction.
	func multiUnwatch(callback: @escaping redisResponseCallback) {
		sendCommand(name: "UNWATCH", callback: callback)
	}

	/// Begin a transaction.
	func multiBegin() throws -> RedisResponse {
		return try sendCommand(name: "MULTI")
	}

	/// Execute a transation.
	func multiExec() throws -> RedisResponse {
		return try sendCommand(name: "EXEC")
	}

	/// Discard a transaction.
	func multiDiscard() throws -> RedisResponse {
		return try sendCommand(name: "DISCARD")
	}

	/// Watch keys for modification during a transaction.
	func multiWatch(keys: [String]) throws -> RedisResponse {
		return try sendCommand(name: "WATCH \(keys.joined(separator: " "))")
	}

	/// Unwatch keys for modification during a transaction.
	func multiUnwatch() throws -> RedisResponse {
		return try sendCommand(name: "UNWATCH")
	}

	/// Execute commands in body. Discard if error is thrown
	func multi(body: () throws -> ()) throws -> RedisResponse {
		_ = try multiBegin()
		do {
			try body()
			return try multiExec()
		} catch {
			_ = try multiDiscard()
			throw error
		}
	}
}

/// Pub/sub related operations.
public extension RedisClient {

	/// Subscribe to the following patterns.
	func subscribe(patterns: [String], callback: @escaping redisResponseCallback) {
		sendCommand(name: "PSUBSCRIBE \(patterns.joined(separator: " "))", callback: callback)
	}
	/// Subscribe to the following channels.
	func subscribe(channels: [String], callback: @escaping redisResponseCallback) {
		sendCommand(name: "SUBSCRIBE \(channels.joined(separator: " "))", callback: callback)
	}

	/// Unsubscribe to the following patterns.
	func unsubscribe(patterns: [String], callback: @escaping redisResponseCallback) {
		sendCommand(name: "PUNSUBSCRIBE \(patterns.joined(separator: " "))", callback: callback)
	}

	/// Unsubscribe to the following channels.
	func unsubscribe(channels: [String], callback: @escaping redisResponseCallback) {
		sendCommand(name: "UNSUBSCRIBE \(channels.joined(separator: " "))", callback: callback)
	}

	/// Publish a message to the channel.
	func publish(channel: String, message: RedisValue, callback: @escaping redisResponseCallback) {
		sendCommand(name: "PUBLISH \(channel) \(message.commandString)", callback: callback)
	}

	/// Subscribe to the following patterns.
	func subscribe(patterns: [String]) throws -> RedisResponse {
		return try sendCommand(name: "PSUBSCRIBE \(patterns.joined(separator: " "))")
	}
	/// Subscribe to the following channels.
	func subscribe(channels: [String]) throws -> RedisResponse {
		return try sendCommand(name: "SUBSCRIBE \(channels.joined(separator: " "))")
	}

	/// Unsubscribe to the following patterns.
	func unsubscribe(patterns: [String]) throws -> RedisResponse {
		return try sendCommand(name: "PUNSUBSCRIBE \(patterns.joined(separator: " "))")
	}

	/// Unsubscribe to the following channels.
	func unsubscribe(channels: [String]) throws -> RedisResponse {
		return try sendCommand(name: "UNSUBSCRIBE \(channels.joined(separator: " "))")
	}

	/// Publish a message to the channel.
	func publish(channel: String, message: RedisValue) throws -> RedisResponse {
		return try sendCommand(name: "PUBLISH \(channel) \(message.commandString)")
	}

	/// Read a published message given a timeout.
	func readPublished(timeoutSeconds: Double, callback: @escaping redisResponseCallback) {
		RedisResponse.readResponse(client: self, timeoutSeconds: timeoutSeconds, callback: callback)
	}
	/// Read a published message given a timeout.
	func readPublished(timeoutSeconds: Double) throws -> RedisResponse {
		let p = Promise<RedisResponse> {
			p in
			RedisResponse.readResponse(client: self, timeoutSeconds: timeoutSeconds) {
				p.set($0)
			}
		}
		return try syncResponse(p)
	}

	// PUBSUB
}

/// Set related operations. Write me! !FIX!
public extension RedisClient {

	/// Inserts the new elements into the set.
	func setAdd(key: String, elements: [RedisValue], callback: @escaping redisResponseCallback) {
		sendCommand(name: "SADD \(key) \(elements.map { $0.commandString }.joined(separator: " "))", callback: callback)
	}

	/// Returns the number of elements in the set.
	func setCount(key: String, callback: @escaping redisResponseCallback) {
		sendCommand(name: "SCARD \(key)", callback: callback)
	}

	/// Returns the difference between `key` and `againstKeys`.
	func setDifference(key: String, againstKeys: [String], callback: @escaping redisResponseCallback) {
		sendCommand(name: "SDIFF \(key) \(againstKeys.joined(separator: " "))", callback: callback)
	}

	/// Stores to `into` the difference between `ofKey` and `againstKeys`.
	func setStoreDifference(into: String, ofKey: String, againstKeys: [String], callback: @escaping redisResponseCallback) {
		sendCommand(name: "SDIFFSTORE \(into) \(ofKey) \(againstKeys.joined(separator: " "))", callback: callback)
	}

	/// Returns the intersection of `key` and `againstKeys`.
	func setIntersection(key: String, againstKeys: [String], callback: @escaping redisResponseCallback) {
		sendCommand(name: "SINTER \(key) \(againstKeys.joined(separator: " "))", callback: callback)
	}

	/// Stores to `into` the intersection of `ofKey` and `againstKeys`.
	func setStoreIntersection(into: String, ofKey: String, againstKeys: [String], callback: @escaping redisResponseCallback) {
		sendCommand(name: "SINTERSTORE \(into) \(ofKey) \(againstKeys.joined(separator: " "))", callback: callback)
	}

	/// Returns the union of `key` and `againstKeys`.
	func setUnion(key: String, againstKeys: [String], callback: @escaping redisResponseCallback) {
		sendCommand(name: "SUNION \(key) \(againstKeys.joined(separator: " "))", callback: callback)
	}

	/// Stores to `into` the union of `ofKey` and `againstKeys`.
	func setStoreUnion(into: String, ofKey: String, againstKeys: [String], callback: @escaping redisResponseCallback) {
		sendCommand(name: "SUNIONSTORE \(into) \(ofKey) \(againstKeys.joined(separator: " "))", callback: callback)
	}

	/// Checks if the set `key` contains `value`.
	func setContains(key: String, value: RedisValue, callback: @escaping redisResponseCallback) {
		sendCommand(name: "SISMEMBER \(key) \(value.commandString)", callback: callback)
	}

	/// Returns the members of set `key`.
	func setMembers(key: String, callback: @escaping redisResponseCallback) {
		sendCommand(name: "SMEMBERS \(key)", callback: callback)
	}

	/// Moves the set `value` `fromKey` to `toKey`.
	func setMove(fromKey: String, toKey: String, value: RedisValue, callback: @escaping redisResponseCallback) {
		sendCommand(name: "SMOVE \(fromKey) \(toKey) \(value.commandString)", callback: callback)
	}

	/// Removes and returns `count` random elements of set `key`.
	func setRandomPop(key: String, count: Int, callback: @escaping redisResponseCallback) {
		sendCommand(name: "SPOP \(key) \(count)", callback: callback)
	}

	/// Removes and returns a random element of set `key`.
	func setRandomPop(key: String, callback: @escaping redisResponseCallback) {
		sendCommand(name: "SPOP \(key)", callback: callback)
	}

	/// Returns `count` random elements of set `key`.
	func setRandomGet(key: String, count: Int, callback: @escaping redisResponseCallback) {
		sendCommand(name: "SRANDMEMBER \(key) \(count)", callback: callback)
	}

	/// Returns a random element of set `key`.
	func setRandomGet(key: String, callback: @escaping redisResponseCallback) {
		sendCommand(name: "SRANDMEMBER \(key)", callback: callback)
	}

	/// Removes the value from set `key`.
	func setRemove(key: String, value: RedisValue, callback: @escaping redisResponseCallback) {
		sendCommand(name: "SREM \(key) \(value.commandString)", callback: callback)
	}

	/// Removes the values from set `key`.
	func setRemove(key: String, values: [RedisValue], callback: @escaping redisResponseCallback) {
		sendCommand(name: "SREM \(key) \(values.map { $0.commandString }.joined(separator: " "))", callback: callback)
	}

	/// Scans the set `key` given the current cursor, which should start from zero.
	/// Optionally accepts a pattern and a maximum returned value count.
	func setScan(key: String, cursor: Int = 0, pattern: String? = nil, count: Int? = nil, callback: @escaping redisResponseCallback) {
		sendCommand(name: "SSCAN \(key) \(cursor) \(pattern ?? "") \(nil == count ? "" : String(count!))", callback: callback)
	}

	/// Inserts the new elements into the set.
	func setAdd(key: String, elements: [RedisValue]) throws -> RedisResponse {
		return try sendCommand(name: "SADD \(key) \(elements.map { $0.commandString }.joined(separator: " "))")
	}

	/// Returns the number of elements in the set.
	func setCount(key: String) throws -> RedisResponse {
		return try sendCommand(name: "SCARD \(key)")
	}

	/// Returns the difference between `key` and `againstKeys`.
	func setDifference(key: String, againstKeys: [String]) throws -> RedisResponse {
		return try sendCommand(name: "SDIFF \(key) \(againstKeys.joined(separator: " "))")
	}

	/// Stores to `into` the difference between `ofKey` and `againstKeys`.
	func setStoreDifference(into: String, ofKey: String, againstKeys: [String]) throws -> RedisResponse {
		return try sendCommand(name: "SDIFFSTORE \(into) \(ofKey) \(againstKeys.joined(separator: " "))")
	}

	/// Returns the intersection of `key` and `againstKeys`.
	func setIntersection(key: String, againstKeys: [String]) throws -> RedisResponse {
		return try sendCommand(name: "SINTER \(key) \(againstKeys.joined(separator: " "))")
	}

	/// Stores to `into` the intersection of `ofKey` and `againstKeys`.
	func setStoreIntersection(into: String, ofKey: String, againstKeys: [String]) throws -> RedisResponse {
		return try sendCommand(name: "SINTERSTORE \(into) \(ofKey) \(againstKeys.joined(separator: " "))")
	}

	/// Returns the union of `key` and `againstKeys`.
	func setUnion(key: String, againstKeys: [String]) throws -> RedisResponse {
		return try sendCommand(name: "SUNION \(key) \(againstKeys.joined(separator: " "))")
	}

	/// Stores to `into` the union of `ofKey` and `againstKeys`.
	func setStoreUnion(into: String, ofKey: String, againstKeys: [String]) throws -> RedisResponse {
		return try sendCommand(name: "SUNIONSTORE \(into) \(ofKey) \(againstKeys.joined(separator: " "))")
	}

	/// Checks if the set `key` contains `value`.
	func setContains(key: String, value: RedisValue) throws -> RedisResponse {
		return try sendCommand(name: "SISMEMBER \(key) \(value.commandString)")
	}

	/// Returns the members of set `key`.
	func setMembers(key: String) throws -> RedisResponse {
		return try sendCommand(name: "SMEMBERS \(key)")
	}

	/// Moves the set `value` `fromKey` to `toKey`.
	func setMove(fromKey: String, toKey: String, value: RedisValue) throws -> RedisResponse {
		return try sendCommand(name: "SMOVE \(fromKey) \(toKey) \(value.commandString)")
	}

	/// Removes and returns `count` random elements of set `key`.
	func setRandomPop(key: String, count: Int) throws -> RedisResponse {
		return try sendCommand(name: "SPOP \(key) \(count)")
	}

	/// Removes and returns a random element of set `key`.
	func setRandomPop(key: String) throws -> RedisResponse {
		return try sendCommand(name: "SPOP \(key)")
	}

	/// Returns `count` random elements of set `key`.
	func setRandomGet(key: String, count: Int) throws -> RedisResponse {
		return try sendCommand(name: "SRANDMEMBER \(key) \(count)")
	}

	/// Returns a random element of set `key`.
	func setRandomGet(key: String) throws -> RedisResponse {
		return try sendCommand(name: "SRANDMEMBER \(key)")
	}

	/// Removes the value from set `key`.
	func setRemove(key: String, value: RedisValue) throws -> RedisResponse {
		return try sendCommand(name: "SREM \(key) \(value.commandString)")
	}

	/// Removes the values from set `key`.
	func setRemove(key: String, values: [RedisValue]) throws -> RedisResponse {
		return try sendCommand(name: "SREM \(key) \(values.map { $0.commandString }.joined(separator: " "))")
	}

	/// Scans the set `key` given the current cursor, which should start from zero.
	/// Optionally accepts a pattern and a maximum returned value count.
	func setScan(key: String, cursor: Int = 0, pattern: String? = nil, count: Int? = nil) throws -> RedisResponse {
		return try sendCommand(name: "SSCAN \(key) \(cursor) \(pattern ?? "") \(nil == count ? "" : String(count!))")
	}
}

/// Cluster related operations. Write me! !FIX!
public extension RedisClient {

}

/// Command related operations. Write me! !FIX!
public extension RedisClient {

}

/// Config related operations. Write me! !FIX!
public extension RedisClient {

}

/// Hash related operations. Write me! !FIX!
public extension RedisClient {

	/// Set field in the hash stored at key to value.
	func hashSet(key: String, field: String, value: RedisValue, callback: @escaping redisResponseCallback) {
		sendCommand(name: "HSET \(key) \(field) \(value.commandString)", callback: callback)
	}

	/// Set multiple field value pairs in an atomic operation
	func hashSet(key: String, fieldsValues: [(String, RedisValue)], callback: @escaping redisResponseCallback) {
		sendCommand(name: "HMSET \(key) \(fieldsValues.map { "\($0.0) \($0.1.commandString)" }.joined(separator: " "))", callback: callback)
	}

	/// Set a field value pair if not exists
	func hashSetIfNonExists(key: String, field: String, value: RedisValue, callback: @escaping redisResponseCallback) {
		sendCommand(name: "HSETNX \(key) \(field) \(value.commandString)", callback: callback)
	}

	/// Get a field in the hash stored at key.
	func hashGet(key: String, field: String, callback: @escaping redisResponseCallback) {
		sendCommand(name: "HGET \(key) \(field)", callback: callback)
	}

	/// Get multiple fields in the hash stored at key.
	func hashGet(key: String, fields: [String], callback: @escaping redisResponseCallback) {
		sendCommand(name: "HMGET \(key) \(fields.joined(separator: " "))", callback: callback)
	}

	/**
	Return 1 if field is an existing field in the hash stored at key.
	0 if does not exist
	*/
	func hashExists(key: String, field: String, callback: @escaping redisResponseCallback) {
		sendCommand(name: "HEXISTS \(key) \(field)", callback: callback)
	}

	/**
	Remove the specified fields from the hash stored at key.
	Specified fields that do not exist within this hash are ignored.
	If key does not exist, it is treated as an empty hash and this command returns 0.
	Otherwise the command returns the number of fields that have been deleted
	*/
	func hashDel(key: String, fields: String..., callback: @escaping redisResponseCallback) {
		sendCommand(name: "HDEL \(key) \(fields.joined(separator: " "))", callback: callback)
	}

	/// Get all field value pairs from the hash
	func hashGetAll(key: String, callback: @escaping redisResponseCallback) {
		sendCommand(name: "HGETALL \(key)", callback: callback)
	}

	/// Get all fields(keys) from the hash
	func hashKeys(key: String, callback: @escaping redisResponseCallback) {
		sendCommand(name: "HKEYS \(key)", callback: callback)
	}

	/// Get all values from the hash
	func hashValues(key: String, callback: @escaping redisResponseCallback) {
		sendCommand(name: "HVALS \(key)", callback: callback)
	}

	/// Get how many items are in the hash
	func hashLength(key: String, callback: @escaping redisResponseCallback) {
		sendCommand(name: "HLEN \(key)", callback: callback)
	}

	/// Get the string length of a field in the hash
	func hashStringLength(key: String, field: String, callback: @escaping redisResponseCallback) {
		sendCommand(name: "HSTRLEN \(key) \(field)", callback: callback)
	}

	/// Increment field by integer value
	func hashIncrementBy(key: String, field: String, by: Int, callback: @escaping redisResponseCallback) {
		sendCommand(name: "HINCRBY \(key) \(field) \(by)", callback: callback)
	}

	/// Increment field by float value
	func hashIncrementBy(key: String, field: String, by: Double, callback: @escaping redisResponseCallback) {
		sendCommand(name: "HINCRBYFLOAT \(key) \(field) \(by)", callback: callback)
	}

	/// Scans the hash `key` given the current cursor, which should start from zero.
	/// Optionally accepts a pattern and a maximum returned value count.
	func hashScan(key: String, cursor: Int = 0, pattern: String? = nil, count: Int? = nil, callback: @escaping redisResponseCallback) {
		sendCommand(name: "HSCAN \(key) \(cursor) \(pattern ?? "") \(nil == count ? "" : String(count!))", callback: callback)
	}

	/// Set field in the hash stored at key to value.
	func hashSet(key: String, field: String, value: RedisValue) throws -> RedisResponse {
		return try sendCommand(name: "HSET \(key) \(field) \(value.commandString)")
	}

	/// Set multiple field value pairs in an atomic operation
	func hashSet(key: String, fieldsValues: [(String, RedisValue)]) throws -> RedisResponse {
		return try sendCommand(name: "HMSET \(key) \(fieldsValues.map { "\($0.0) \($0.1.commandString)" }.joined(separator: " "))")
	}

	/// Set a field value pair if not exists
	func hashSetIfNonExists(key: String, field: String, value: RedisValue) throws -> RedisResponse {
		return try sendCommand(name: "HSETNX \(key) \(field) \(value.commandString)")
	}

	/// Get a field in the hash stored at key.
	func hashGet(key: String, field: String) throws -> RedisResponse {
		return try sendCommand(name: "HGET \(key) \(field)")
	}

	/// Get multiple fields in the hash stored at key.
	func hashGet(key: String, fields: [String]) throws -> RedisResponse {
		return try sendCommand(name: "HMGET \(key) \(fields.joined(separator: " "))")
	}

	/**
	Return 1 if field is an existing field in the hash stored at key.
	0 if does not exist
	*/
	func hashExists(key: String, field: String) throws -> RedisResponse {
		return try sendCommand(name: "HEXISTS \(key) \(field)")
	}

	/**
	Remove the specified fields from the hash stored at key.
	Specified fields that do not exist within this hash are ignored.
	If key does not exist, it is treated as an empty hash and this command returns 0.
	Otherwise the command returns the number of fields that have been deleted
	*/
	func hashDel(key: String, fields: String...) throws -> RedisResponse {
		return try sendCommand(name: "HDEL \(key) \(fields.joined(separator: " "))")
	}
	func hashDel(key: String, fields: [String]) throws -> RedisResponse {
		return try sendCommand(name: "HDEL \(key) \(fields.joined(separator: " "))")
	}

	/// Get all field value pairs from the hash
	func hashGetAll(key: String) throws -> RedisResponse {
		return try sendCommand(name: "HGETALL \(key)")
	}

	/// Get all fields(keys) from the hash
	func hashKeys(key: String) throws -> RedisResponse {
		return try sendCommand(name: "HKEYS \(key)")
	}

	/// Get all values from the hash
	func hashValues(key: String) throws -> RedisResponse {
		return try sendCommand(name: "HVALS \(key)")
	}

	/// Get how many items are in the hash
	func hashLength(key: String) throws -> RedisResponse {
		return try sendCommand(name: "HLEN \(key)")
	}

	/// Get the string length of a field in the hash
	func hashStringLength(key: String, field: String) throws -> RedisResponse {
		return try sendCommand(name: "HSTRLEN \(key) \(field)")
	}

	/// Increment field by integer value
	func hashIncrementBy(key: String, field: String, by: Int) throws -> RedisResponse {
		return try sendCommand(name: "HINCRBY \(key) \(field) \(by)")
	}

	/// Increment field by float value
	func hashIncrementBy(key: String, field: String, by: Double) throws -> RedisResponse {
		return try sendCommand(name: "HINCRBYFLOAT \(key) \(field) \(by)")
	}

	/// Scans the hash `key` given the current cursor, which should start from zero.
	/// Optionally accepts a pattern and a maximum returned value count.
	func hashScan(key: String, cursor: Int = 0, pattern: String? = nil, count: Int? = nil) throws -> RedisResponse {
		return try sendCommand(name: "HSCAN \(key) \(cursor) \(pattern ?? "") \(nil == count ? "" : String(count!))")
	}
}
