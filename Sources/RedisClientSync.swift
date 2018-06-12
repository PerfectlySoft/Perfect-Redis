//
//  RedisClientSync.swift
//  PerfectRedis
//
//  Created by Kyle Jessup on 2018-06-06.
//

import Foundation
import PerfectThread
import PerfectNet

extension RedisClient {
	func syncResponse<T>(_ sync: Promise<T>) throws -> T {
		guard let ret = try sync.wait() else {
			throw PerfectNetError.networkError(404, "Server was not available")
		}
		return ret
	}
}

public extension RedisClient {
	public struct CommandError: Error, CustomStringConvertible {
		public let description: String
		init(_ msg: String) {
			description = msg
		}
	}
	
	static func getClient(withIdentifier: RedisClientIdentifier) throws -> RedisClient {
		let net = withIdentifier.netGenerator()
		let sync = Promise<RedisClient> {
			(sync: Promise) in
			try net.connect(address: withIdentifier.host, port: UInt16(withIdentifier.port), timeoutSeconds: redisDefaultNetTimeout) {
				net in
				if let n = net {
					let client = RedisClient(net: n)
					if !withIdentifier.password.isEmpty {
						client.auth(withPassword: withIdentifier.password) {
							response in
							guard response.isSimpleOK else {
								return sync.fail(PerfectNetError.networkError(401, "Not authorized"))
							}
							sync.set(client)
						}
					} else {
						sync.set(client)
					}
				} else {
					sync.fail(PerfectNetError.networkError(404, "Server was not available"))
				}
			}
		}
		guard let ret = try sync.wait() else {
			throw PerfectNetError.networkError(404, "Server was not available")
		}
		return ret
	}
	
	public func sendCommand(name: String, parameters: [RedisResponse]) throws -> RedisResponse {
		let a = commandBytes(name: name, parameters: parameters)
		return try sendRawCommand(bytes: a)
	}
	
	public func sendCommand(name: String) throws -> RedisResponse {
		let a = commandBytes(name: name)
		return try sendRawCommand(bytes: a)
	}
	
	// Send command as serialized in RESP format: See https://redis.io/topics/protocol
	public func sendCommandAsRESP(name: String, parameters: [String]) throws -> RedisResponse {
		var array = [RedisResponse.bulkString(name.bytes)]
		array.append(contentsOf: parameters.flatMap({ RedisResponse.bulkString($0.bytes) }))
		return try sendRawCommand(bytes: RedisResponse.array(array).toBytes())
	}
	
	func sendRawCommand(bytes: [UInt8]) throws -> RedisResponse {
		let sync = Promise<RedisResponse> {
			(sync: Promise) in
			self.net.write(bytes: bytes) {
				wrote in
				guard wrote == bytes.count else {
					return sync.fail(CommandError("Failed to write all bytes"))
				}
				self.readResponse {
					response in
					if case .error(_, let msg) = response {
						sync.fail(CommandError(msg))
					} else {
						sync.set(response)
					}
				}
			}
		}
		return try syncResponse(sync)
	}
}

