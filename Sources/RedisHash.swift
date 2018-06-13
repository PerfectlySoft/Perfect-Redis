//
//  RedisHash.swift
//  PerfectRedis
//
//  Created by Kyle Jessup on 2018-06-11.
//

import Foundation

public struct RedisHash {
	public typealias Element = (key: String, value: RedisClient.RedisValue)
	public typealias KeyType = String
	public typealias ValueType = RedisClient.RedisValue
	let client: RedisClient
	let name: String
	public var exists: Bool {
		guard let response = try? client.exists(keys: name),
				case .integer(let i) = response,
				i == 1 else {
			return false
		}
		return true
	}
	public init(_ client: RedisClient, name: String) {
		self.client = client
		self.name = name
	}
	public subscript(_ key: KeyType) -> ValueType? {
		get {
			guard let r = try? client.hashGet(key: name, field: key) else {
				return nil
			}
			return r.value
		}
		set {
			guard let newValue = newValue else {
				_ = try? client.hashDel(key: name, fields: key)
				return
			}
			_ = try? client.hashSet(key: name, field: key, value: newValue)
		}
	}
}
