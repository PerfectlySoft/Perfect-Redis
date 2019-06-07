//
//  RedisSet.swift
//  RoboRepNotifications
//
//  Created by Kyle Jessup on 2019-06-06.
//

import Foundation

public struct RedisSet {
	let client: RedisClient
	let name: String
	public init(_ client: RedisClient, name: String) {
		self.client = client
		self.name = name
	}
}

public extension RedisClient {
	func set(named: String) -> RedisSet {
		return RedisSet(self, name: named)
	}
}

extension RedisSet {
	public var exists: Bool {
		return 1 == (try? client.exists(keys: name))?.integer ?? 0
	}
	public var count: Int {
		return (try? client.setCount(key: name))?.integer ?? 0
	}
	public var isEmpty: Bool {
		return count == 0
	}
	public func contains(_ value: String) -> Bool {
		return 1 == (try? client.setContains(key: name, value: .string(value)))?.integer ?? 0
	}
	public func insert(_ value: String) {
		_ = try? client.setAdd(key: name, elements: [.string(value)])
	}
	public func remove(_ value: String) {
		_ = try? client.setRemove(key: name, value: .string(value))
	}
	public func values() -> [String] {
		guard let resp = try? client.setMembers(key: name) else {
			return []
		}
		if case .array(let a) = resp {
			return a.compactMap { $0.string }
		}
		if let s = resp.string {
			return [s]
		}
		return []
	}
}
