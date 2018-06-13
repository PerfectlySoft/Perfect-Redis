//
//  RedisHash.swift
//  PerfectRedis
//
//  Created by Kyle Jessup on 2018-06-11.
//

import Foundation

public struct RedisHash {
	public typealias Element = (key: KeyType, value: ValueType)
	public typealias KeyType = String
	public typealias ValueType = RedisClient.RedisValue
	let client: RedisClient
	let name: String
	public var exists: Bool {
		return 1 == (try? client.exists(keys: name))?.integer ?? 0
	}
	public var count: Int {
		return (try? client.hashLength(key: name))?.integer ?? 0
	}
	public var keys: [String] {
		guard let response = try? client.hashKeys(key: name), case .array(let a) = response else {
			return []
		}
		return a.compactMap { $0.string }
	}
	public var values: [ValueType] {
		guard let response = try? client.hashValues(key: name), case .array(let a) = response else {
			return []
		}
		return a.compactMap { $0.value }
	}
	public init(_ client: RedisClient, name: String) {
		self.client = client
		self.name = name
	}
	public subscript(_ key: KeyType) -> ValueType? {
		get {
			return (try? client.hashGet(key: name, field: key))?.value
		}
		set {
			if let newValue = newValue {
				_ = try? client.hashSet(key: name, field: key, value: newValue)
			} else {
				_ = try? client.hashDel(key: name, fields: key)
			}
		}
	}
}

extension RedisHash: Sequence {
	public func makeIterator() -> Iterator {
		guard let response = try? client.hashGetAll(key: name),
				case .array(let a) = response,
				!a.isEmpty,
				a.count % 2 == 0 else {
			return Iterator(items: [])
		}
		return Iterator(items: a.compactMap { $0.value })
	}
	
	public struct Iterator: IteratorProtocol {
		public typealias Element = RedisHash.Element
		var index = 0
		var items: Array<RedisHash.ValueType>.Iterator
		init(items: [RedisHash.ValueType]) {
			self.items = items.makeIterator()
		}
		public mutating func next() -> Element? {
			guard let name = items.next()?.string,
				let value = items.next() else {
				return nil
			}
			
			
			return nil
		}
	}
}

public extension RedisClient {
	func hash(named: String) -> RedisHash {
		return RedisHash(self, name: named)
	}
}
