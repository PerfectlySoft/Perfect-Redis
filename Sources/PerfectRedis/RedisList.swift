//
//  RedisList.swift
//  PerfectRedis
//
//  Created by Kyle Jessup on 2018-06-12.
//

import Foundation

public struct RedisList {
	let client: RedisClient
	let name: String
	public init(_ client: RedisClient, name: String) {
		self.client = client
		self.name = name
	}
}

public extension RedisClient {
	func list(named: String) -> RedisList {
		return RedisList(self, name: named)
	}
}

extension RedisList: Sequence {
	public struct Iterator: IteratorProtocol {
		public typealias Element = RedisList.Element
		let list: RedisList
		var index: RedisList.Index
		let endIndex: RedisList.Index
		init(list: RedisList, startIndex: RedisList.Index = 0, endIndex: RedisList.Index = -1) {
			self.list = list
			self.index = startIndex
			self.endIndex = endIndex
		}
		public mutating func next() -> Element? {
			guard endIndex == -1 || index < endIndex else {
				return nil
			}
			if let v = list[index] {
				index += 1
				return v
			}
			return nil
		}
	}
	public typealias Index = Int
	public typealias Element = RedisClient.RedisValue
	public func makeIterator() -> RedisList.Iterator {
		return Iterator(list: self)
	}
}

extension RedisList {
	public var exists: Bool {
		return 1 == (try? client.exists(keys: name))?.integer ?? 0
	}
	public var count: Int {
		return (try? client.listLength(key: name))?.integer ?? 0
	}
	public var isEmpty: Bool {
		return count == 0
	}
	public var values: [Element] {
		guard let response = try? client.listRange(key: name, start: 0, stop: -1),
			case .array(let a) = response else {
				return []
		}
		return a.compactMap { $0.value }
	}
	public var first: Element? {
		return self[0]
	}
	public var last: Element? {
		return self[-1]
	}
	public subscript(position: Index) -> Element? {
		return (try? client.listGetElement(key: name, index: position))?.value
	}
	/*
	public subscript(positions: Range<Index>) -> SubSequence {
		return SubSequence({
			return Iterator(list: self, startIndex: positions.startIndex, endIndex: positions.endIndex)
		})
	}
	*/
	public func popFirst() -> Element? {
		return (try? client.listPopFirst(key: name))?.value
	}
	public func popFirst(timeout: Int) -> Element? {
		return (try? client.listPopFirstBlocking(keys: name, timeout: timeout))?.value
	}
	public func popLast() -> Element? {
		return (try? client.listPopLast(key: name))?.value
	}
	public func popLast(timeout: Int) -> Element? {
		return (try? client.listPopLastBlocking(keys: name, timeout: timeout))?.value
	}
	public func popLast(appendTo: String) -> Element? {
		return (try? client.listPopLastAppend(sourceKey: name, destKey: appendTo))?.value
	}
	public func popLast(appendTo: String, timeout: Int) -> Element? {
		return (try? client.listPopLastAppendBlocking(sourceKey: name, destKey: appendTo, timeout: timeout))?.value
	}
	public func removeFirst(_ count: Int = 1) {
		_ = try? client.listTrim(key: name, start: count, stop: -1)
	}
	public func removeLast(_ count: Int = 1) {
		_ = try? client.listTrim(key: name, start: 0, stop: -count)
	}
	@discardableResult
	public func remove(matching: RedisValueRepresentable, count: Int = 0) -> Int {
		return (try? client.listRemoveMatching(key: name, value: matching.redisValue, count: count))?.integer ?? 0
	}
	
	@discardableResult
	public func append(_ element: RedisValueRepresentable) -> Int {
		return (try? client.listAppend(key: name, values: [element.redisValue]))?.integer ?? 0
	}
	@discardableResult
	public func prepend(_ element: RedisValueRepresentable) -> Int {
		return (try? client.listPrepend(key: name, values: [element.redisValue]))?.integer ?? 0
	}
	@discardableResult
	public func appendIfExists(_ element: RedisValueRepresentable) -> Int {
		return (try? client.listAppendX(key: name, value: element.redisValue))?.integer ?? 0
	}
	@discardableResult
	public func prependIfExists(_ element: RedisValueRepresentable) -> Int {
		return (try? client.listPrependX(key: name, value: element.redisValue))?.integer ?? 0
	}
	public func insert(_ element: RedisValueRepresentable, at: Index) {
		_ = try? client.listSet(key: name, index: at, value: element.redisValue)
	}
	@discardableResult
	public func insert(_ element: RedisValueRepresentable, after: RedisValueRepresentable) -> Int {
		return (try? client.listInsert(key: name, element: element.redisValue, after: after.redisValue))?.integer ?? 0
	}
	@discardableResult
	public func insert(_ element: RedisValueRepresentable, before: RedisValueRepresentable) -> Int {
		return (try? client.listInsert(key: name, element: element.redisValue, before: before.redisValue))?.integer ?? 0
	}
}
