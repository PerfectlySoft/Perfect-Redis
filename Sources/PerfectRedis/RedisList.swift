// Convenience wrapper — async methods over a named list key.
public struct RedisList: Sendable {
    private let client: RedisClient
    public let key: String

    public init(client: RedisClient, key: String) {
        self.client = client
        self.key = key
    }

    public func prepend(_ values: RedisClient.RedisValue...) async throws -> RedisResponse {
        try await client.listPrepend(key: key, values: values)
    }
    public func append(_ values: RedisClient.RedisValue...) async throws -> RedisResponse {
        try await client.listAppend(key: key, values: values)
    }
    public func popFirst() async throws -> RedisResponse {
        try await client.listPopFirst(key: key)
    }
    public func popLast() async throws -> RedisResponse {
        try await client.listPopLast(key: key)
    }
    public func length() async throws -> RedisResponse {
        try await client.listLength(key: key)
    }
    public func range(start: Int, stop: Int) async throws -> RedisResponse {
        try await client.listRange(key: key, start: start, stop: stop)
    }
    public func trim(start: Int, stop: Int) async throws -> RedisResponse {
        try await client.listTrim(key: key, start: start, stop: stop)
    }
    public func get(index: Int) async throws -> RedisResponse {
        try await client.listGetElement(key: key, index: index)
    }
    public func set(index: Int, value: RedisClient.RedisValue) async throws -> RedisResponse {
        try await client.listSet(key: key, index: index, value: value)
    }
    public func insert(_ element: RedisClient.RedisValue, before pivot: RedisClient.RedisValue) async throws -> RedisResponse {
        try await client.listInsert(key: key, element: element, before: pivot)
    }
    public func insert(_ element: RedisClient.RedisValue, after pivot: RedisClient.RedisValue) async throws -> RedisResponse {
        try await client.listInsert(key: key, element: element, after: pivot)
    }
    public func removeMatching(_ value: RedisClient.RedisValue, count: Int) async throws -> RedisResponse {
        try await client.listRemoveMatching(key: key, value: value, count: count)
    }
}
