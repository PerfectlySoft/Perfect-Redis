// Convenience wrapper — async methods over a named set key.
public struct RedisSet: Sendable {
    private let client: RedisClient
    public let key: String

    public init(client: RedisClient, key: String) {
        self.client = client
        self.key = key
    }

    public func add(_ elements: RedisClient.RedisValue...) async throws -> RedisResponse {
        try await client.setAdd(key: key, elements: elements)
    }
    public func add(_ elements: [RedisClient.RedisValue]) async throws -> RedisResponse {
        try await client.setAdd(key: key, elements: elements)
    }
    public func remove(_ value: RedisClient.RedisValue) async throws -> RedisResponse {
        try await client.setRemove(key: key, value: value)
    }
    public func remove(_ values: [RedisClient.RedisValue]) async throws -> RedisResponse {
        try await client.setRemove(key: key, values: values)
    }
    public func contains(_ value: RedisClient.RedisValue) async throws -> RedisResponse {
        try await client.setContains(key: key, value: value)
    }
    public func members() async throws -> RedisResponse {
        try await client.setMembers(key: key)
    }
    public func count() async throws -> RedisResponse {
        try await client.setCount(key: key)
    }
    public func randomPop(count: Int? = nil) async throws -> RedisResponse {
        if let count { return try await client.setRandomPop(key: key, count: count) }
        return try await client.setRandomPop(key: key)
    }
    public func randomGet(count: Int? = nil) async throws -> RedisResponse {
        if let count { return try await client.setRandomGet(key: key, count: count) }
        return try await client.setRandomGet(key: key)
    }
    public func scan(cursor: Int = 0, pattern: String? = nil, count: Int? = nil) async throws -> RedisResponse {
        try await client.setScan(key: key, cursor: cursor, pattern: pattern, count: count)
    }
}
