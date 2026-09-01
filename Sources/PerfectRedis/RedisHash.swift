// Convenience wrapper — async methods over a named hash key.
public struct RedisHash: Sendable {
    private let client: RedisClient
    public let key: String

    public init(client: RedisClient, key: String) {
        self.client = client
        self.key = key
    }

    public func get(field: String) async throws -> RedisResponse {
        try await client.hashGet(key: key, field: field)
    }
    public func get(fields: [String]) async throws -> RedisResponse {
        try await client.hashGet(key: key, fields: fields)
    }
    public func set(field: String, value: RedisClient.RedisValue) async throws -> RedisResponse {
        try await client.hashSet(key: key, field: field, value: value)
    }
    public func set(fieldsValues: [(String, RedisClient.RedisValue)]) async throws -> RedisResponse {
        try await client.hashSet(key: key, fieldsValues: fieldsValues)
    }
    public func setIfNonExists(field: String, value: RedisClient.RedisValue) async throws -> RedisResponse {
        try await client.hashSetIfNonExists(key: key, field: field, value: value)
    }
    public func exists(field: String) async throws -> RedisResponse {
        try await client.hashExists(key: key, field: field)
    }
    public func del(fields: String...) async throws -> RedisResponse {
        try await client.hashDel(key: key, fields: fields)
    }
    public func del(fields: [String]) async throws -> RedisResponse {
        try await client.hashDel(key: key, fields: fields)
    }
    public func getAll() async throws -> RedisResponse {
        try await client.hashGetAll(key: key)
    }
    public func keys() async throws -> RedisResponse {
        try await client.hashKeys(key: key)
    }
    public func values() async throws -> RedisResponse {
        try await client.hashValues(key: key)
    }
    public func length() async throws -> RedisResponse {
        try await client.hashLength(key: key)
    }
    public func stringLength(field: String) async throws -> RedisResponse {
        try await client.hashStringLength(key: key, field: field)
    }
    public func increment(field: String, by: Int) async throws -> RedisResponse {
        try await client.hashIncrementBy(key: key, field: field, by: by)
    }
    public func increment(field: String, by: Double) async throws -> RedisResponse {
        try await client.hashIncrementBy(key: key, field: field, by: by)
    }
    public func scan(cursor: Int = 0, pattern: String? = nil, count: Int? = nil) async throws -> RedisResponse {
        try await client.hashScan(key: key, cursor: cursor, pattern: pattern, count: count)
    }
}
