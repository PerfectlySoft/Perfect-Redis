import Foundation
import Testing
@testable import PerfectRedis

extension LiveRedisTests {
    @Suite struct PubSubTests {
        /// Collects `count` messages from `client`, giving up after five seconds.
        func receive(_ count: Int, from client: RedisClient) -> Task<[RedisClient.PublishedMessage], Never> {
            let collector = Task {
                var received: [RedisClient.PublishedMessage] = []
                for await message in client.messages {
                    received.append(message)
                    if received.count == count { break }
                }
                return received
            }
            Task {
                try? await Task.sleep(nanoseconds: 5_000_000_000)
                collector.cancel()
            }
            return collector
        }

        @Test func subscribedClientReceivesMessagesAndStaysInStep() async throws {
            let subscriber = try await RedisTestEnvironment.connect()
            let publisher = try await RedisClient.connect(withIdentifier: RedisTestEnvironment.identifier)
            let received = receive(3, from: subscriber)

            // Two channels at once: Redis sends a confirmation per channel.
            try await subscriber.subscribe(channels: ["news", "sport"])
            try await subscriber.subscribe(patterns: ["weather.*"])
            #expect(try await publisher.publish(channel: "news", message: .string("hello")).integer == 1)
            #expect(try await publisher.publish(channel: "sport", message: .binary([0, 13, 10, 255])).integer == 1)
            #expect(try await publisher.publish(channel: "weather.denver", message: .string("")).integer == 1)

            let messages = await received.value
            #expect(messages.map(\.channel) == ["news", "sport", "weather.denver"])
            #expect(messages.map { $0.message.string } == ["hello", nil, ""])  // [0, 13, 10, 255] isn't UTF-8
            if case .bulkString(let bytes) = messages.dropFirst().first?.message {
                #expect(bytes == [0, 13, 10, 255])
            }

            // Once every subscription ends, ordinary commands work and get their own replies.
            try await subscriber.unsubscribe(channels: [])
            try await subscriber.unsubscribe(patterns: [])
            #expect(try await subscriber.ping().string == "PONG")
            _ = try await subscriber.set(key: "after", value: .string("subscribing"))
            #expect(try await subscriber.get(key: "after").string == "subscribing")

            try await subscriber.close()
            try await publisher.close()
        }

        @Test func unsubscribedChannelsStopDelivering() async throws {
            let subscriber = try await RedisTestEnvironment.connect()
            let publisher = try await RedisClient.connect(withIdentifier: RedisTestEnvironment.identifier)
            let received = receive(1, from: subscriber)
            try await subscriber.subscribe(channels: ["a", "b"])
            try await subscriber.unsubscribe(channels: ["a"])
            #expect(try await publisher.publish(channel: "a", message: .string("dropped")).integer == 0)
            #expect(try await publisher.publish(channel: "b", message: .string("kept")).integer == 1)
            let messages = await received.value
            #expect(messages.map(\.channel) == ["b"])
            #expect(messages.first?.message.string == "kept")
            try await subscriber.close()
            try await publisher.close()
        }

        @Test func closingEndsTheMessageStream() async throws {
            let client = try await RedisTestEnvironment.connect()
            try await client.subscribe(channels: ["x"])
            let drained = Task { for await _ in client.messages {} ; return true }
            try await client.close()
            #expect(await drained.value)
        }

        @Test func emptySubscriptionListIsRejected() async throws {
            let client = try await RedisTestEnvironment.connect()
            await #expect(throws: RedisClient.CommandError.self) { try await client.subscribe(channels: []) }
            await #expect(throws: RedisClient.CommandError.self) { try await client.subscribe(patterns: []) }
            try await client.close()
        }
    }
}
