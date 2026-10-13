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

        /// A reply to a command sent before subscribing must not reach RediStack's pub/sub
        /// handler: one shaped like a subscription confirmation crashed the process, and one
        /// shaped like a message was delivered as a forged message while its caller hung.
        @Test(arguments: ["{'subscribe','x','y'}", "{'message','c','forged'}"])
        func subscribeWaitsForCommandsAlreadySent(_ result: String) async throws {
            let client = try await RedisTestEnvironment.connect()
            let script = "local i=0 while i<20000000 do i=i+1 end return \(result)"
            let slow = Task { try await client.sendCommandAsRESP(name: "EVAL", parameters: [script, "0"]) }
            try await Task.sleep(nanoseconds: 50_000_000)
            try await client.subscribe(channels: ["c"])
            let reply = try await slow.value
            guard case .array(let items) = reply else {
                Issue.record("Expected the script's array, got \(reply)"); return
            }
            #expect(items.count == 3)

            let publisher = try await RedisClient.connect(withIdentifier: RedisTestEnvironment.identifier)
            _ = try await publisher.publish(channel: "c", message: .string("real"))
            var iterator = client.messages.makeAsyncIterator()
            let first = await iterator.next()
            #expect(first?.message.string == "real")
            try await publisher.close()
            try await client.close()
        }

        @Test func subscribedClientRefusesOrdinaryCommandsUntilFullyUnsubscribed() async throws {
            let client = try await RedisTestEnvironment.connect()
            try await client.subscribe(channels: ["a"])
            try await client.subscribe(patterns: ["p*"])
            await #expect(throws: RedisClient.CommandError.self) { try await client.get(key: "k") }
            try await client.unsubscribe(channels: [])
            await #expect(throws: RedisClient.CommandError.self) { try await client.ping() }
            try await client.unsubscribe(patterns: [])
            let pong = try await client.ping()
            #expect(pong.string == "PONG")
            try await client.close()
        }

        @Test func subscribeInsideMultiIsRefused() async throws {
            let client = try await RedisTestEnvironment.connect()
            await #expect(throws: RedisClient.CommandError.self) {
                _ = try await client.multi { try await client.subscribe(channels: ["a"]) }
            }
            let pong = try await client.ping()
            #expect(pong.string == "PONG")
            try await client.close()
        }

        @Test func emptySubscriptionListIsRejected() async throws {
            let client = try await RedisTestEnvironment.connect()
            await #expect(throws: RedisClient.CommandError.self) { try await client.subscribe(channels: []) }
            await #expect(throws: RedisClient.CommandError.self) { try await client.subscribe(patterns: []) }
            try await client.close()
        }
    }
}
