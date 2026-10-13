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

        /// While a subscribe waits for a command in flight, nothing else may reset the client's
        /// subscribing state: a concurrent unsubscribe used to, letting a PING go out
        /// mid-SUBSCRIBE.
        @Test func concurrentUnsubscribeDoesNotReopenTheClient() async throws {
            let client = try await RedisTestEnvironment.connect()
            let blpop = Task { try await client.listPopFirstBlocking(keys: "nothing", timeout: 1) }
            try await Task.sleep(nanoseconds: 100_000_000)
            let subscribe = Task { try await client.subscribe(channels: ["x"]) }
            try await Task.sleep(nanoseconds: 100_000_000)
            let unsubscribe = Task { try await client.unsubscribe(channels: []) }
            try await Task.sleep(nanoseconds: 100_000_000)
            let ping = await bounded(3, "PING while subscribing") { try await client.ping() }
            if case .success(let reply)? = ping { Issue.record("PING went out while subscribing: \(reply)") }
            _ = await bounded(5, "BLPOP") { try await blpop.value }
            _ = await bounded(5, "subscribe") { try await subscribe.value }
            _ = await bounded(5, "unsubscribe") { try await unsubscribe.value }
            try? await client.close()
        }

        @Test func cancelledSubscribeDoesNotReopenTheClient() async throws {
            let client = try await RedisTestEnvironment.connect()
            let blpop = Task { try await client.listPopFirstBlocking(keys: "nothing", timeout: 1) }
            try await Task.sleep(nanoseconds: 100_000_000)
            let a = Task { try await client.subscribe(channels: ["a"]) }
            let b = Task { try await client.subscribe(channels: ["b"]) }
            try await Task.sleep(nanoseconds: 100_000_000)
            a.cancel()
            _ = await bounded(3, "cancelled subscribe") { try await a.value }
            let ping = await bounded(3, "PING while b subscribes") { try await client.ping() }
            if case .success(let reply)? = ping { Issue.record("PING went out while subscribing: \(reply)") }
            _ = await bounded(5, "BLPOP") { try await blpop.value }
            let bOutcome = await bounded(5, "subscribe b") { try await b.value }
            if case .failure(let error)? = bOutcome { Issue.record("subscribe b failed: \(error)") }
            try? await client.close()
        }

        /// An unsubscribe-all racing a subscribe used to leave the subscribe waiting forever
        /// while the server had the client subscribed.
        @Test func unsubscribeAllRacingSubscribeBothFinish() async throws {
            let client = try await RedisTestEnvironment.connect()
            let publisher = try await RedisClient.connect(withIdentifier: RedisTestEnvironment.identifier)
            try await client.subscribe(channels: ["x"])
            let unsubscribe = Task { try await client.unsubscribe(channels: []) }
            let subscribe = Task { try await client.subscribe(channels: ["y"]) }
            let u = await bounded(4, "unsubscribe-all") { try await unsubscribe.value }
            let s = await bounded(4, "subscribe y") { try await subscribe.value }
            if case .failure(let error)? = u { Issue.record("unsubscribe failed: \(error)") }
            if case .failure(let error)? = s { Issue.record("subscribe failed: \(error)") }
            // They ran in order, so the client ends subscribed to y only.
            #expect(try await publisher.publish(channel: "y", message: .string("m")).integer == 1)
            #expect(try await publisher.publish(channel: "x", message: .string("m")).integer == 0)
            try? await client.close()
            try await publisher.close()
        }

        /// RediStack 1.6.3 drops the server's reply when an unsubscribe names nothing it is
        /// tracking, so these used to wait forever and leave the client refusing commands.
        @Test(arguments: [
            "channels-all with only patterns", "patterns-all with only channels",
            "unknown channel", "duplicate channel", "same channel twice", "nothing subscribed",
        ])
        func unsubscribeFromNothingReturns(_ scenario: String) async throws {
            let client = try await RedisTestEnvironment.connect()
            let finished = await bounded(4, scenario) {
                switch scenario {
                case "channels-all with only patterns":
                    try await client.subscribe(patterns: ["p*"])
                    try await client.unsubscribe(channels: [])
                    try await client.unsubscribe(patterns: [])
                case "patterns-all with only channels":
                    try await client.subscribe(channels: ["c"])
                    try await client.unsubscribe(patterns: [])
                    try await client.unsubscribe(channels: [])
                case "unknown channel":
                    try await client.subscribe(channels: ["a", "b"])
                    try await client.unsubscribe(channels: ["zzz"])
                    try await client.unsubscribe(channels: ["a", "b"])
                case "duplicate channel":
                    try await client.subscribe(channels: ["a", "a", "b"])
                    try await client.unsubscribe(channels: ["a", "a"])
                    try await client.unsubscribe(channels: ["b"])
                case "same channel twice":
                    try await client.subscribe(channels: ["a", "b"])
                    try await client.unsubscribe(channels: ["a"])
                    try await client.unsubscribe(channels: ["a"])
                    try await client.unsubscribe(channels: ["b"])
                default:
                    try await client.unsubscribe(channels: [])
                    try await client.unsubscribe(patterns: ["p*"])
                }
                return try await client.ping()
            }
            if case .success(let pong)? = finished {
                #expect(pong.string == "PONG", "Fully unsubscribed, so PING should work normally")
            } else if case .failure(let error)? = finished {
                Issue.record("\(scenario): \(error)")
            }
            try? await client.close()
        }

        @Test func emptySubscriptionListIsRejected() async throws {
            let client = try await RedisTestEnvironment.connect()
            await #expect(throws: RedisClient.CommandError.self) { try await client.subscribe(channels: []) }
            await #expect(throws: RedisClient.CommandError.self) { try await client.subscribe(patterns: []) }
            try await client.close()
        }
    }
}
