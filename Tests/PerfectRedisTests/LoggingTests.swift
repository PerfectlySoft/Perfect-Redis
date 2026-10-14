import Foundation
import Logging
import NIOConcurrencyHelpers
import Testing
@testable import PerfectRedis

/// RediStack logs commands' arguments (AUTH's password included) at debug level.
@Suite(.timeLimit(.minutes(1))) struct LoggingTests {
    /// Collects every record logged through it, whatever the level.
    final class Capture: LogHandler, @unchecked Sendable {
        let lines = NIOLockedValueBox<[String]>([])
        var metadata: Logger.Metadata = [:]
        var logLevel: Logger.Level = .trace
        subscript(metadataKey key: String) -> Logger.Metadata.Value? {
            get { metadata[key] }
            set { metadata[key] = newValue }
        }
        // swift-log 1.11's LogEvent API; the tests resolve the latest swift-log.
        func log(event: LogEvent) {
            let all = metadata.merging(event.metadata ?? [:]) { $1 }
            lines.withLockedValue { $0.append("\(event.level) \(event.message) \(all)") }
        }
    }

    @Test func connectionsLogNothingByDefault() {
        #expect(RedisClientIdentifier().connectionLogger.handler is SwiftLogNoOpLogHandler)
        let id = RedisClientIdentifier(withHost: "h", port: 1, password: "secret")
        #expect(id.connectionLogger.handler is SwiftLogNoOpLogHandler)
    }

    @Test func aSuppliedLoggerReceivesRediStacksLogs() async throws {
        let capture = Capture()
        let logger = Logger(label: "test", factory: { _ in capture })
        let server = try await FakeRedisServer(replies: "+OK\r\n", "-ERR boom\r\n")
        let id = RedisClientIdentifier(withHost: "127.0.0.1", port: server.port, password: "hunter2", logger: logger)
        let client = try await RedisClient.connect(withIdentifier: id)
        await #expect(throws: (any Error).self) { try await client.ping() }
        let lines = capture.lines.withLockedValue { $0 }
        #expect(lines.contains { $0.contains("AUTH") })
        #expect(lines.contains { $0.hasPrefix("error") })
        try? await client.close()
        await server.shutdown()
    }
}
