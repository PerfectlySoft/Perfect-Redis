import Foundation
import NIOCore
import NIOPosix
import NIOConcurrencyHelpers
@testable import PerfectRedis

/// A one-connection TCP server on 127.0.0.1 that answers each read with the next scripted
/// reply, so tests can feed the client replies a real Redis would never send.
final class FakeRedisServer: Sendable {
    enum Step: Sendable {
        /// Write these bytes in reply to the next request.
        case reply([UInt8])
        /// Write nothing; the request goes unanswered.
        case silence
        /// Close the connection.
        case close
        /// Write these bytes, then close the connection.
        case replyThenClose([UInt8])
    }

    private let channel: Channel
    private let script: Script
    var port: Int { channel.localAddress?.port ?? 0 }
    /// How many client connections have closed so far.
    var closedConnections: Int { script.closed.withLockedValue { $0 } }

    init(_ steps: [Step]) async throws {
        let script = Script(steps)
        self.script = script
        channel = try await ServerBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .serverChannelOption(.socketOption(.so_reuseaddr), value: 1)
            .childChannelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(Handler(script: script))
                }
            }
            .bind(host: "127.0.0.1", port: 0)
            .get()
    }

    convenience init(replies: String...) async throws {
        try await self.init(replies.map { .reply(Array($0.utf8)) })
    }

    var identifier: RedisClientIdentifier {
        RedisClientIdentifier(withHost: "127.0.0.1", port: port)
    }

    func shutdown() async {
        try? await channel.close()
    }

    private final class Script: @unchecked Sendable {
        // Only touched on the child channel's event loop.
        var steps: [Step]
        let closed = NIOLockedValueBox(0)
        init(_ steps: [Step]) { self.steps = steps }
    }

    private final class Handler: ChannelInboundHandler {
        typealias InboundIn = ByteBuffer
        let script: Script
        init(script: Script) { self.script = script }

        func channelRead(context: ChannelHandlerContext, data: NIOAny) {
            guard !script.steps.isEmpty else { return }
            switch script.steps.removeFirst() {
            case .reply(let bytes):
                context.writeAndFlush(NIOAny(context.channel.allocator.buffer(bytes: bytes)), promise: nil)
            case .silence:
                break
            case .close:
                context.close(promise: nil)
            case .replyThenClose(let bytes):
                context.writeAndFlush(NIOAny(context.channel.allocator.buffer(bytes: bytes)), promise: nil)
                context.close(promise: nil)
            }
        }

        func channelInactive(context: ChannelHandlerContext) {
            script.closed.withLockedValue { $0 += 1 }
            context.fireChannelInactive()
        }
    }
}
