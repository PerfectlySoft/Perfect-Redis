import NIOCore

/// Sits in front of RediStack's reply decoder and rejects reply framing that would crash it.
///
/// RediStack 1.6's parser computes `size + 2` on a bulk string length (an overflow trap for
/// `$9223372036854775807`), reserves array capacity from an unchecked element count, and
/// recurses once per nesting level (a stack overflow for a few thousand nested arrays). A
/// real Redis never sends any of these, but a hostile or broken server, or anything on an
/// unencrypted path to it, could. This handler tracks the framing of the incoming byte
/// stream and, on anything out of bounds, raises an error instead of passing the bytes on.
/// RediStack then fails every pending command and closes the connection.
///
/// It only checks framing: lengths, counts, nesting depth, CRLF line ends and type bytes.
/// Contents are left
/// to RediStack's parser.
final class RESPReplyGuard: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer

    /// Redis caps a string at 512 MB by default (proto-max-bulk-len); this leaves room for a
    /// raised setting while keeping RediStack's arithmetic far from overflow.
    static let maxBulkLength = Int(UInt32.max)
    /// Redis's own aggregates hold at most 2^32 - 1 elements.
    static let maxArrayCount = Int(UInt32.max)
    /// Real replies nest a handful of levels deep.
    static let maxDepth = 64
    /// A length or count header is at most 20 digits, a sign and CR.
    static let maxHeaderLength = 24

    private enum State {
        /// Expecting the type byte of the next value.
        case type
        /// Inside a simple string, error or integer line; skip to its CRLF. `lastWasCR` carries
        /// the last byte of the previous read, in case CR and LF arrive separately.
        case line(lastWasCR: Bool)
        /// Reading the length of a bulk string (`$`) or the count of an array (`*`).
        case header(kind: UInt8, bytes: [UInt8])
        /// Skipping a bulk string's content and trailing CRLF.
        case body(remaining: Int)
        /// A violation was reported; drop everything after it.
        case failed
    }

    private var state = State.type
    /// Elements still to come in each open array, innermost last.
    private var openArrays: [Int] = []

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        if case .failed = state { return }
        let buffer = unwrapInboundIn(data)
        do {
            try scan(buffer.readableBytesView)
        } catch {
            state = .failed
            context.fireErrorCaught(error)
            return
        }
        context.fireChannelRead(data)
    }

    private func violation(_ what: String) -> RedisClient.CommandError {
        RedisClient.CommandError("Malformed reply from server: \(what)")
    }

    private func scan(_ bytes: ByteBufferView) throws {
        var i = bytes.startIndex
        while i < bytes.endIndex {
            switch state {
            case .failed:
                return

            case .type:
                let byte = bytes[i]
                i += 1
                switch byte {
                case UInt8(ascii: "+"), UInt8(ascii: "-"), UInt8(ascii: ":"):
                    state = .line(lastWasCR: false)
                case UInt8(ascii: "$"), UInt8(ascii: "*"):
                    state = .header(kind: byte, bytes: [])
                default:
                    throw violation("unknown type byte \(byte)")
                }

            case .line(let lastWasCR):
                guard let newline = bytes[i...].firstIndex(of: UInt8(ascii: "\n")) else {
                    state = .line(lastWasCR: bytes[bytes.index(before: bytes.endIndex)] == UInt8(ascii: "\r"))
                    i = bytes.endIndex
                    continue
                }
                // RediStack drops the byte before LF whatever it is, so a bare LF would make
                // it and this guard disagree about where the value ends.
                let crBefore = newline > i ? bytes[newline - 1] == UInt8(ascii: "\r") : lastWasCR
                guard crBefore else { throw violation("line ends in LF without CR") }
                i = newline + 1
                completeValue()

            case .header(let kind, var header):
                let byte = bytes[i]
                i += 1
                guard byte == UInt8(ascii: "\n") else {
                    header.append(byte)
                    guard header.count <= Self.maxHeaderLength else {
                        throw violation("length header too long")
                    }
                    state = .header(kind: kind, bytes: header)
                    continue
                }
                // As for lines: without the CR, RediStack would read one digit fewer.
                guard header.last == UInt8(ascii: "\r") else {
                    throw violation("length header ends in LF without CR")
                }
                header.removeLast()
                guard let value = Int(String(decoding: header, as: UTF8.self)) else {
                    throw violation("invalid length header")
                }
                try headerEnded(kind: kind, value: value)

            case .body(let remaining):
                let take = min(remaining, bytes.endIndex - i)
                i += take
                if take == remaining {
                    completeValue()
                } else {
                    state = .body(remaining: remaining - take)
                }
            }
        }
    }

    private func headerEnded(kind: UInt8, value: Int) throws {
        if kind == UInt8(ascii: "$") {
            switch value {
            case -1:
                completeValue()
            case 0...Self.maxBulkLength:
                state = .body(remaining: value + 2)
            default:
                throw violation("bulk string length \(value)")
            }
        } else {
            switch value {
            case -1, 0:
                completeValue()
            case 1...Self.maxArrayCount:
                guard openArrays.count < Self.maxDepth else {
                    throw violation("arrays nested more than \(Self.maxDepth) deep")
                }
                openArrays.append(value)
                state = .type
            default:
                throw violation("array count \(value)")
            }
        }
    }

    /// A value ended; count it against the arrays that contain it.
    private func completeValue() {
        state = .type
        while let last = openArrays.indices.last {
            openArrays[last] -= 1
            guard openArrays[last] == 0 else { return }
            openArrays.removeLast()
        }
    }
}
