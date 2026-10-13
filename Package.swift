// swift-tools-version: 6.2
import PackageDescription

let package = Package(
    name: "PerfectRedis",
    platforms: [.macOS(.v12), .iOS(.v15)],
    products: [
        .library(name: "PerfectRedis", targets: ["PerfectRedis"]),
    ],
    dependencies: [
        .package(url: "https://github.com/swift-server/RediStack.git", from: "1.6.0"),
        // Already a RediStack dependency; the library imports NIOCore and NIOPosix
        // directly, and the tests run a scripted fake server on it.
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.65.0"),
        // Also already RediStack's; the client gives RediStack a silent logger by default.
        .package(url: "https://github.com/apple/swift-log.git", from: "1.5.3"),
    ],
    targets: [
        .target(
            name: "PerfectRedis",
            dependencies: [
                .product(name: "RediStack", package: "RediStack"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOConcurrencyHelpers", package: "swift-nio"),
                .product(name: "Logging", package: "swift-log"),
            ],
            swiftSettings: [.swiftLanguageMode(.v6)]
        ),
        .testTarget(
            name: "PerfectRedisTests",
            dependencies: [
                "PerfectRedis",
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOEmbedded", package: "swift-nio"),
                .product(name: "NIOConcurrencyHelpers", package: "swift-nio"),
                .product(name: "Logging", package: "swift-log"),
            ],
            swiftSettings: [.swiftLanguageMode(.v6)]
        ),
    ]
)
