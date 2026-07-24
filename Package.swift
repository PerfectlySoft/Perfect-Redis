// swift-tools-version: 6.2
import PackageDescription

let package = Package(
    name: "PerfectRedis",
    platforms: [.macOS(.v12)],
    products: [
        .library(name: "PerfectRedis", targets: ["PerfectRedis"]),
    ],
    dependencies: [
        .package(url: "https://github.com/swift-server/RediStack.git", from: "1.6.0"),
    ],
    targets: [
        .target(
            name: "PerfectRedis",
            dependencies: [
                .product(name: "RediStack", package: "RediStack"),
            ],
            swiftSettings: [.swiftLanguageMode(.v6)]
        ),
        .testTarget(
            name: "PerfectRedisTests",
            dependencies: ["PerfectRedis"],
            swiftSettings: [.swiftLanguageMode(.v6)]
        ),
    ]
)
