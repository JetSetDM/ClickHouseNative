// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "ClickHouseNativeTools",
    platforms: [
        .macOS(.v10_15),
        .iOS(.v13),
        .tvOS(.v13),
        .watchOS(.v6)
    ],
    products: [
        .executable(
            name: "ClickHouseNativeBench",
            targets: ["ClickHouseNativeBench"]
        ),
        .executable(
            name: "ClickHouseNativeProbe",
            targets: ["ClickHouseNativeProbe"]
        )
    ],
    dependencies: [
        .package(path: ".."),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.67.0")
    ],
    targets: [
        .executableTarget(
            name: "ClickHouseNativeBench",
            dependencies: [
                .product(name: "ClickHouseNativeCore", package: "ClickHouseNative"),
                .product(name: "NIOCore", package: "swift-nio")
            ]
        ),
        .executableTarget(
            name: "ClickHouseNativeProbe",
            dependencies: [
                .product(name: "ClickHouseNative", package: "ClickHouseNative"),
                .product(name: "ClickHouseNativeNIO", package: "ClickHouseNative")
            ]
        )
    ]
)
