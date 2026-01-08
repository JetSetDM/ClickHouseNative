// swift-tools-version: 6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "ClickHouseNative",
    platforms: [
        .macOS(.v10_15),
        .iOS(.v13),
        .tvOS(.v13),
        .watchOS(.v6)
    ],
    products: [
        .library(
            name: "ClickHouseNative",
            targets: ["ClickHouseNative"]
        ),
        .library(
            name: "ClickHouseNativeCore",
            targets: ["ClickHouseNativeCore"]
        ),
        .library(
            name: "ClickHouseNativeNIO",
            targets: ["ClickHouseNativeNIO"]
        ),
        .executable(
            name: "ClickHouseNativeSample",
            targets: ["ClickHouseNativeSample"]
        ),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.67.0"),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", from: "2.30.0"),
        .package(url: "https://github.com/attaswift/BigInt.git", from: "5.0.0")
    ],
    targets: [
        .systemLibrary(
            name: "CLZ4",
            pkgConfig: "liblz4",
            providers: [
                .brew(["lz4"]),
                .apt(["liblz4-dev"])
            ]
        ),
        .target(
            name: "ClickHouseNativeCore",
            dependencies: [
                .product(name: "NIOCore", package: "swift-nio"),
                "CLZ4",
                .product(name: "BigInt", package: "BigInt")
            ]
        ),
        .target(
            name: "ClickHouseNativeNIO",
            dependencies: [
                "ClickHouseNativeCore",
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOSSL", package: "swift-nio-ssl")
            ]
        ),
        .target(
            name: "ClickHouseNative",
            dependencies: ["ClickHouseNativeCore", "ClickHouseNativeNIO"]
        ),
        .executableTarget(
            name: "ClickHouseNativeSample",
            dependencies: [
                "ClickHouseNative"
            ],
            path: "Sample"
        ),
        .testTarget(
            name: "ClickHouseNativeTests",
            dependencies: [
                "ClickHouseNative",
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio")
            ]
        ),
    ]
)
