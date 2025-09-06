// swift-tools-version: 6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "AvidyneR9ToGarmin",
    platforms: [.macOS(.v15)],

    products: [
        // Products define the executables and libraries a package produces, and make them visible to other packages.
        .library(name: "libAvidyneR9ToGarmin", targets: ["libAvidyneR9ToGarmin"]),
        .executable(name: "avidyne-r9-to-garmin", targets: ["AvidyneR9ToGarmin"])
    ],
    dependencies: [
        // Dependencies declare other packages that this package depends on.
        .package(url: "https://github.com/dehesa/CodableCSV.git", from: "0.6.7"),
        .package(url: "https://github.com/apple/swift-argument-parser.git", from: "1.5.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.6.1"),
        .package(url: "https://github.com/nicklockwood/VectorMath.git", branch: "master"),
        .package(url: "https://github.com/jkandzi/Progress.swift.git", from: "0.4.0")
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages this package depends on.
        .target(
            name: "libAvidyneR9ToGarmin",
            dependencies: [
                .product(name: "CodableCSV", package: "CodableCSV"),
                .product(name: "Logging", package: "swift-log"),
                "VectorMath"
            ]),
        .executableTarget(
            name: "AvidyneR9ToGarmin",
            dependencies: [
                .target(name: "libAvidyneR9ToGarmin"),
                .product(name: "ArgumentParser", package: "swift-argument-parser"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "Progress", package: "Progress.swift")
            ]),
        .testTarget(
            name: "AvidyneR9ToGarminTests",
            dependencies: ["libAvidyneR9ToGarmin"])
    ],

    swiftLanguageModes: [.v6]
)
