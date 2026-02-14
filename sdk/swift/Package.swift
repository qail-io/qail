// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "Qail",
    platforms: [
        .iOS(.v15),
        .macOS(.v12),
        .tvOS(.v15),
        .watchOS(.v8),
    ],
    products: [
        .library(
            name: "Qail",
            targets: ["Qail"]
        ),
    ],
    targets: [
        .target(
            name: "Qail",
            path: "Sources/Qail"
        ),
        .testTarget(
            name: "QailTests",
            dependencies: ["Qail"],
            path: "Tests/QailTests"
        ),
    ]
)
