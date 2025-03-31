import Foundation
@preconcurrency import VectorMath

actor R9RecordCombiner<RecordType: R9Record> {
    private let records: [RecordType]

    init(records: [RecordType]) {
        self.records = records
    }

    func get(_ keyPath: KeyPath<RecordType, Int?>, strategy: AverageStrategy = .mean) -> Int? {
        let values = records.compactMap { $0[keyPath: keyPath] }
        guard !values.isEmpty else { return nil }
        return average(values, strategy: strategy)
    }

    func get(_ keyPath: KeyPath<RecordType, UInt?>, strategy: AverageStrategy = .mean) -> UInt? {
        let values = records.compactMap { $0[keyPath: keyPath] }
        guard !values.isEmpty else { return nil }
        return average(values, strategy: strategy)
    }

    func get(_ keyPath: KeyPath<RecordType, UInt8?>, strategy: AverageStrategy = .mean) -> UInt8? {
        let values = records.compactMap { $0[keyPath: keyPath] }
        guard !values.isEmpty else { return nil }
        return average(values, strategy: strategy)
    }

    func get(_ keyPath: KeyPath<RecordType, UInt16?>, strategy: AverageStrategy = .mean) -> UInt16? {
        let values = records.compactMap { $0[keyPath: keyPath] }
        guard !values.isEmpty else { return nil }
        return average(values, strategy: strategy)
    }

    func get(_ keyPath: KeyPath<RecordType, Float?>, strategy: AverageStrategy = .mean) -> Float? {
        let values = records.compactMap { $0[keyPath: keyPath] }
        guard !values.isEmpty else { return nil }

        return average(values, strategy: strategy)
    }

    func get(_ keyPath: KeyPath<RecordType, Float>, strategy: AverageStrategy = .mean) -> Float? {
        let values = records.compactMap { $0[keyPath: keyPath] }
        guard !values.isEmpty else { return nil }

        return average(values, strategy: strategy)
    }

    func get(_ keyPath: KeyPath<RecordType, String?>) -> String? {
        let values = records.compactMap { $0[keyPath: keyPath] }
        guard !values.isEmpty else { return nil }

        return values.mode
    }

    func get(_ keyPath: KeyPath<RecordType, [Float?]>) -> [Float?] {
        return (0...5).map { index in
            let values = records.compactMap { $0[keyPath: keyPath][index] }
            guard !values.isEmpty else { return nil }

            return values.reduce(0, +) / Float(values.count)
        }
    }

    func average<T>(_ values: [T], strategy: AverageStrategy) -> T? where T: BinaryInteger {
        switch strategy {
            case .mean:
                return T((values.reduce(0) { $0 + Float($1) } / Float(values.count)).rounded())
            case .mode:
                return values.mode?.values.first
            case .bearing:
                let vectors = values.map { Vector2(cos(deg2rad(Float($0))), sin(deg2rad(Float($0)))) }
                let meanVector = vectors.reduce(Vector2.zero) { $0 + $1 } / vectors.count
                let meanAngle = rad2deg(north.angle(with: meanVector)).rounded()
                return T(meanAngle < 0 ? 360 + meanAngle : meanAngle)
        }
    }

    func average(_ values: [Float], strategy: AverageStrategy) -> Float? {
        switch strategy {
            case .mean:
                return values.reduce(0, +) / Float(values.count)
            case .mode:
                return values.mode?.values.first
            case .bearing:
                let vectors = values.map { Vector2(cos(deg2rad($0)), sin(deg2rad($0))) }
                let meanVector = vectors.reduce(.zero, +) / vectors.count
                let meanAngle = rad2deg(north.angle(with: meanVector))
                return meanAngle < 0 ? 360 + meanAngle : meanAngle
        }
    }

    enum AverageStrategy {
        case mean
        case mode
        case bearing
    }
}

private let north = Vector2(1, 0)

private func deg2rad(_ deg: Float) -> Float {
    deg * .pi / 180
}

private func rad2deg(_ rad: Float) -> Float {
    rad * 180 / .pi
}

private extension Vector2 {
    static func / <T: BinaryInteger>(lhs: Vector2, rhs: T) -> Vector2 {
        return .init(lhs.x / Float(rhs), lhs.y / Float(rhs))
    }

    static func / (lhs: Vector2, rhs: Float) -> Vector2 {
        return .init(lhs.x / rhs, lhs.y / rhs)
    }
}

extension Array where Element: Hashable {
    var mode: Element? {
        let counts = reduce(into: [:]) { $0[$1, default: 0] += 1 }

        guard let (value, _) = counts.max(by: { $0.1 < $1.1 }) else { return nil }
        return value
    }
}
