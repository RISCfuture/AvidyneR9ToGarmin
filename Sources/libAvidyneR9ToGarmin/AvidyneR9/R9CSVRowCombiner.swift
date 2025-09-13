import Foundation
@preconcurrency import VectorMath

actor R9CSVRowCombiner<RowType: Sendable> {
    private let rows: [RowType]

    init(rows: [RowType]) {
        self.rows = rows
    }

    func get<T>(_ keyPath: KeyPath<RowType, T?>, strategy: AverageStrategy = .mean) -> T? where T: BinaryInteger {
        let values = rows.compactMap { $0[keyPath: keyPath] }
        guard !values.isEmpty else { return nil }
        return average(values, strategy: strategy)
    }

    func get<T>(_ keyPath: KeyPath<RowType, T?>, strategy: AverageStrategy = .mean) -> T? where T: BinaryFloatingPoint {
        let values = rows.compactMap { $0[keyPath: keyPath] }
        guard !values.isEmpty else { return nil }
        return average(values, strategy: strategy)
    }

    func get(_ keyPath: KeyPath<RowType, String?>) -> String? {
        let values = rows.compactMap { $0[keyPath: keyPath] }
        guard !values.isEmpty else { return nil }
        return values.mode
    }

    func get<E: RawRepresentable & Hashable>(_ keyPath: KeyPath<RowType, E?>) -> E? {
        let values = rows.compactMap { $0[keyPath: keyPath] }
        guard !values.isEmpty else { return nil }
        return values.mode
    }

    func get(_ keyPath: KeyPath<RowType, Bool?>) -> Bool? {
        let values = rows.compactMap { $0[keyPath: keyPath] }
        guard !values.isEmpty else { return nil }
        // For booleans, use mode (most common value)
        return values.mode
    }

    private func average<T>(_ values: [T], strategy: AverageStrategy) -> T where T: BinaryInteger {
        switch strategy {
        case .mean:
            let sum = values.reduce(0.0) { $0 + Double($1) }
            return T((sum / Double(values.count)).rounded())
        case .mode:
            return values.mode ?? values.first!
        case .bearing:
            let floatValues = values.map { Float($0) }
            let vectors = floatValues.map { Vector2(cos(deg2rad($0)), sin(deg2rad($0))) }
            let meanVector = vectors.reduce(Vector2.zero) { $0 + $1 } / vectors.count
            let meanAngle = rad2deg(north.angle(with: meanVector)).rounded()
            return T(meanAngle < 0 ? 360 + meanAngle : meanAngle)
        }
    }

    private func average<T>(_ values: [T], strategy: AverageStrategy) -> T where T: BinaryFloatingPoint {
        switch strategy {
        case .mean:
            return values.reduce(0, +) / T(values.count)
        case .mode:
            // For floating point, we can't easily do mode, so default to mean
            return values.reduce(0, +) / T(values.count)
        case .bearing:
            let floatValues = values.map { Float($0) }
            let vectors = floatValues.map { Vector2(cos(deg2rad($0)), sin(deg2rad($0))) }
            let meanVector = vectors.reduce(.zero, +) / vectors.count
            let meanAngle = rad2deg(north.angle(with: meanVector))
            return T(meanAngle < 0 ? 360 + meanAngle : meanAngle)
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
