import Foundation

let zulu = TimeZone(secondsFromGMT: 0)!

extension Sequence where Element: Hashable {
    var frequency: [Element: Int] { reduce(into: [:]) { $0[$1, default: 0] += 1 } }
    
    var mode: (values: [Element], count: Int)? {
        guard let maxCount = frequency.values.max() else { return nil }
        return (frequency.compactMap { $0.value == maxCount ? $0.key : nil }, maxCount)
    }
}

extension AsyncSequence {
    func collect() async rethrows -> [Element] {
        try await reduce(into: [Element]()) { $0.append($1) }
    }
}

extension URL {
    var isDirectory: Bool {
        (try? resourceValues(forKeys: [.isDirectoryKey]))?.isDirectory == true
    }
}

func zipOptionals<each T>(_ values: repeat (each T)?) -> (repeat each T)? {
    for case nil in repeat (each values) {
        return nil
    }
    return (repeat (each values)!)
}
