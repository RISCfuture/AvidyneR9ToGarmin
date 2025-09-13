import Foundation

/// Manages progress tracking for the conversion process
public actor ProgressManager {
    public struct ProgressState: Sendable {
        public var phase: ConversionPhase = .idle
        public var filesProcessed: Int = 0
        public var totalFiles: Int = 0
        public var recordsProcessed: Int = 0
        public var totalRecords: Int = 0
    }

    public enum ConversionPhase: String, Sendable {
        case idle = "Idle"
        case scanningPhase1 = "Phase 1: Scanning"
        case processingPhase2 = "Phase 2: Processing"
        case combiningRecords = "Combining Records"
        case writingFiles = "Writing Files"
        case complete = "Complete"
    }

    private var state = ProgressState()
    private var progressCallback: (@Sendable (ProgressState) async -> Void)?

    public init(progressCallback: (@Sendable (ProgressState) async -> Void)? = nil) {
        self.progressCallback = progressCallback
    }

    public func setCallback(_ callback: @escaping @Sendable (ProgressState) async -> Void) {
        self.progressCallback = callback
    }

    public func startPhase1(totalFiles: Int) async {
        state.phase = .scanningPhase1
        state.totalFiles = totalFiles
        state.filesProcessed = 0
        await notifyUpdate()
    }

    public func updatePhase1Progress(filesScanned: Int, currentFile _: String? = nil) async {
        state.filesProcessed = filesScanned
        await notifyUpdate()
    }

    public func completePhase1(flightsFound _: Int, powerOnEvents _: Int) async {
        // Phase 1 is complete
        await notifyUpdate()
    }

    public func startPhase2(totalFiles: Int) async {
        state.phase = .processingPhase2
        state.totalFiles = totalFiles
        state.filesProcessed = 0
        await notifyUpdate()
    }

    public func updatePhase2FileProgress(fileIndex: Int, fileName _: String) async {
        state.filesProcessed = fileIndex + 1
        await notifyUpdate()
    }

    public func startCombiningRecords(totalRecords: Int) async {
        state.phase = .combiningRecords
        state.totalRecords = totalRecords
        state.recordsProcessed = 0
        await notifyUpdate()
    }

    public func startWritingFiles(totalBoundaries: Int) async {
        state.phase = .writingFiles
        state.totalFiles = totalBoundaries
        state.filesProcessed = 0
        await notifyUpdate()
    }

    public func updateRecordProgress(processed: Int, written _: Int) async {
        state.recordsProcessed = processed
        await notifyUpdate()
    }

    public func updateWritingProgress(filesWritten: Int) async {
        state.filesProcessed = filesWritten
        await notifyUpdate()
    }

    public func updateMemoryUsage(_: Double) {
        // Not needed for simple progress
    }

    public func addError(_: String) {
        // Not needed for simple progress
    }

    public func completeConversion() async {
        state.phase = .complete
        await notifyUpdate()
    }

    public func getCurrentState() -> ProgressState {
        return state
    }

    private func notifyUpdate() async {
        if let callback = progressCallback {
            await callback(state)
        }
    }

    // Helper to get memory usage - kept for compatibility but not used
    public static func getCurrentMemoryUsageMB() -> Double {
        var info = mach_task_basic_info()
        var count = mach_msg_type_number_t(MemoryLayout<mach_task_basic_info>.size) / 4

        let result = withUnsafeMutablePointer(to: &info) {
            $0.withMemoryRebound(to: integer_t.self, capacity: 1) {
                task_info(mach_task_self_,
                         task_flavor_t(MACH_TASK_BASIC_INFO),
                         $0,
                         &count)
            }
        }

        if result == KERN_SUCCESS {
            return Double(info.resident_size) / 1024.0 / 1024.0
        }
        return 0.0
    }
}
