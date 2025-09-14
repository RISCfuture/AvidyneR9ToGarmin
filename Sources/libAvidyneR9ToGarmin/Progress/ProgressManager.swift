import Foundation

/// Manages progress tracking for the conversion process
public actor ProgressManager {
    private var state = ProgressState()
    private var progressCallback: (@Sendable (ProgressState) async -> Void)?

    /// Creates a new ProgressManager with an optional progress callback
    /// - Parameter progressCallback: Optional callback to receive progress updates
    public init(progressCallback: (@Sendable (ProgressState) async -> Void)? = nil) {
        self.progressCallback = progressCallback
    }

    /// Sets a callback to receive progress state updates
    /// - Parameter callback: The callback to be invoked when progress changes
    public func setCallback(_ callback: @escaping @Sendable (ProgressState) async -> Void) {
        self.progressCallback = callback
    }

    func startPhase1(totalFiles: Int) async {
        state.phase = .scanningPhase1
        state.totalFiles = totalFiles
        state.filesProcessed = 0
        await notifyUpdate()
    }

    func updatePhase1Progress(filesScanned: Int, currentFile _: String? = nil) async {
        state.filesProcessed = filesScanned
        await notifyUpdate()
    }

    func completePhase1(flightsFound _: Int, powerOnEvents _: Int) async {
        // Phase 1 is complete
        await notifyUpdate()
    }

    func startPhase2(totalFiles: Int) async {
        state.phase = .processingPhase2
        state.totalFiles = totalFiles
        state.filesProcessed = 0
        await notifyUpdate()
    }

    func updatePhase2FileProgress(fileIndex: Int, fileName _: String) async {
        state.filesProcessed = fileIndex + 1
        await notifyUpdate()
    }

    func startCombiningRecords(totalRecords: Int) async {
        state.phase = .combiningRecords
        state.totalRecords = totalRecords
        state.recordsProcessed = 0
        await notifyUpdate()
    }

    func startWritingFiles(totalBoundaries: Int) async {
        state.phase = .writingFiles
        state.totalFiles = totalBoundaries
        state.filesProcessed = 0
        await notifyUpdate()
    }

    func updateRecordProgress(processed: Int, written _: Int) async {
        state.recordsProcessed = processed
        await notifyUpdate()
    }

    func updateWritingProgress(filesWritten: Int) async {
        state.filesProcessed = filesWritten
        await notifyUpdate()
    }

    func updateMemoryUsage(_: Double) {
        // Not needed for simple progress
    }

    func addError(_: String) {
        // Not needed for simple progress
    }

    /// Marks the conversion process as complete
    public func completeConversion() async {
        state.phase = .complete
        await notifyUpdate()
    }

    func getCurrentState() -> ProgressState {
        return state
    }

    private func notifyUpdate() async {
        if let callback = progressCallback {
            await callback(state)
        }
    }

    /// Represents the current state of the conversion progress
    public struct ProgressState: Sendable {
        /// The current phase of the conversion process
        public var phase: ConversionPhase = .idle
        /// Number of files processed in the current phase
        public var filesProcessed: Int = 0
        /// Total number of files to process in the current phase
        public var totalFiles: Int = 0
        /// Number of records processed during combination
        public var recordsProcessed: Int = 0
        /// Total number of records to process during combination
        public var totalRecords: Int = 0
    }

    /// Phases of the conversion process
    public enum ConversionPhase: String, Sendable {
        case idle = "Idle"
        case scanningPhase1 = "Phase 1: Scanning"
        case processingPhase2 = "Phase 2: Processing"
        case combiningRecords = "Combining Records"
        case writingFiles = "Writing Files"
        case complete = "Complete"
    }
}
