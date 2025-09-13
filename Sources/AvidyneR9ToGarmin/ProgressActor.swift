import Foundation
import libAvidyneR9ToGarmin
import Progress

actor ProgressActor {
    private let verbose: Bool
    private var phase1ProgressBar: ProgressBar?
    private var phase2ProgressBar: ProgressBar?
    private var combiningProgressBar: ProgressBar?
    private var writingProgressBar: ProgressBar?
    private var currentPhase: ProgressManager.ConversionPhase = .idle
    private let progressManager: ProgressManager

    init(verbose: Bool) {
        self.verbose = verbose
        self.progressManager = ProgressManager()

        // Set up progress callback
        Task {
            await progressManager.setCallback { [weak self] state in
                await self?.displayProgress(state)
            }
        }
    }

    func getProgressManager() -> ProgressManager {
        return progressManager
    }

    private func displayProgress(_ state: ProgressManager.ProgressState) {
        guard !verbose else { return }

        // Handle phase transitions
        if state.phase != currentPhase {
            switch state.phase {
            case .scanningPhase1:
                print("Phase 1: Scanning for flight boundaries…")
                // Don't create the bar yet - wait until we have totalFiles
            case .processingPhase2:
                // Finish phase 1 bar if needed
                if var bar = phase1ProgressBar {
                    while bar.index < bar.count {
                        bar.next()
                    }
                    phase1ProgressBar = nil
                }
                print("\nPhase 2: Processing files…")
                // Don't create the bar yet - wait until we have totalFiles
            case .combiningRecords:
                // Finish phase 2 bar if needed
                if var bar = phase2ProgressBar {
                    while bar.index < bar.count {
                        bar.next()
                    }
                    phase2ProgressBar = nil
                }
                print("\nPhase 3: Combining records…")
                // Don't create bar yet - wait for totalRecords
            case .writingFiles:
                // Finish combining bar if needed
                if var bar = combiningProgressBar {
                    while bar.index < bar.count {
                        bar.next()
                    }
                    combiningProgressBar = nil
                }
                print("\nPhase 4: Writing output files…")
                // Don't create bar yet - wait for totalFiles
            case .complete:
                // Finish writing bar if needed
                if var bar = writingProgressBar {
                    while bar.index < bar.count {
                        bar.next()
                    }
                    writingProgressBar = nil
                }
                print("\nConversion complete!")
            default:
                break
            }
            currentPhase = state.phase
        }

        // Create or update progress bars
        if state.phase == .scanningPhase1 {
            // Create bar if we don't have one and we have totalFiles
            if phase1ProgressBar == nil && state.totalFiles > 0 {
                phase1ProgressBar = ProgressBar(count: state.totalFiles)
            }
            // Update the bar
            if var bar = phase1ProgressBar {
                while bar.index < min(state.filesProcessed, bar.count) {
                    bar.next()
                }
                phase1ProgressBar = bar
            }
        } else if state.phase == .processingPhase2 {
            // Create bar if we don't have one and we have totalFiles
            if phase2ProgressBar == nil && state.totalFiles > 0 {
                phase2ProgressBar = ProgressBar(count: state.totalFiles)
            }
            // Update the bar
            if var bar = phase2ProgressBar {
                while bar.index < min(state.filesProcessed, bar.count) {
                    bar.next()
                }
                phase2ProgressBar = bar
            }
        } else if state.phase == .combiningRecords {
            // Create bar if we don't have one and we have totalRecords
            if combiningProgressBar == nil && state.totalRecords > 0 {
                combiningProgressBar = ProgressBar(count: state.totalRecords)
            }
            // Update the bar
            if var bar = combiningProgressBar {
                while bar.index < min(state.recordsProcessed, bar.count) {
                    bar.next()
                }
                combiningProgressBar = bar
            }
        } else if state.phase == .writingFiles {
            // Create bar if we don't have one and we have totalFiles
            if writingProgressBar == nil && state.totalFiles > 0 {
                writingProgressBar = ProgressBar(count: state.totalFiles)
            }
            // Update the bar
            if var bar = writingProgressBar {
                while bar.index < min(state.filesProcessed, bar.count) {
                    bar.next()
                }
                writingProgressBar = bar
            }
        }
    }

    func update(progress _: Float, message _: String) {
        // Legacy interface - no longer used
    }

    func finish() {
        Task {
            await progressManager.completeConversion()
        }
    }
}
