import ArgumentParser
import Foundation
import libAvidyneR9ToGarmin
import Logging

@main
struct AvidyneR9ToGarmin: AsyncParsableCommand {

    static let configuration = CommandConfiguration(
        abstract: "Converts Avidyne R9 log files to Garmin CSV format, for use with websites that expect logs in Garmin format.",
        discussion: """
            The executable will create a Garmin log file for each power-on cycle of the R9
            system. Empty log files will automatically be deleted; however, a "long tail" of
            trivially small log files will still be present. You can, at your discretion,
            delete these irrelevant smaller log files.
            """
    )

    @Argument(help: "The directory where Avidyne R9 CSVs are stored.",
              completion: .directory,
              transform: { .init(filePath: $0, directoryHint: .isDirectory) })
    var input: URL

    @Argument(help: "The directory to contain the generated Garmin CSV files.",
              completion: .directory,
              transform: { .init(filePath: $0, directoryHint: .isDirectory) })
    var output: URL

    @Flag(help: "Include extra information in the output.")
    var verbose = false

    @Flag(help: "Show memory usage statistics after conversion.")
    var showStats = false

    mutating func run() async throws {
        var logger = Logger(label: "codes.tim.R9ToGarminConverter")
        logger.logLevel = verbose ? .info : .warning

        let converter = R9ToGarminConverter()
        await converter.setLogger(logger)

        // Set up enhanced progress tracking
        let progressActor = ProgressActor(verbose: verbose)

        // The ProgressActor creates its own ProgressManager internally
        // We need to get a reference to it and pass it to the converter
        let progressManager = await progressActor.getProgressManager()
        await converter.setProgressManager(progressManager)

        do {
            let startTime = Date()
            try await converter.convert(from: input, to: output)
            let endTime = Date()

            if !verbose {
                await progressActor.finish()
            }

            if showStats || verbose {
                let stats = await converter.getMemoryStats()
                let totalTime = endTime.timeIntervalSince(startTime)

                print("\n=== Performance Statistics ===")
                print("Total Processing Time: \(String(format: "%.2f", totalTime)) seconds")
                print("Phase 1 Memory Usage: \(String(format: "%.2f", stats.phase1MemoryMB)) MB")
                print("Phase 2 Peak Memory: \(String(format: "%.2f", stats.phase2PeakMemoryMB)) MB")
                print("==============================\n")
            }
        } catch {
            logger.critical("\(error.localizedDescription)")
            throw ExitCode.failure
        }
    }
}
