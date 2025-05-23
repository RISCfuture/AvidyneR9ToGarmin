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

    mutating func run() async throws {
        var logger = Logger(label: "codes.tim.R9ToGarminConverter")
        logger.logLevel = verbose ? .info : .warning

        let converter = R9ToGarminConverter()
        await converter.setLogger(logger)

        do {
            await converter.parseR9Records(from: input)
            try await converter.writeGarminRecords(to: output)
        } catch {
            logger.critical("\(error.localizedDescription)")
        }
    }
}
