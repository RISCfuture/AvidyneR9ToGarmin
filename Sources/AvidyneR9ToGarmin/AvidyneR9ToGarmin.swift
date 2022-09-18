import Foundation
import Logging
import ArgumentParser
import libAvidyneR9ToGarmin

@main
struct AvidyneR9ToGarmin: AsyncParsableCommand {
    @Argument(help: "The directory where Avidyne R9 CSVs are stored.",
              completion: .directory,
              transform: URL.init(fileURLWithPath:))
    var input: URL
    
    @Argument(help: "The directory to contain the generated Garmin CSV files.",
              completion: .directory,
              transform: URL.init(fileURLWithPath:))
    var output: URL
    
    @Flag(help: "Include extra information in the output.")
    var verbose = false
    
    mutating func run() async throws {
        var logger = Logger(label: "codes.tim.R9ToGarminConverter")
        logger.logLevel = verbose ? .info : .warning
        
        let converter = R9ToGarminConverter()
        converter.logger = logger
        
        do {
            await converter.parseR9Records(from: input)
            try await converter.writeGarminRecords(to: output)
        } catch {
            logger.critical("\(error.localizedDescription)")
        }
    }
}
