@preconcurrency import CodableCSV
import Foundation
import Logging

/// Converts Avidyne R9 logfiles to Garmin-esque loggiles.
public actor R9ToGarminConverter {
    private static let headers = """
    #airframe_info,log_version="1.00",log_content_version="1.02",product="GDU 460",aircraft_ident="N171MA",unit_software_part_number="710-00118-000",product="GDU 460",software_version="9.00",system_id="M093570387",unit="PFD1"
    Date (yyyy-mm-dd),Time (hh:mm:ss),UTC Time (hh:mm:ss),UTC Offset (hh:mm),Latitude (deg),Longitude (deg),GPS Altitude (ft),GPS Fix Status,GPS Time of Week (sec),GPS Ground Speed (kt),GPS Ground Track (deg),GPS Velocity E (m/sec),GPS Velocity N (m/sec),GPS Velocity U (m/sec),Magnetic Heading (deg),GPS PDOP,GPS Sats,Pressure Altitude (ft),Baro Altitude (ft),Vertical Speed (ft/min),Indicated Airspeed (kt),True Airspeed (kt),Pitch (deg),Roll (deg),Lateral Acceleration (G),Normal Acceleration (G),Selected Heading (deg),Selected Altitude (ft),Selected Vertical Speed (ft/min),Selected Airspeed (kt),Baro Setting (inch Hg),COM Frequency 1 (MHz),COM Frequency 2 (MHz),NAV Frequency (MHz),Active Nav Source,Nav Annunciation,Nav Identifier,Nav Distance (nm),Nav Bearing (deg),Nav Course (deg),Nav Cross Track Distance (nm),Horizontal CDI Deflection,Horizontal CDI Full Scale (ft),Horizontal CDI Scale,Vertical CDI Deflection,Vertical CDI Full Scale (ft),VNAV CDI Deflection,VNAV Altitude (ft),Autopilot State,FD Lateral Mode,FD Vertical Mode,FD Roll Command (deg),FD Pitch Command (deg),FD Altitude (ft),AP Roll Command (deg),AP Pitch Command (deg),AP VS Command (ft/min),AP Altitude Command (ft),AP Roll Torque (%),AP Pitch Torque (%),AP Roll Trim Motor,AP Pitch Trim Motor,Magnetic Variation (deg),Outside Air Temp (deg C),Density Altitude (ft),Height Above Ground (ft),Wind Speed (kt),Wind Direction (deg),AHRS Status,AHRS Dev (%),Magnetometer Status,Network Status,Transponder Code,Transponder Mode,Oil Temp (deg F),Fuel L Qty (gal),Fuel R Qty (gal),Fuel Press (PSI),Oil Press (PSI),RPM,Manifold Press (inch Hg),Volts,Volts,Amps,Amps,Fuel Flow (gal/hour),Elevator Trim,Aileron Trim,CHT1 (deg F),CHT2 (deg F),CHT3 (deg F),CHT4 (deg F),CHT5 (deg F),CHT6 (deg F),EGT1 (deg F),EGT2 (deg F),EGT3 (deg F),EGT4 (deg F),EGT5 (deg F),EGT6 (deg F),Engine Power (%),CAS Alert,Terrain Alert,Engine 1 Cycle Count
    Lcl Date,Lcl Time,UTC Time,UTCOfst,Latitude,Longitude,AltGPS,GPSfix,,GndSpd,TRK,GPSVelE,GPSVelN,GPSVelU,HDG,PDOP,,AltP,AltInd,VSpd,IAS,TAS,Pitch,Roll,LatAc,NormAc,SelHDG,SelALT,SelVSpd,SelIAS,Baro,COM1,COM2,NAV1,NavSrc,,NavIdent,NavDist,NavBrg,NavCRS,NavXTK,HCDI,,,VCDI,,VNAV CDI,VNAVAlt,,,,,,,,,,,,,,,MagVar,OAT,AltD,AGL,WndSpd,WndDr,,,,,,,E1 OilT,FQty1,FQty2,E1 FPres,E1 OilP,E1 RPM,E1 MAP,Volts1,Volts2,Amps1,Amps2,E1 FFlow,PTrim,RTrim,E1 CHT1,E1 CHT2,E1 CHT3,E1 CHT4,E1 CHT5,E1 CHT6,E1 EGT1,E1 EGT2,E1 EGT3,E1 EGT4,E1 EGT5,E1 EGT6,E1 %Pwr
    """

    private static var headerRows: [[String]] {
        headers.split(separator: "\n").map { line in line.split(separator: ",", omittingEmptySubsequences: false).map { cell in String(cell) } }
    }

    private static var headerRowCount: Int {
        headerRows.map(\.count).max()!
    }

    private static let garminFilenameFormat = "log_%04d%02d%02d_%02d%02d%02d_%@.csv"
    private static let placeholderAirportID = "____"

    private let inRecords = R9Records()

    /// The logger instance.
    public var logger: Logger?

    /// Creates a new record.
    public init() {}

    /// Sets the logger instance.
    public func setLogger(_ logger: Logger?) { self.logger = logger }

    /// Finds and parses R9 records recursively within the given URL.
    public func parseR9Records(from url: URL) async {
        await inRecords.setLogger(logger)

        await withDiscardingTaskGroup { group in
            guard let fileEnumerator = FileManager.default.enumerator(at: url, includingPropertiesForKeys: [.nameKey, .isDirectoryKey]) else {
                self.logger?.info("Cannot enumerate",
                                  metadata: ["path": .string(url.path)])
                return
            }

            let allURLs = fileEnumerator.compactMap { $0 as? URL }
            for fileURL in allURLs {
                guard let resourceValues = try? fileURL.resourceValues(forKeys: [.nameKey, .isDirectoryKey]),
                      let isDirectory = resourceValues.isDirectory,
                      let name = resourceValues.name
                else { continue }

                guard !isDirectory else { continue }
                guard name.hasSuffix(".CSV") else { continue }

                group.addTask { await self.inRecords.process(url: fileURL) }
            }
        }
    }

    /// Writes Garmin records to the given outut directory.
    public func writeGarminRecords(to url: URL) async throws {
        guard url.isDirectory else { throw AvidyneR9ToGarminError.urlNotDirectory(url) }

        var writer: CSVWriter?
        var rowsRecorded = 0
        var CSVFile: URL?

        var records = [SortedRecord]()
        for try await record in await generateGarminRecords() {
            records.append(record)
        }
        records.sort { $0 < $1 }

        for record in records {
            switch record {
                case let .record(record):
                    if writer == nil { (writer, CSVFile) = try startNewFile(date: record.date, directory: url) }
                    let row = garminRecordToRow(record)
                    try writer!.write(row: row)
                    rowsRecorded += 1
                case let .newFile(date):
                    let oldFile = CSVFile
                    (writer, CSVFile) = try startNewFile(date: date, directory: url)
                    if let oldFile {
                        if rowsRecorded == 0 { try FileManager.default.removeItem(at: oldFile) }
                    }
                    rowsRecorded = 0
            }
        }
    }

    private func startNewFile(date: Date, directory: URL) throws -> (CSVWriter, URL) {
        let calendarDate = Calendar.current.dateComponents(in: zulu, from: date)
        let filename = String(format: Self.garminFilenameFormat,
                              calendarDate.year!, calendarDate.month!, calendarDate.day!,
                              calendarDate.hour!, calendarDate.minute!, calendarDate.second!,
                              Self.placeholderAirportID)

        let url = directory.appendingPathComponent(filename)
        let writer = try CSVWriter(fileURL: url)
        try writeHeaderRows(writer: writer)

        return (writer, url)
    }

    private func writeHeaderRows(writer: CSVWriter) throws {
        for row in Self.headerRows {
            var fixedRow = [String](row)
            fixedRow.reserveCapacity(Self.headerRowCount)
            if row.count < Self.headerRowCount {
                fixedRow.append(contentsOf: Array(repeating: "", count: Self.headerRowCount - row.count))
            }
            try writer.write(row: fixedRow)
        }
    }

    private func generateGarminRecords() async -> any AsyncSequence<SortedRecord, Error> {
        await inRecords.dates().map { date, records, newFile in
            if newFile { return .newFile(date: date) }
            let record = try await self.r9RecordsToGarminRecord(records, date: date)
            return SortedRecord.record(record)
        }
    }

    private enum SortedRecord: Comparable {
        case record(_ record: GarminRecord)
        case newFile(date: Date)

        var date: Date {
            switch self {
                case let .record(record): return record.date
                case let .newFile(date): return date
            }
        }

        static func < (lhs: Self, rhs: Self) -> Bool {
            return lhs.date < rhs.date
        }

        static func == (lhs: Self, rhs: Self) -> Bool {
            return lhs.date == rhs.date
        }
    }
}
