import Foundation
import Logging
@preconcurrency import CodableCSV

let powerOnSentinel = "<<<< **** POWER ON **** >>>>"
let incrementalExtractSentinel = "<< Incremental extract >>"

enum R9LogEntry: Sendable {
    case record(_ record: R9Record)
    case powerOn(date: Date)
    case incrementalExtract(date: Date)
}

actor R9FileParser {
    let url: URL
    let type: R9RecordType
    var logger: Logger? = nil

    private var path: String { url.path }

    private static let engineFields: Dictionary<String, Array<String>> = [
        "oilTemperature": ["Eng1 Oil Temperature", "Eng1OilTemperature(°F)"],
        "oilPressure": ["Eng1 Oil Pressure", "Eng1OilPressure"],
        "RPM": ["Eng1 RPM", "Eng1RPM"],
        "manifoldPressure": ["Eng1 Manifold Pressure (In.Hg)", "Eng1ManifoldPressure(In.Hg)"],
        //        "TIT": ["Eng1 TIT (°F)", "Eng1TIT(°F)"],
        "CHTs": ["Eng1 CHT[#] (°F)", "Eng1CHT[#](°F)"],
        "EGTs": ["Eng1 EGT[#] (°F)", "Eng1EGT[#](°F)"],
        "percentPower": ["Eng1 Percent Pwr", "Eng1PercentPwr"],
        "fuelFlow": ["FuelFlow", "FuelFlow(Gal/hr)"],
        "fuelUsed": ["FuelUsed", "FuelUsed(Gal)"],
        "fuelRemaining": ["FuelRemaining", "FuelRemaining(Gal)"],
        "fuelTimeRemaining": ["FuelTimeRemaining (min)", "FuelTimeRemaining(min)"],
        "fuelEconomy": ["FuelEconomy", "FuelEconomy(nm/gal)"],
        //        "_alt1Current": ["Eng1 Alt1 current (A)", "Eng1Alt1current(A)"],
        //        "_alt2Current": ["Eng1 Alt2 current (A)", "Eng1Alt2current(A)"],
        //        "_bat1Current": ["Eng1 Bat current (A)", "Eng1Batcurrent(A)"],
        //        "_bat2Current": ["Eng1 Bat2 current (A)", "Eng1Bat2current(A)"],
        "mainBus1Potential": ["Eng1 MBus1 Volts", "Eng1Bus1Volts"],
        //        "_mainBus2Potential": ["Eng1 MBus2 Volts", "Eng1Bus2Volts"],
        "emergencyBusPotential": ["Eng1 Bus2 Volts", "Eng1Bus3Volts"],
        //        "_fuelQuantityLeft": ["L Fuel Qty"],
        //        "_fuelQuantityRight": ["R Fuel Qty"],
        //        "_deiceVacuum": ["DeiceVac (in-hg)"],
        //        "_rudderTrim": ["Rudder Trim (deg)"],
        //        "_flapSetting": ["Flaps (deg)"],
        //        "_Ng": ["Ng (%)"],
        //        "_torque": ["Torque(ft-lbs)"],
        //        "_ITT": ["ITT (°C)"],
        //        "_Np": ["Np (rpm)"],
        //        "_discreteInputs": ["Eng1 Discrete Inputs", "Eng1DiscreteInputs"],
        //        "_discreteOutputs": ["Eng1 Discrete Outputs", "Eng1DiscreteOutputs"]
    ]

    private var engineFields: Dictionary<String, String> = [:]

    init?(url: URL, logger: Logger? = nil) throws {
        self.url = url
        self.logger = logger

        if url.lastPathComponent.hasSuffix("_ENGINE.CSV") { type = .engine }
        else if url.lastPathComponent.hasSuffix("_FLIGHT.CSV") { type = .flight }
        else if url.lastPathComponent.hasSuffix("_SYSTEM.CSV") { type = .system }
        else {
            logger?.info("Bad file name; skipping",
                         metadata: ["path": .string(url.path)])
            return nil
        }
    }

    func parse() throws -> AsyncStream<R9LogEntry> {
        let string = try CSVString(url: url)
        let reader = try CSVReader(input: string) {
            $0.headerStrategy = .firstLine
            $0.delimiters.row = .standard
            $0.trimStrategy = .whitespaces
        }

        return AsyncStream { continuation in
            Task {
                do {
                    while let row = try reader.readRecord() {
                        do {
                            let record = try await parseRow(row, headers: reader.headers)
                            continuation.yield(record)
                        } catch {
                            let index = reader.rowIndex - 1
                            logger?.info("Couldn’t parse row",
                                         metadata: metadataForError(error, rowIndex: index))
                        }
                    }
                } catch {
                    logger?.error("Couldn’t parse rows",
                                  metadata: metadataForError(error))
                }
                continuation.finish()
            }
        }
    }

    private func CSVString(url: URL) throws -> String {
        let csv = try String(contentsOf: url, encoding: .windowsCP1250)
        let columns = csv.split(separator: "\r\n").first!.split(separator: ",", omittingEmptySubsequences: false).count
        return csv
            .replacingOccurrences(of: powerOnSentinel, with: powerOnSentinel.appending(String(repeating: ",", count: columns - 4)))
            .replacingOccurrences(of: incrementalExtractSentinel, with: "")
    }

    private func parseRow(_ row: CSVReader.Record, headers: Array<String>) async throws -> R9LogEntry {
        let record: R9LogEntry
        switch type {
            case .engine:
                try determineEngineFields(header: headers)
                record = try await parseEngineRecord(row: row)
            case .flight: record = try await parseFlightRecord(row: row)
            case .system: record = try await parseSystemRecord(row: row)
        }
        return record
    }

    private func metadataForError(_ error: Error, rowIndex: Int? = nil) -> Logger.Metadata {
        var metadata: Logger.Metadata = [
            "error": .string(error.localizedDescription),
            "path": .string(path)
        ]

        if let rowIndex = rowIndex {
            metadata["line"] = .string(String(rowIndex))
        }

        if let error = error as? AvidyneR9ToGarminError {
            switch error {
                case let .invalidValue(field, value):
                    metadata["field"] = .string(field)
                    metadata["value"] = .string(value)
                case let .invalidRow(row, childError):
                    metadata = metadataForError(childError)
                    metadata["row"] = .string(String(row))
                default: break
            }
        }

        return metadata
    }

    private func parseEngineRecord(row: CSVReader.Record) async throws -> R9LogEntry {
        let parser = R9RecordParser(row: row, record: R9EngineRecord())

        try await parser.parse(field: "Systime", into: \.systime)
        try await parser.parse(dateField: "Date", timeField: "Time", into: \.date)

        if await parser.isPowerOn { return await .powerOn(date: parser.record.date!) }
        if await parser.isIncrementalExtract { return await .incrementalExtract(date: parser.record.date!) }

        try await parser.parse(field: engineFields["oilTemperature"]!, into: \.oilTemperature)
        try await parser.parse(field: engineFields["oilPressure"]!, into: \.oilPressure)
        try await parser.parse(field: engineFields["RPM"]!, into: \.RPM)
        try await parser.parse(field: engineFields["manifoldPressure"]!, into: \.manifoldPressure)
        //        try await parser.parse(field: engineFields["TIT"]!, into: \.TIT)
        try await parser.parse(field: engineFields["CHTs"]!, into: \.CHTs, count: 6)
        try await parser.parse(field: engineFields["EGTs"]!, into: \.EGTs, count: 6)
        try await parser.parse(field: engineFields["percentPower"]!, into: \.percentPower)
        try await parser.parse(field: engineFields["fuelFlow"]!, into: \.fuelFlow)
        try await parser.parse(field: engineFields["fuelUsed"]!, into: \.fuelUsed)
        try await parser.parse(field: engineFields["fuelRemaining"]!, into: \.fuelRemaining)
        try await parser.parse(field: engineFields["fuelTimeRemaining"]!, into: \.fuelTimeRemaining)
        try await parser.parse(field: engineFields["fuelEconomy"]!, into: \.fuelEconomy)
        //        try await parser.parse(field: engineFields["_alt1Current"]!, into: \._alt1Current)
        //        try await parser.parse(field: engineFields["_alt2Current"]!, into: \._alt2Current)
        //        try await parser.parse(field: engineFields["_bat1Current"]!, into: \._bat1Current)
        //        try await parser.parse(field: engineFields["_bat2Current"]!, into: \._bat2Current)
        try await parser.parse(field: engineFields["mainBus1Potential"]!, into: \.mainBus1Potential)
        //        try parser.parse(field: engineFields["_mainBus2Potential"]!, into: \._mainBus2Potential)
        try await parser.parse(field: engineFields["emergencyBusPotential"]!, into: \.emergencyBusPotential)
        //        try await parser.parse(field: engineFields["_fuelQuantityLeft"]!, into: \._fuelQuantityLeft)
        //        try await parser.parse(field: engineFields["_fuelQuantityRight"]!, into: \._fuelQuantityRight)
        //        try await parser.parse(field: engineFields["_deiceVacuum"]!, into: \._deiceVacuum)
        //        try await parser.parse(field: engineFields["_rudderTrim"]!, into: \._rudderTrim)
        //        try await parser.parse(field: engineFields["_flapSetting"]!, into: \._flapSetting)
        //        try await parser.parse(field: engineFields["_Ng"]!, into: \._Ng)
        //        try await parser.parse(field: engineFields["_torque"]!, into: \._torque)
        //        try await parser.parse(field: engineFields["_ITT"]!, into: \._ITT)
        //        try await parser.parse(field: engineFields["_Np"]!, into: \._Np)
        //        try await parser.parse(field: engineFields["_discreteInputs"]!, into: \._discreteInputs)
        //        try await parser.parse(field: engineFields["_discreteOutputs"]!, into: \._discreteOutputs)

        return await .record(parser.record)
    }

    private func parseFlightRecord(row: CSVReader.Record) async throws -> R9LogEntry {
        let parser = R9RecordParser(row: row, record: R9FlightRecord())

        try await parser.parse(field: "Systime", into: \.systime)
        try await parser.parse(dateField: "Date", timeField: "Time", into: \.date)

        if await parser.isPowerOn { return await .powerOn(date: parser.record.date!) }
        if await parser.isIncrementalExtract { return await .incrementalExtract(date: parser.record.date!) }

        try await parser.parse(field: "Systime", into: \.systime)
        try await parser.parse(dateField: "Date", timeField: "Time", into: \.date)
        try await parser.parse(field: "Filtered NormAcc (G)", into: \.filteredNormalAcceleration)
        try await parser.parse(field: "NormAcc (G)", into: \.normalAcceleration)
        try await parser.parse(field: "LongAcc (G)", into: \.longitudinalAcceleration)
        try await parser.parse(field: "LateralAcc (G)", into: \.lateralAcceleration)
        try await parser.parse(field: "ADAHRSUsed", into: \.activeADAHRS)
        try await parser.parse(field: "AHRSStatusbits", into: \.AHRSStatus, radix: 16, prefix: "0x")
        try await parser.parse(field: "Heading (°M)", into: \.heading)
        try await parser.parse(field: "Pitch (°)", into: \.pitch)
        try await parser.parse(field: "Roll (°)", into: \.roll)
        try await parser.parse(field: "FlightDirectorPitch (°)", into: \.FDPitch)
        try await parser.parse(field: "FlightDirectorRoll (°)", into: \.FDRoll)
        try await parser.parse(field: "HeadingRate (°/sec)", into: \.headingRate)
        try await parser.parse(field: "PressureAltitude (ft)", into: \.pressureAltitude)
        try await parser.parse(field: "IndicatedAirspeed (kts)", into: \.indicatedAirspeed)
        try await parser.parse(field: "TrueAirspeed (kts)", into: \.trueAirspeed)
        try await parser.parse(field: "VerticalSpeed (ft/min)", into: \.verticalSpeed)
        try await parser.parse(field: "GPSLatitude", into: \.GPSLatitude, zeroIsNull: true)
        try await parser.parse(field: "GPSLongitude", into: \.GPSLongitude, zeroIsNull: true)
        try await parser.parse(field: "BodyYawRate (°/sec)", into: \.bodyYawRate)
        try await parser.parse(field: "BodyPitchRate (°/sec)", into: \.bodyPitchRate)
        try await parser.parse(field: "BodyRollRate (°/sec)", into: \.bodyRollRate)
        try await parser.parse(field: "MagStatus", into: \.magnetometerStatus, radix: 16, prefix: "0x")
        try await parser.parse(field: "IRUStatus", into: \.IRUStatus, radix: 16, prefix: "0x")
        try await parser.parse(field: "MPUStatus", into: \.MPUStatus, radix: 16, prefix: "0x")
        try await parser.parse(field: "ADCStatus", into: \.ADCStatus)
        try await parser.parse(field: "AHRSSeq", into: \.AHRSSequence)
        try await parser.parse(field: "ADCSeq", into: \.ADCSequence)
        try await parser.parse(field: "AHRSStartupMode", into: \.AHRStartupMode)
        try await parser.parse(field: "DFC100 Lat Active", into: \.DFC100_activeLateralMode)
        try await parser.parse(field: "DFC100 Lat Armed", into: \.DFC100_armedLateralMode)
        try await parser.parse(field: "DFC100 Vert Active", into: \.DFC100_activeVerticalMode)
        try await parser.parse(field: "DFC100 Vert Armed", into: \.DFC100_armedVerticalMode)
        try await parser.parse(field: "DFC100 Status Flags", into: \.DFC100_statusFlags, radix: 16, prefix: "0x")
        try await parser.parse(field: "DFC100 Fail Flags", into: \.DFC100_failFlags, radix: 16, prefix: "0x")
        try await parser.parse(field: "DFC100 Alt Target", into: \.DFC100_altitudeTarget)

        return await .record(parser.record)
    }

    private func parseSystemRecord(row: CSVReader.Record) async throws -> R9LogEntry {
        let parser = R9RecordParser(row: row, record: R9SystemRecord())

        try await parser.parse(field: "Systime", into: \.systime)
        try await parser.parse(dateField: "Date", timeField: "Time", into: \.date)

        if await parser.isPowerOn { return await .powerOn(date: parser.record.date!) }
        if await parser.isIncrementalExtract { return await .incrementalExtract(date: parser.record.date!) }

        try await parser.parse(field: "Systime", into: \.systime)
        try await parser.parse(dateField: "Date", timeField: "Time", into: \.date)
        try await parser.parse(field: "OutsideAirTemperature (°C)", into: \.oat)
        try await parser.parse(field: "LocalizerDeviation (-1..1)", into: \.localizerDeviation)
        try await parser.parse(field: "GlideslopeDeviation (-1..1)", into: \.glideslopeDeviation)
        try await parser.parse(field: "FlightDirectorOn_Off", into: \.flightDirectorOnOff)
        try await parser.parse(field: "AutopilotMode", into: \.autopilotMode)
        try await parser.parse(field: "GroundSpeed (kts)", into: \.groundSpeed)
        try await parser.parse(field: "GroundTrack (°M)", into: \.groundTrack)
        try await parser.parse(field: "CrossTrackDeviation (nm)", into: \.crossTrackDeviation)
        try await parser.parse(field: "VerticalDeviation (ft)", into: \.verticalDeviation)
        try await parser.parse(field: "AltimeterSetting (in.hg)", into: \.altimeterSetting)
        try await parser.parse(field: "AltBug (ft)", into: \.altitudeBug)
        try await parser.parse(field: "VSIBug (ft/min)", into: \.verticalSpeedBug)
        try await parser.parse(field: "HdgBug (°)", into: \.headingBug)
        try await parser.parse(field: "DisplayMode", into: \.displayMode)
        try await parser.parse(field: "NavigationMode", into: \.navigationMode)
        try await parser.parse(field: "ActiveWptId", into: \.activeWaypoint)
        try await parser.parse(field: "GPSSelect", into: \.activeGPS)
        try await parser.parse(field: "NavaidBrg (°M)", into: \.navaidBearing)
        try await parser.parse(field: "OBS (°M)", into: \.OBS)
        try await parser.parse(field: "DesiredTrack (°M)", into: \.desiredTrack)
        try await parser.parse(field: "NavFreq (kHz)", into: \.navFrequency)
        try await parser.parse(field: "CrsSelect", into: \.courseSelect)
        try await parser.parse(field: "NavType", into: \.navType)
        try await parser.parse(field: "CourseDeviation (°)", into: \.courseDeviation)
        try await parser.parse(field: "GPSAltitude (m)", into: \.GPSAltitude)
        try await parser.parse(field: "DistanceToActiveWpt (nm)", into: \.distanceToWaypoint)
        try await parser.parse(field: "GPSState", into: \.GPSState)
        try await parser.parse(field: "GPSHorizProtLimit (m)", into: \.GPSHorizontalProterctionLimit)
        try await parser.parse(field: "GPSVertProtLimit (m)", into: \.GPSVerticalProterctionLimit)
        try await parser.parse(field: "HPL_SBAS (m)", into: \.SBAS_HPL)
        try await parser.parse(field: "VPL_SBAS (m)", into: \.SBAS_VPL)
        try await parser.parse(field: "HFOM (m)", into: \.HFOM)
        try await parser.parse(field: "VFOM (m)", into: \.VFOM)
        try await parser.parse(field: "FmsCourse (°M)", into: \.FMSCourse)
        try await parser.parse(field: "MagVar (° -W/+E)", into: \.magneticVariation, zeroIsNull: true)
        try await parser.parse(field: "GPS MSL Altitude (m)", into: \.GPSAltitudeMSL)
        try await parser.parse(field: "GPS AGL Height (m)", into: \.GPSHeightAGL)
        try await parser.parse(field: "FLTA RTC (m)", into: \.FLTA_RTC)
        try await parser.parse(field: "FLTA ATC (m)", into: \.FLTA_ATC)
        try await parser.parse(field: "FLTA vspd (fpm)", into: \.FLTA_VerticalSpeed)
        try await parser.parse(field: "FLTA RTC dist (m)", into: \.FLTA_RTCDistance)
        try await parser.parse(field: "FLTA terr dist (m)", into: \.FLTA_TerrainDistance)
        try await parser.parse(field: "FLTA Status", into: \.FLTA_Status, radix: 16, prefix: "0x")

        return await .record(parser.record)
    }

    private func determineEngineFields(header: Array<String>) throws {
        for (localField, externalFields) in Self.engineFields {
            guard let externalField = externalFields.first(where: { field in
                let indexField = field.replacingOccurrences(of: "#", with: "1")
                return header.contains(indexField)
            }) else {
                throw AvidyneR9ToGarminError.missingHeaderField(externalFields)
            }
            engineFields[localField] = externalField
        }
    }
}
