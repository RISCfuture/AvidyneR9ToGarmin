import Foundation
import Logging
import CodableCSV

let powerOnSentinel = "<<<< **** POWER ON **** >>>>"
let incrementalExtractSentinel = "<< Incremental extract >>"

enum R9LogEntry {
    case record(_ record: R9Record)
    case powerOn(date: Date)
    case incrementalExtract(date: Date)
}

class R9FileParser {
    let url: URL
    let type: R9RecordType
    var logger: Logger? = nil
    
    private var path: String { url.path }
    
    private static var engineFields: Dictionary<String, Array<String>> = [
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
            do {
                while let row = try reader.readRecord() {
                    do {
                        let record = try parseRow(row, headers: reader.headers)
                        continuation.yield(record)
                    } catch {
                        logger?.info("Couldn’t parse row",
                                      metadata: metadataForError(error, rowIndex: reader.rowIndex - 1))
                    }
                }
            } catch {
                logger?.error("Couldn’t parse rows",
                              metadata: metadataForError(error))
            }
            continuation.finish()
        }
    }
    
    private func CSVString(url: URL) throws -> String {
        let csv = try String(contentsOf: url, encoding: .windowsCP1250)
        let columns = csv.split(separator: "\r\n").first!.split(separator: ",", omittingEmptySubsequences: false).count
        return csv
            .replacingOccurrences(of: powerOnSentinel, with: powerOnSentinel.appending(String(repeating: ",", count: columns - 4)))
            .replacingOccurrences(of: incrementalExtractSentinel, with: "")
    }
    
    private func parseRow(_ row: CSVReader.Record, headers: Array<String>) throws -> R9LogEntry {
        let record: R9LogEntry
        switch type {
            case .engine:
                try determineEngineFields(header: headers)
                record = try parseEngineRecord(row: row)
            case .flight: record = try parseFlightRecord(row: row)
            case .system: record = try parseSystemRecord(row: row)
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
    
    private func parseEngineRecord(row: CSVReader.Record) throws -> R9LogEntry {
        let parser = R9RecordParser(row: row, record: R9EngineRecord())
        
        try parser.parse(field: "Systime", into: \.systime)
        try parser.parse(dateField: "Date", timeField: "Time", into: \.date)
        
        if parser.isPowerOn { return .powerOn(date: parser.record.date!) }
        if parser.isIncrementalExtract { return .incrementalExtract(date: parser.record.date!) }
        
        try parser.parse(field: engineFields["oilTemperature"]!, into: \.oilTemperature)
        try parser.parse(field: engineFields["oilPressure"]!, into: \.oilPressure)
        try parser.parse(field: engineFields["RPM"]!, into: \.RPM)
        try parser.parse(field: engineFields["manifoldPressure"]!, into: \.manifoldPressure)
//        try parser.parse(field: engineFields["TIT"]!, into: \.TIT)
        try parser.parse(field: engineFields["CHTs"]!, into: \.CHTs, count: 6)
        try parser.parse(field: engineFields["EGTs"]!, into: \.EGTs, count: 6)
        try parser.parse(field: engineFields["percentPower"]!, into: \.percentPower)
        try parser.parse(field: engineFields["fuelFlow"]!, into: \.fuelFlow)
        try parser.parse(field: engineFields["fuelUsed"]!, into: \.fuelUsed)
        try parser.parse(field: engineFields["fuelRemaining"]!, into: \.fuelRemaining)
        try parser.parse(field: engineFields["fuelTimeRemaining"]!, into: \.fuelTimeRemaining)
        try parser.parse(field: engineFields["fuelEconomy"]!, into: \.fuelEconomy)
//        try parser.parse(field: engineFields["_alt1Current"]!, into: \._alt1Current)
//        try parser.parse(field: engineFields["_alt2Current"]!, into: \._alt2Current)
//        try parser.parse(field: engineFields["_bat1Current"]!, into: \._bat1Current)
//        try parser.parse(field: engineFields["_bat2Current"]!, into: \._bat2Current)
        try parser.parse(field: engineFields["mainBus1Potential"]!, into: \.mainBus1Potential)
//        try parser.parse(field: engineFields["_mainBus2Potential"]!, into: \._mainBus2Potential)
        try parser.parse(field: engineFields["emergencyBusPotential"]!, into: \.emergencyBusPotential)
//        try parser.parse(field: engineFields["_fuelQuantityLeft"]!, into: \._fuelQuantityLeft)
//        try parser.parse(field: engineFields["_fuelQuantityRight"]!, into: \._fuelQuantityRight)
//        try parser.parse(field: engineFields["_deiceVacuum"]!, into: \._deiceVacuum)
//        try parser.parse(field: engineFields["_rudderTrim"]!, into: \._rudderTrim)
//        try parser.parse(field: engineFields["_flapSetting"]!, into: \._flapSetting)
//        try parser.parse(field: engineFields["_Ng"]!, into: \._Ng)
//        try parser.parse(field: engineFields["_torque"]!, into: \._torque)
//        try parser.parse(field: engineFields["_ITT"]!, into: \._ITT)
//        try parser.parse(field: engineFields["_Np"]!, into: \._Np)
//        try parser.parse(field: engineFields["_discreteInputs"]!, into: \._discreteInputs)
//        try parser.parse(field: engineFields["_discreteOutputs"]!, into: \._discreteOutputs)
        
        return .record(parser.record)
    }
    
    private func parseFlightRecord(row: CSVReader.Record) throws -> R9LogEntry {
        let parser = R9RecordParser(row: row, record: R9FlightRecord())
        
        try parser.parse(field: "Systime", into: \.systime)
        try parser.parse(dateField: "Date", timeField: "Time", into: \.date)
        
        if parser.isPowerOn { return .powerOn(date: parser.record.date!) }
        if parser.isIncrementalExtract { return .incrementalExtract(date: parser.record.date!) }
        
        try parser.parse(field: "Systime", into: \.systime)
        try parser.parse(dateField: "Date", timeField: "Time", into: \.date)
        try parser.parse(field: "Filtered NormAcc (G)", into: \.filteredNormalAcceleration)
        try parser.parse(field: "NormAcc (G)", into: \.normalAcceleration)
        try parser.parse(field: "LongAcc (G)", into: \.longitudinalAcceleration)
        try parser.parse(field: "LateralAcc (G)", into: \.lateralAcceleration)
        try parser.parse(field: "ADAHRSUsed", into: \.activeADAHRS)
        try parser.parse(field: "AHRSStatusbits", into: \.AHRSStatus, radix: 16, prefix: "0x")
        try parser.parse(field: "Heading (°M)", into: \.heading)
        try parser.parse(field: "Pitch (°)", into: \.pitch)
        try parser.parse(field: "Roll (°)", into: \.roll)
        try parser.parse(field: "FlightDirectorPitch (°)", into: \.FDPitch)
        try parser.parse(field: "FlightDirectorRoll (°)", into: \.FDRoll)
        try parser.parse(field: "HeadingRate (°/sec)", into: \.headingRate)
        try parser.parse(field: "PressureAltitude (ft)", into: \.pressureAltitude)
        try parser.parse(field: "IndicatedAirspeed (kts)", into: \.indicatedAirspeed)
        try parser.parse(field: "TrueAirspeed (kts)", into: \.trueAirspeed)
        try parser.parse(field: "VerticalSpeed (ft/min)", into: \.verticalSpeed)
        try parser.parse(field: "GPSLatitude", into: \.GPSLatitude, zeroIsNull: true)
        try parser.parse(field: "GPSLongitude", into: \.GPSLongitude, zeroIsNull: true)
        try parser.parse(field: "BodyYawRate (°/sec)", into: \.bodyYawRate)
        try parser.parse(field: "BodyPitchRate (°/sec)", into: \.bodyPitchRate)
        try parser.parse(field: "BodyRollRate (°/sec)", into: \.bodyRollRate)
        try parser.parse(field: "MagStatus", into: \.magnetometerStatus, radix: 16, prefix: "0x")
        try parser.parse(field: "IRUStatus", into: \.IRUStatus, radix: 16, prefix: "0x")
        try parser.parse(field: "MPUStatus", into: \.MPUStatus, radix: 16, prefix: "0x")
        try parser.parse(field: "ADCStatus", into: \.ADCStatus)
        try parser.parse(field: "AHRSSeq", into: \.AHRSSequence)
        try parser.parse(field: "ADCSeq", into: \.ADCSequence)
        try parser.parse(field: "AHRSStartupMode", into: \.AHRStartupMode)
        try parser.parse(field: "DFC100 Lat Active", into: \.DFC100_activeLateralMode)
        try parser.parse(field: "DFC100 Lat Armed", into: \.DFC100_armedLateralMode)
        try parser.parse(field: "DFC100 Vert Active", into: \.DFC100_activeVerticalMode)
        try parser.parse(field: "DFC100 Vert Armed", into: \.DFC100_armedVerticalMode)
        try parser.parse(field: "DFC100 Status Flags", into: \.DFC100_statusFlags, radix: 16, prefix: "0x")
        try parser.parse(field: "DFC100 Fail Flags", into: \.DFC100_failFlags, radix: 16, prefix: "0x")
        try parser.parse(field: "DFC100 Alt Target", into: \.DFC100_altitudeTarget)
        
        return .record(parser.record)
    }
    
    private func parseSystemRecord(row: CSVReader.Record) throws -> R9LogEntry {
        let parser = R9RecordParser(row: row, record: R9SystemRecord())
        
        try parser.parse(field: "Systime", into: \.systime)
        try parser.parse(dateField: "Date", timeField: "Time", into: \.date)
        
        if parser.isPowerOn { return .powerOn(date: parser.record.date!) }
        if parser.isIncrementalExtract { return .incrementalExtract(date: parser.record.date!) }
        
        try parser.parse(field: "Systime", into: \.systime)
        try parser.parse(dateField: "Date", timeField: "Time", into: \.date)
        try parser.parse(field: "OutsideAirTemperature (°C)", into: \.oat)
        try parser.parse(field: "LocalizerDeviation (-1..1)", into: \.localizerDeviation)
        try parser.parse(field: "GlideslopeDeviation (-1..1)", into: \.glideslopeDeviation)
        try parser.parse(field: "FlightDirectorOn_Off", into: \.flightDirectorOnOff)
        try parser.parse(field: "AutopilotMode", into: \.autopilotMode)
        try parser.parse(field: "GroundSpeed (kts)", into: \.groundSpeed)
        try parser.parse(field: "GroundTrack (°M)", into: \.groundTrack)
        try parser.parse(field: "CrossTrackDeviation (nm)", into: \.crossTrackDeviation)
        try parser.parse(field: "VerticalDeviation (ft)", into: \.verticalDeviation)
        try parser.parse(field: "AltimeterSetting (in.hg)", into: \.altimeterSetting)
        try parser.parse(field: "AltBug (ft)", into: \.altitudeBug)
        try parser.parse(field: "VSIBug (ft/min)", into: \.verticalSpeedBug)
        try parser.parse(field: "HdgBug (°)", into: \.headingBug)
        try parser.parse(field: "DisplayMode", into: \.displayMode)
        try parser.parse(field: "NavigationMode", into: \.navigationMode)
        try parser.parse(field: "ActiveWptId", into: \.activeWaypoint)
        try parser.parse(field: "GPSSelect", into: \.activeGPS)
        try parser.parse(field: "NavaidBrg (°M)", into: \.navaidBearing)
        try parser.parse(field: "OBS (°M)", into: \.OBS)
        try parser.parse(field: "DesiredTrack (°M)", into: \.desiredTrack)
        try parser.parse(field: "NavFreq (kHz)", into: \.navFrequency)
        try parser.parse(field: "CrsSelect", into: \.courseSelect)
        try parser.parse(field: "NavType", into: \.navType)
        try parser.parse(field: "CourseDeviation (°)", into: \.courseDeviation)
        try parser.parse(field: "GPSAltitude (m)", into: \.GPSAltitude)
        try parser.parse(field: "DistanceToActiveWpt (nm)", into: \.distanceToWaypoint)
        try parser.parse(field: "GPSState", into: \.GPSState)
        try parser.parse(field: "GPSHorizProtLimit (m)", into: \.GPSHorizontalProterctionLimit)
        try parser.parse(field: "GPSVertProtLimit (m)", into: \.GPSVerticalProterctionLimit)
        try parser.parse(field: "HPL_SBAS (m)", into: \.SBAS_HPL)
        try parser.parse(field: "VPL_SBAS (m)", into: \.SBAS_VPL)
        try parser.parse(field: "HFOM (m)", into: \.HFOM)
        try parser.parse(field: "VFOM (m)", into: \.VFOM)
        try parser.parse(field: "FmsCourse (°M)", into: \.FMSCourse)
        try parser.parse(field: "MagVar (° -W/+E)", into: \.magneticVariation, zeroIsNull: true)
        try parser.parse(field: "GPS MSL Altitude (m)", into: \.GPSAltitudeMSL)
        try parser.parse(field: "GPS AGL Height (m)", into: \.GPSHeightAGL)
        try parser.parse(field: "FLTA RTC (m)", into: \.FLTA_RTC)
        try parser.parse(field: "FLTA ATC (m)", into: \.FLTA_ATC)
        try parser.parse(field: "FLTA vspd (fpm)", into: \.FLTA_VerticalSpeed)
        try parser.parse(field: "FLTA RTC dist (m)", into: \.FLTA_RTCDistance)
        try parser.parse(field: "FLTA terr dist (m)", into: \.FLTA_TerrainDistance)
        try parser.parse(field: "FLTA Status", into: \.FLTA_Status, radix: 16, prefix: "0x")

        return .record(parser.record)
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
