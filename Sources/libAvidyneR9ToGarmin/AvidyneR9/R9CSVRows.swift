import Foundation
import StreamingCSV

// MARK: - Enums for known status/mode values

enum AutopilotMode: String, CSVCodable, Sendable {
    case off = "OFF"
    case on = "ON"
    case fail = "FAIL"
    case ap = "AP"
    case heading = "HDG"
    case nav = "NAV"
    case approach = "APR"
    case verticalSpeed = "VS"
    case altitude = "ALT"
    case glideslope = "GS"
}

enum GPSFixStatus: String, CSVCodable, Sendable {
    case noSolution = "NoSoln"
    case fix3D = "3D"
    case fix3DMinus = "3D-"
    case fix3DDiff = "3DDiff"
}

enum DFC100LateralMode: UInt8, CSVCodable, Sendable {
    case off = 0
    case roll = 1
    case heading = 2
    case nav = 3
    case approach = 4
    case backcourse = 5
    case goAround = 6
}

enum DFC100VerticalMode: UInt8, CSVCodable, Sendable {
    case off = 0
    case pitch = 1
    case verticalSpeed = 2
    case altitude = 3
    case glideslope = 4
    case goAround = 5
    case vnav = 6
}

// CSVRow structs for direct parsing - StreamingCSV will map based on property names
// The actual CSV headers will be handled by the decoder created from headers

struct R9EngineRow: Sendable {
    var Systime: UInt?
    var Date: String?
    var Time: String?

    // These will be mapped to the actual CSV column names via the decoder
    // We'll handle the mapping when creating the decoder from headers
    var oilTemperature: Int?
    var oilPressure: UInt?
    var RPM: UInt?
    var manifoldPressure: Float?
    var TIT: Float?
    var CHT1: Float?
    var CHT2: Float?
    var CHT3: Float?
    var CHT4: Float?
    var CHT5: Float?
    var CHT6: Float?
    var EGT1: Float?
    var EGT2: Float?
    var EGT3: Float?
    var EGT4: Float?
    var EGT5: Float?
    var EGT6: Float?
    var percentPower: Float?
    var fuelFlow: Float?
    var fuelUsed: Float?
    var fuelRemaining: Float?
    var fuelTimeRemaining: Int?
    var fuelEconomy: Float?
    var alt1Current: Float?
    var alt2Current: Float?
    var batCurrent: Float?
    var bat2Current: Float?
    var mainBus1Potential: Float?
    var mainBus2Potential: Float?
    var emergencyBusPotential: Float?
}

struct R9EngineLegacyRow: Sendable {
    var Systime: UInt?
    var Date: String?
    var Time: String?

    // These match the property names used in the legacy format
    var oilTemperature: Int?
    var oilPressure: UInt?
    var RPM: UInt?
    var manifoldPressure: Float?
    var TIT: Float?
    var CHT1: Float?
    var CHT2: Float?
    var CHT3: Float?
    var CHT4: Float?
    var CHT5: Float?
    var CHT6: Float?
    var EGT1: Float?
    var EGT2: Float?
    var EGT3: Float?
    var EGT4: Float?
    var EGT5: Float?
    var EGT6: Float?
    var percentPower: Float?
    var fuelFlow: Float?
    var fuelUsed: Float?
    var fuelRemaining: Float?
    var fuelTimeRemaining: Int?
    var fuelEconomy: Float?
    var alt1Current: Float?
    var alt2Current: Float?
    var batCurrent: Float?
    var bat2Current: Float?
    var mainBus1Potential: Float?
    var mainBus2Potential: Float?
    var emergencyBusPotential: Float?
}

extension R9EngineRow {
    func isPowerOn() -> Bool {
        return false // Will be handled at parse time
    }

    func isIncrementalExtract() -> Bool {
        return false // Will be handled at parse time
    }
}

extension R9EngineLegacyRow {
    func isPowerOn() -> Bool {
        return false // Will be handled at parse time
    }

    func isIncrementalExtract() -> Bool {
        return false // Will be handled at parse time
    }
}

struct R9FlightRow: Sendable {
    var Systime: UInt?
    var Date: String?
    var Time: String?
    var filteredNormalAcceleration: Float?
    var normalAcceleration: Float?
    var longitudinalAcceleration: Float?
    var lateralAcceleration: Float?
    var activeADAHRS: UInt8?
    var AHRSStatus: UInt8?
    var heading: Float?
    var pitch: Float?
    var roll: Float?
    var FDPitch: Float?
    var FDRoll: Float?
    var headingRate: Float?
    var pressureAltitude: Int?
    var indicatedAirspeed: UInt?
    var trueAirspeed: UInt?
    var verticalSpeed: Int?
    var GPSLatitude: Float?
    var GPSLongitude: Float?
    var bodyYawRate: Float?
    var bodyPitchRate: Float?
    var bodyRollRate: Float?
    var magnetometerStatus: UInt8?
    var IRUStatus: UInt8?
    var MPUStatus: UInt8?
    var ADCStatus: String?
    var AHRSSequence: UInt?
    var ADCSequence: UInt?
    var AHRStartupMode: UInt8?
    var DFC100_activeLateralMode: DFC100LateralMode?
    var DFC100_armedLateralMode: DFC100LateralMode?
    var DFC100_activeVerticalMode: DFC100VerticalMode?
    var DFC100_armedVerticalMode: DFC100VerticalMode?
    var DFC100_statusFlags: UInt?
    var DFC100_failFlags: UInt?
    var DFC100_altitudeTarget: Int?
}

extension R9FlightRow {
    func isPowerOn() -> Bool {
        // POWER ON markers appear as special string values in numeric fields
        return false // Will be handled at parse time
    }

    func isIncrementalExtract() -> Bool {
        return false // Will be handled at parse time
    }
}

struct R9SystemRow: Sendable {
    var Systime: UInt?
    var Date: String?
    var Time: String?
    var oat: Int?
    var localizerDeviation: Float?
    var glideslopeDeviation: Float?
    var flightDirectorOnOff: Bool?
    var autopilotMode: AutopilotMode?
    var groundSpeed: UInt?
    var groundTrack: Int?
    var crossTrackDeviation: Float?
    var verticalDeviation: Int?
    var altimeterSetting: Float?
    var altitudeBug: Int?
    var verticalSpeedBug: Int?
    var headingBug: UInt16?
    var displayMode: UInt8?
    var navigationMode: UInt8?
    var activeWaypoint: String?
    var activeGPS: UInt8?
    var navaidBearing: UInt16?
    var OBS: UInt16?
    var desiredTrack: UInt16?
    var navFrequency: UInt?
    var courseSelect: UInt8?
    var navType: UInt8?
    var courseDeviation: Int?
    var GPSAltitude: Int?
    var distanceToWaypoint: Float?
    var GPSState: UInt8?
    var GPSHorizontalProterctionLimit: Float?
    var GPSVerticalProterctionLimit: Float?
    var SBAS_HPL: Float?
    var SBAS_VPL: Float?
    var HFOM: Float?
    var VFOM: Float?
    var FMSCourse: UInt16?
    var magneticVariation: Float?
    var GPSAltitudeMSL: Int?
    var GPSHeightAGL: Int?
    var FLTA_RTC: Int?
    var FLTA_ATC: Int?
    var FLTA_VerticalSpeed: Int?
    var FLTA_RTCDistance: Int?
    var FLTA_TerrainDistance: Int?
    var FLTA_Status: UInt8?
}

extension R9SystemRow {
    func isPowerOn() -> Bool {
        // POWER ON markers appear as special string values in numeric fields
        return false // Will be handled at parse time
    }

    func isIncrementalExtract() -> Bool {
        return false // Will be handled at parse time
    }
}

let powerOnSentinel = "<<<< **** POWER ON **** >>>>"
let incrementalExtractSentinel = "<< Incremental extract >>"
