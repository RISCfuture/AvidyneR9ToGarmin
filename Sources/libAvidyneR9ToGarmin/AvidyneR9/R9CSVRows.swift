import Foundation
import StreamingCSV

// MARK: - Enums for known status/mode values

public enum AutopilotMode: String, CSVCodable, Sendable {
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

public enum GPSFixStatus: String, CSVCodable, Sendable {
    case noSolution = "NoSoln"
    case fix3D = "3D"
    case fix3DMinus = "3D-"
    case fix3DDiff = "3DDiff"
}

public enum DFC100LateralMode: UInt8, CSVCodable, Sendable {
    case off = 0
    case roll = 1
    case heading = 2
    case nav = 3
    case approach = 4
    case backcourse = 5
    case goAround = 6
}

public enum DFC100VerticalMode: UInt8, CSVCodable, Sendable {
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

public struct R9EngineRow: Sendable {
    public var Systime: UInt?
    public var Date: String?
    public var Time: String?

    // These will be mapped to the actual CSV column names via the decoder
    // We'll handle the mapping when creating the decoder from headers
    public var oilTemperature: Int?
    public var oilPressure: UInt?
    public var RPM: UInt?
    public var manifoldPressure: Float?
    public var TIT: Float?
    public var CHT1: Float?
    public var CHT2: Float?
    public var CHT3: Float?
    public var CHT4: Float?
    public var CHT5: Float?
    public var CHT6: Float?
    public var EGT1: Float?
    public var EGT2: Float?
    public var EGT3: Float?
    public var EGT4: Float?
    public var EGT5: Float?
    public var EGT6: Float?
    public var percentPower: Float?
    public var fuelFlow: Float?
    public var fuelUsed: Float?
    public var fuelRemaining: Float?
    public var fuelTimeRemaining: Int?
    public var fuelEconomy: Float?
    public var alt1Current: Float?
    public var alt2Current: Float?
    public var batCurrent: Float?
    public var bat2Current: Float?
    public var mainBus1Potential: Float?
    public var mainBus2Potential: Float?
    public var emergencyBusPotential: Float?
}

public struct R9EngineLegacyRow: Sendable {
    public var Systime: UInt?
    public var Date: String?
    public var Time: String?

    // These match the property names used in the legacy format
    public var oilTemperature: Int?
    public var oilPressure: UInt?
    public var RPM: UInt?
    public var manifoldPressure: Float?
    public var TIT: Float?
    public var CHT1: Float?
    public var CHT2: Float?
    public var CHT3: Float?
    public var CHT4: Float?
    public var CHT5: Float?
    public var CHT6: Float?
    public var EGT1: Float?
    public var EGT2: Float?
    public var EGT3: Float?
    public var EGT4: Float?
    public var EGT5: Float?
    public var EGT6: Float?
    public var percentPower: Float?
    public var fuelFlow: Float?
    public var fuelUsed: Float?
    public var fuelRemaining: Float?
    public var fuelTimeRemaining: Int?
    public var fuelEconomy: Float?
    public var alt1Current: Float?
    public var alt2Current: Float?
    public var batCurrent: Float?
    public var bat2Current: Float?
    public var mainBus1Potential: Float?
    public var mainBus2Potential: Float?
    public var emergencyBusPotential: Float?
}

extension R9EngineRow {
    public func isPowerOn() -> Bool {
        return false // Will be handled at parse time
    }

    public func isIncrementalExtract() -> Bool {
        return false // Will be handled at parse time
    }
}

extension R9EngineLegacyRow {
    public func isPowerOn() -> Bool {
        return false // Will be handled at parse time
    }

    public func isIncrementalExtract() -> Bool {
        return false // Will be handled at parse time
    }
}

public struct R9FlightRow: Sendable {
    public var Systime: UInt?
    public var Date: String?
    public var Time: String?
    public var filteredNormalAcceleration: Float?
    public var normalAcceleration: Float?
    public var longitudinalAcceleration: Float?
    public var lateralAcceleration: Float?
    public var activeADAHRS: UInt8?
    public var AHRSStatus: UInt8?
    public var heading: Float?
    public var pitch: Float?
    public var roll: Float?
    public var FDPitch: Float?
    public var FDRoll: Float?
    public var headingRate: Float?
    public var pressureAltitude: Int?
    public var indicatedAirspeed: UInt?
    public var trueAirspeed: UInt?
    public var verticalSpeed: Int?
    public var GPSLatitude: Float?
    public var GPSLongitude: Float?
    public var bodyYawRate: Float?
    public var bodyPitchRate: Float?
    public var bodyRollRate: Float?
    public var magnetometerStatus: UInt8?
    public var IRUStatus: UInt8?
    public var MPUStatus: UInt8?
    public var ADCStatus: String?
    public var AHRSSequence: UInt?
    public var ADCSequence: UInt?
    public var AHRStartupMode: UInt8?
    public var DFC100_activeLateralMode: DFC100LateralMode?
    public var DFC100_armedLateralMode: DFC100LateralMode?
    public var DFC100_activeVerticalMode: DFC100VerticalMode?
    public var DFC100_armedVerticalMode: DFC100VerticalMode?
    public var DFC100_statusFlags: UInt?
    public var DFC100_failFlags: UInt?
    public var DFC100_altitudeTarget: Int?
}

extension R9FlightRow {
    public func isPowerOn() -> Bool {
        // POWER ON markers appear as special string values in numeric fields
        return false // Will be handled at parse time
    }

    public func isIncrementalExtract() -> Bool {
        return false // Will be handled at parse time
    }
}

public struct R9SystemRow: Sendable {
    public var Systime: UInt?
    public var Date: String?
    public var Time: String?
    public var oat: Int?
    public var localizerDeviation: Float?
    public var glideslopeDeviation: Float?
    public var flightDirectorOnOff: Bool?
    public var autopilotMode: AutopilotMode?
    public var groundSpeed: UInt?
    public var groundTrack: Int?
    public var crossTrackDeviation: Float?
    public var verticalDeviation: Int?
    public var altimeterSetting: Float?
    public var altitudeBug: Int?
    public var verticalSpeedBug: Int?
    public var headingBug: UInt16?
    public var displayMode: UInt8?
    public var navigationMode: UInt8?
    public var activeWaypoint: String?
    public var activeGPS: UInt8?
    public var navaidBearing: UInt16?
    public var OBS: UInt16?
    public var desiredTrack: UInt16?
    public var navFrequency: UInt?
    public var courseSelect: UInt8?
    public var navType: UInt8?
    public var courseDeviation: Int?
    public var GPSAltitude: Int?
    public var distanceToWaypoint: Float?
    public var GPSState: UInt8?
    public var GPSHorizontalProterctionLimit: Float?
    public var GPSVerticalProterctionLimit: Float?
    public var SBAS_HPL: Float?
    public var SBAS_VPL: Float?
    public var HFOM: Float?
    public var VFOM: Float?
    public var FMSCourse: UInt16?
    public var magneticVariation: Float?
    public var GPSAltitudeMSL: Int?
    public var GPSHeightAGL: Int?
    public var FLTA_RTC: Int?
    public var FLTA_ATC: Int?
    public var FLTA_VerticalSpeed: Int?
    public var FLTA_RTCDistance: Int?
    public var FLTA_TerrainDistance: Int?
    public var FLTA_Status: UInt8?
}

extension R9SystemRow {
    public func isPowerOn() -> Bool {
        // POWER ON markers appear as special string values in numeric fields
        return false // Will be handled at parse time
    }

    public func isIncrementalExtract() -> Bool {
        return false // Will be handled at parse time
    }
}

internal let powerOnSentinel = "<<<< **** POWER ON **** >>>>"
internal let incrementalExtractSentinel = "<< Incremental extract >>"
