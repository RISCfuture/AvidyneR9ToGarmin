import Foundation
import StreamingCSV

// Custom wrapper types for date and time formatting
public struct DateOnly: CSVCodable, Sendable {
    public let date: Date

    public init(_ date: Date) {
        self.date = date
    }

    public init?(csvString: String) {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd"
        formatter.timeZone = TimeZone(identifier: "UTC")
        guard let date = formatter.date(from: csvString) else { return nil }
        self.date = date
    }

    public var csvString: String {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd"
        formatter.timeZone = TimeZone(identifier: "UTC")
        return formatter.string(from: date)
    }
}

public struct TimeOnly: CSVCodable, Sendable {
    public let date: Date

    public init(_ date: Date) {
        self.date = date
    }

    public init?(csvString: String) {
        let formatter = DateFormatter()
        formatter.dateFormat = "HH:mm:ss"
        formatter.timeZone = TimeZone(identifier: "UTC")
        guard let date = formatter.date(from: csvString) else { return nil }
        self.date = date
    }

    public var csvString: String {
        let formatter = DateFormatter()
        formatter.dateFormat = "HH:mm:ss"
        formatter.timeZone = TimeZone(identifier: "UTC")
        return formatter.string(from: date)
    }
}

// Garmin CSV output format using StreamingCSV encoder
@CSVRowEncoderBuilder
public struct GarminRecord: Sendable {
    // Date and time fields - using wrapper types for proper formatting
    @Field public var dateField: DateOnly { DateOnly(date) }
    @Field public var timeField: TimeOnly { TimeOnly(date) }
    @Field public var localTimeField: TimeOnly { TimeOnly(date) }

    // The actual date storage
    public var date = Date()
    @Field public var utcOffset: String = "+00:00"

    // GPS fields
    @Field public var latitude: Float? // °
    @Field public var longitude: Float? // °
    @Field public var altitudeGPS: Int? // ft
    @Field public var GPSFixStatus: GPSFixStatus? //  NoSoln, 3D, 3D-, 3DDiff
    @Field public var timeOfWeek: String? // Empty in our case
    @Field public var groundSpeed: Float? // kts
    @Field public var groundTrack: UInt16? // °T
    @Field public var eastVelocity: Float? // Empty in our case
    @Field public var northVelocity: Float? // Empty in our case
    @Field public var upVelocity: Float? // Empty in our case

    // Flight instruments
    @Field public var heading: Float? // °M
    @Field public var PDOP: Float? // Empty in our case
    @Field public var GPSSats: Int? // Empty in our case
    @Field public var pressureAltitude: Int? // ft
    @Field public var baroAltitude: Int? // ft
    @Field public var verticalSpeed: Int? // fpm
    @Field public var indicatedAirspeed: Float? // kts
    @Field public var trueAirspeed: Float? // kts
    @Field public var pitch: Float? // °
    @Field public var roll: Float? // °
    @Field public var lateralAcceleration: Float? // g
    @Field public var normalAcceleration: Float? // g

    // Autopilot/Flight Director
    @Field public var headingBug: UInt16? // °M
    @Field public var altitudeBug: Int? // ft
    @Field public var selectedVS: Int? // Empty in our case
    @Field public var selectedAirspeed: Float? // Empty in our case
    @Field public var altimeterSetting: Float? // inHg

    // Radios (mostly empty)
    @Field public var COM1: String? // Empty
    @Field public var COM2: String? // Empty
    @Field public var NAV1: String? // Empty

    // Navigation
    @Field public var navSource: String? // GPS1
    @Field public var navAnnunciation: String? // Empty
    @Field public var navIdentifier: String?
    @Field public var navDistance: Float? // NM
    @Field public var navBearing: UInt16? // °M
    @Field public var navCourse: UInt16? // °M
    @Field public var crossTrackDistance: Float? // NM
    @Field public var horizontalCDIDeflection: Float? // -1..1
    @Field public var horizontalCDIFullScale: Float? // Empty
    @Field public var horizontalCDIScale: String? // OCN, TERM, ENR, etc.
    @Field public var verticalCDIDeflection: Float? // -1..1
    @Field public var verticalCDIFullScale: Float? // Empty
    @Field public var VNAVCDIDeflection: Float? // Empty
    @Field public var VNAVTargetAltitude: Int? // ft

    // Autopilot modes
    @Field public var autopilotState: String? // AP, Fail
    @Field public var FDLateralMode: String? // active (armed)
    @Field public var FDVerticalMode: String? // active (armed)
    @Field public var FDRollCommand: Float? // °
    @Field public var FDPitchCommand: Float? // °
    @Field public var FDAltitude: Int? // Empty
    @Field public var APRollCommand: Float? // °
    @Field public var APPitchCommand: Float? // °
    @Field public var APVSCommand: Int? // Empty
    @Field public var APAltitudeCommand: Int? // Empty
    @Field public var APRollTorque: Float? // Empty
    @Field public var APPitchTorque: Float? // Empty
    @Field public var APRollTrimMotor: Float? // Empty
    @Field public var APPitchTrimMotor: Float? // Empty

    // Environment
    @Field public var magneticVariation: Float? // °
    @Field public var outsideAirTemperature: Float? // °C
    @Field public var densityAltitude: Int? // ft
    @Field public var heightAGL: Int?
    @Field public var windSpeed: Float? // kt
    @Field public var windDirection: Int? // °T

    // System status (mostly empty)
    @Field public var AHRSStatus: String? // Empty
    @Field public var AHRSDev: Float? // Empty
    @Field public var magnetometerStatus: String? // Empty
    @Field public var networkStatus: String? // Empty
    @Field public var transponderCode: String? // Empty
    @Field public var transponderMode: String? // Empty

    // Engine parameters
    @Field public var oilTemperature: Int? // °F
    @Field public var fuelLQty: Float? // Empty - fuel left qty
    @Field public var fuelRQty: Float? // Empty - fuel right qty
    @Field public var fuelPress: Float? // Empty - fuel pressure
    @Field public var oilPressure: UInt? // psi
    @Field public var RPM: UInt? // rpm
    @Field public var manifoldPressure: Float? // inHg
    @Field public var potential1: Float? // V
    @Field public var potential2: Float? // V
    @Field public var amps1: Float? // Empty
    @Field public var amps2: Float? // Empty
    @Field public var fuelFlow: Float? // gph
    @Field public var elevatorTrim: Float? // Empty
    @Field public var aileronTrim: Float? // Empty

    // CHTs and EGTs - as arrays that will be expanded
    @Fields(6) public var CHTs: [Int?] = Array(repeating: nil, count: 6) // °F
    @Fields(6) public var EGTs: [Int?] = Array(repeating: nil, count: 6) // °F

    @Field public var percentPower: UInt8? // %
    @Field public var CASAlert: String? // Empty
    @Field public var terrainAlert: String? // Empty
    @Field public var engineCycleCount: Int? // Empty
}

// Custom date formatter for Garmin CSV
extension GarminRecord {
    public func formatDate() -> String {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd"
        formatter.timeZone = TimeZone(identifier: "UTC")
        return formatter.string(from: date)
    }

    public func formatTime() -> String {
        let formatter = DateFormatter()
        formatter.dateFormat = "HH:mm:ss"
        formatter.timeZone = TimeZone(identifier: "UTC")
        return formatter.string(from: date)
    }
}
