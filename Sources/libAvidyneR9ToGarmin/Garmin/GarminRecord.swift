import Foundation
import StreamingCSV

// Cached date formatters for performance
private enum DateFormatters {
    static let dateFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd"
        formatter.timeZone = TimeZone(identifier: "UTC")
        formatter.locale = Locale(identifier: "en_US_POSIX")
        return formatter
    }()

    static let timeFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "HH:mm:ss"
        formatter.timeZone = TimeZone(identifier: "UTC")
        formatter.locale = Locale(identifier: "en_US_POSIX")
        return formatter
    }()
}

// Custom wrapper types for date and time formatting
struct DateOnly: CSVCodable, Sendable {
    let date: Date

    var csvString: String {
        return DateFormatters.dateFormatter.string(from: date)
    }

    init(_ date: Date) {
        self.date = date
    }

    init?(csvString: String) {
        guard let date = DateFormatters.dateFormatter.date(from: csvString) else { return nil }
        self.date = date
    }
}

struct TimeOnly: CSVCodable, Sendable {
    let date: Date

    var csvString: String {
        return DateFormatters.timeFormatter.string(from: date)
    }

    init(_ date: Date) {
        self.date = date
    }

    init?(csvString: String) {
        guard let date = DateFormatters.timeFormatter.date(from: csvString) else { return nil }
        self.date = date
    }
}

// Garmin CSV output format using StreamingCSV encoder
@CSVRowEncoderBuilder
struct GarminRecord: Sendable {
    // Date and time fields - using wrapper types for proper formatting
    @Field var dateField: DateOnly { DateOnly(date) }
    @Field var timeField: TimeOnly { TimeOnly(date) }
    @Field var localTimeField: TimeOnly { TimeOnly(date) }

    // The actual date storage
    var date = Date()
    @Field var utcOffset: String = "+00:00"

    // GPS fields
    @Field var latitude: Float? // °
    @Field var longitude: Float? // °
    @Field var altitudeGPS: Int? // ft
    @Field var GPSFixStatus: GPSFixStatus? //  NoSoln, 3D, 3D-, 3DDiff
    @Field var timeOfWeek: String? // Empty in our case
    @Field var groundSpeed: Float? // kts
    @Field var groundTrack: UInt16? // °T
    @Field var eastVelocity: Float? // Empty in our case
    @Field var northVelocity: Float? // Empty in our case
    @Field var upVelocity: Float? // Empty in our case

    // Flight instruments
    @Field var heading: Float? // °M
    @Field var PDOP: Float? // Empty in our case
    @Field var GPSSats: Int? // Empty in our case
    @Field var pressureAltitude: Int? // ft
    @Field var baroAltitude: Int? // ft
    @Field var verticalSpeed: Int? // fpm
    @Field var indicatedAirspeed: Float? // kts
    @Field var trueAirspeed: Float? // kts
    @Field var pitch: Float? // °
    @Field var roll: Float? // °
    @Field var lateralAcceleration: Float? // g
    @Field var normalAcceleration: Float? // g

    // Autopilot/Flight Director
    @Field var headingBug: UInt16? // °M
    @Field var altitudeBug: Int? // ft
    @Field var selectedVS: Int? // Empty in our case
    @Field var selectedAirspeed: Float? // Empty in our case
    @Field var altimeterSetting: Float? // inHg

    // Radios (mostly empty)
    @Field var COM1: String? // Empty
    @Field var COM2: String? // Empty
    @Field var NAV1: String? // Empty

    // Navigation
    @Field var navSource: String? // GPS1
    @Field var navAnnunciation: String? // Empty
    @Field var navIdentifier: String?
    @Field var navDistance: Float? // NM
    @Field var navBearing: UInt16? // °M
    @Field var navCourse: UInt16? // °M
    @Field var crossTrackDistance: Float? // NM
    @Field var horizontalCDIDeflection: Float? // -1..1
    @Field var horizontalCDIFullScale: Float? // Empty
    @Field var horizontalCDIScale: String? // OCN, TERM, ENR, etc.
    @Field var verticalCDIDeflection: Float? // -1..1
    @Field var verticalCDIFullScale: Float? // Empty
    @Field var VNAVCDIDeflection: Float? // Empty
    @Field var VNAVTargetAltitude: Int? // ft

    // Autopilot modes
    @Field var autopilotState: String? // AP, Fail
    @Field var FDLateralMode: String? // active (armed)
    @Field var FDVerticalMode: String? // active (armed)
    @Field var FDRollCommand: Float? // °
    @Field var FDPitchCommand: Float? // °
    @Field var FDAltitude: Int? // Empty
    @Field var APRollCommand: Float? // °
    @Field var APPitchCommand: Float? // °
    @Field var APVSCommand: Int? // Empty
    @Field var APAltitudeCommand: Int? // Empty
    @Field var APRollTorque: Float? // Empty
    @Field var APPitchTorque: Float? // Empty
    @Field var APRollTrimMotor: Float? // Empty
    @Field var APPitchTrimMotor: Float? // Empty

    // Environment
    @Field var magneticVariation: Float? // °
    @Field var outsideAirTemperature: Float? // °C
    @Field var densityAltitude: Int? // ft
    @Field var heightAGL: Int?
    @Field var windSpeed: Float? // kt
    @Field var windDirection: Int? // °T

    // System status (mostly empty)
    @Field var AHRSStatus: String? // Empty
    @Field var AHRSDev: Float? // Empty
    @Field var magnetometerStatus: String? // Empty
    @Field var networkStatus: String? // Empty
    @Field var transponderCode: String? // Empty
    @Field var transponderMode: String? // Empty

    // Engine parameters
    @Field var oilTemperature: Int? // °F
    @Field var fuelLQty: Float? // Empty - fuel left qty
    @Field var fuelRQty: Float? // Empty - fuel right qty
    @Field var fuelPress: Float? // Empty - fuel pressure
    @Field var oilPressure: UInt? // psi
    @Field var RPM: UInt? // rpm
    @Field var manifoldPressure: Float? // inHg
    @Field var potential1: Float? // V
    @Field var potential2: Float? // V
    @Field var amps1: Float? // Empty
    @Field var amps2: Float? // Empty
    @Field var fuelFlow: Float? // gph
    @Field var elevatorTrim: Float? // Empty
    @Field var aileronTrim: Float? // Empty

    // CHTs and EGTs - as arrays that will be expanded
    @Fields(6)
    var CHTs: [Int?] = Array(repeating: nil, count: 6) // °F
    @Fields(6)
    var EGTs: [Int?] = Array(repeating: nil, count: 6) // °F

    @Field var percentPower: UInt8? // %
    @Field var CASAlert: String? // Empty
    @Field var terrainAlert: String? // Empty
    @Field var engineCycleCount: Int? // Empty
}

// Custom date formatter for Garmin CSV
extension GarminRecord {
    func formatDate() -> String {
        return DateFormatters.dateFormatter.string(from: date)
    }

    func formatTime() -> String {
        return DateFormatters.timeFormatter.string(from: date)
    }
}
