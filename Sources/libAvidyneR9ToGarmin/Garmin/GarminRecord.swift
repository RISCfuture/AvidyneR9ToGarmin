import Foundation

struct GarminRecord {
    let date: Date
    let latitude: Float? // °
    let longitude: Float? // °
    let altitudeGPS: Int? // ft
    let GPSFixStatus: String? //  NoSoln, 3D, 3D-, 3DDiff
    let groundSpeed: Float? // kts
    let groundTrack: UInt16? // °T
    let heading: Float? // °M
    let pressureAltitude: Int? // ft
    let baroAltitude: Int? // ft
    let verticalSpeed: Int? // fpm
    let indicatedAirspeed: Float? // kts
    let trueAirspeed: Float? // kts
    let pitch: Float? // °
    let roll: Float? // °
    let lateralAcceleration: Float? // g
    let normalAcceleration: Float? // g
    let headingBug: UInt16? // °M
    let altitudeBug: Int? // ft
    let altimeterSetting: Float? // inHg
    let navSource: String? // GPS1
    let navIdentifier: String?
    let navFrequency: Float? // MHz
    let navDistance: Float? // NM
    let navBearing: UInt16? // °M
    let navCourse: UInt16? // °M
    let crossTrackDistance: Float? // NM
    let horizontalCDIDeflection: Float? // -1..1
    let horizontalCDIScale: String? // OCN, TERM, ENR, etc.
    let verticalCDIDeflection: Float? // -1..1
    let VNAVTargetAltitude: Int? // ft
    let autopilotState: String? // AP, Fail
    let FDLateralMode: String? // active (armed)
    let FDVerticalMode: String? // active (armed)
    let FDRollCommand: Float? // °
    let FDPitchCommand: Float? // °
    let APRollCommand: Float? // °
    let APPitchCommand: Float? // °
    let magneticVariation: Float? // °
    let outsideAirTemperature: Float? // °C
    let densityAltitude: Int? // ft
    let heightAGL: Int?
    let windSpeed: Float? // kt
    let windDirection: Int? // °T
    let oilTemperature: Int? // °F
    let oilPressure: UInt? // psi
    let RPM: UInt? // rpm
    let manifoldPressure: Float? // inHg
    let potential1: Float? // V
    let potential2: Float? // V
    let fuelFlow: Float? // gph
    let CHTs: Array<Int?> // °F
    let EGTs: Array<Int?> // °F
    let percentPower: UInt8? // %
}
