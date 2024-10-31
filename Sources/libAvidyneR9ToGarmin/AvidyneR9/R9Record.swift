import Foundation

enum R9RecordType {
    case engine
    case flight
    case system
}

protocol R9Record: Sendable {
    var type: R9RecordType { get }
    var systime: UInt? { get }
    var date: Date? { get }
}

struct R9EngineRecord: R9Record {
    var type = R9RecordType.engine
    
    var systime: UInt?
    var date: Date?
    
    var oilTemperature: Int? // °F
    var oilPressure: UInt? // psi
    var RPM: UInt? // rpm
    var manifoldPressure: Float? // inHg
//    var TIT: Int? // °F
    var CHTs: Array<Float?> = [] // °F
    var EGTs: Array<Float?> = [] // °F
    var percentPower: Float? // 0..100
    var fuelFlow: Float? // gph
    var fuelUsed: Float? // gal
    var fuelRemaining: Float? // gal
    var fuelTimeRemaining: Int? // min
    var fuelEconomy: Float? // NM/gal
//    var _alt1Current: Int? // A
//    var _alt2Current: Int? // A
//    var _bat1Current: Int? // A
//    var _bat2Current: Int? // A
    var mainBus1Potential: Float? // V
//    var _mainBus2Potential: Float? // V
    var emergencyBusPotential: Float? // V
//    var _fuelQuantityLeft: Int? // gal
//    var _fuelQuantityRight: Int? // gal
//    var _deiceVacuum: Float? // inHg
//    var _rudderTrim: Int? // °
//    var _flapSetting: Int? // °
//    var _Ng: Float? // %
//    var _torque: Int? // ft-lbs
//    var _ITT: Int? // °C
//    var _Np: Int? // rpm
//    var _discreteInputs: String?
//    var _discreteOutputs: String?
}

struct R9FlightRecord: R9Record {
    var type = R9RecordType.flight
    
    var systime: UInt?
    var date: Date?
    
    var filteredNormalAcceleration: Float? // g
    var normalAcceleration: Float? // g
    var longitudinalAcceleration: Float? // g
    var lateralAcceleration: Float? // g
    var activeADAHRS: UInt8?
    var AHRSStatus: UInt8?
    var heading: Float? // °M
    var pitch: Float? // °
    var roll: Float? // °
    var FDPitch: Float? // °
    var FDRoll: Float? // °
    var headingRate: Float? // °/sec
    var pressureAltitude: Int? // ft
    var indicatedAirspeed: UInt? // kts
    var trueAirspeed: UInt? // kts
    var verticalSpeed: Int? // fpm
    var GPSLatitude: Float?
    var GPSLongitude: Float?
    var bodyYawRate: Float? // °/sec
    var bodyPitchRate: Float? // °/sec
    var bodyRollRate: Float? // °/sec
    var magnetometerStatus: UInt8?
    var IRUStatus: UInt8?
    var MPUStatus: UInt8?
    var ADCStatus: String?
    var AHRSSequence: UInt?
    var ADCSequence: UInt?
    var AHRStartupMode: UInt8?
    var DFC100_activeLateralMode: UInt8?
    var DFC100_armedLateralMode: UInt8?
    var DFC100_activeVerticalMode: UInt8?
    var DFC100_armedVerticalMode: UInt8?
    var DFC100_statusFlags: UInt? // hex
    var DFC100_failFlags: UInt? // hex
    var DFC100_altitudeTarget: Int? // ft
}

struct R9SystemRecord: R9Record {
    var type = R9RecordType.system
    
    var systime: UInt?
    var date: Date?
    
    var oat: Int? // °C
    var localizerDeviation: Float? // -1..1
    var glideslopeDeviation: Float? // -1..1
    var flightDirectorOnOff: Bool?
    var autopilotMode: String?
    var groundSpeed: UInt? // kts
    var groundTrack: Int? // °M
    var crossTrackDeviation: Float? // NM
    var verticalDeviation: Int? // ft
    var altimeterSetting: Float? // inHg
    var altitudeBug: Int? // ft
    var verticalSpeedBug: Int? // fpm
    var headingBug: UInt16? // °M
    var displayMode: UInt8?
    var navigationMode: UInt8? // fmsModes
    var activeWaypoint: String?
    var activeGPS: UInt8?
    var navaidBearing: UInt16? // °M
    var OBS: UInt16? // °M
    var desiredTrack: UInt16? // °M
    var navFrequency: UInt? // kHz
    var courseSelect: UInt8? // cdiSources
    var navType: UInt8? // ???
    var courseDeviation: Int? // °
    var GPSAltitude: Int? // m
    var distanceToWaypoint: Float? // NM
    var GPSState: UInt8?
    var GPSHorizontalProterctionLimit: Float? // m
    var GPSVerticalProterctionLimit: Float? // m
    var SBAS_HPL: Float? // m
    var SBAS_VPL: Float? // m
    var HFOM: Float? // m
    var VFOM: Float? // m
    var FMSCourse: UInt16?
    var magneticVariation: Float?
    var GPSAltitudeMSL: Int? // m
    var GPSHeightAGL: Int? // m
    var FLTA_RTC: Int? // m
    var FLTA_ATC: Int?  // m
    var FLTA_VerticalSpeed: Int? // fpm
    var FLTA_RTCDistance: Int? // m
    var FLTA_TerrainDistance: Int? // m
    var FLTA_Status: UInt8?
}
