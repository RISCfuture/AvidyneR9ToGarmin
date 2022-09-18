fileprivate let UTCOffset = "+00:00"

extension R9ToGarminConverter {
    func garminRecordToRow(_ record: GarminRecord) -> Array<String> {
        return [
            record.date.formatted(.iso8601.dateSeparator(.dash).year().month().day()),
            record.date.formatted(.iso8601.timeSeparator(.colon).time(includingFractionalSeconds: false)),
            record.date.formatted(.iso8601.timeSeparator(.colon).time(includingFractionalSeconds: false)),
            UTCOffset,
            format(record.latitude, precision: 7),
            format(record.longitude, precision: 7),
            format(record.altitudeGPS),
            record.GPSFixStatus ?? "",
            "", // time of week
            format(record.groundSpeed, precision: 1),
            format(record.groundTrack),
            "", // E velocity
            "", // N velocity
            "", // U velocity
            format(record.heading, precision: 1),
            "", // GPS PDOP
            "", // GPS sats
            format(record.pressureAltitude),
            format(record.baroAltitude),
            format(record.verticalSpeed),
            format(record.indicatedAirspeed, precision: 1),
            format(record.trueAirspeed),
            format(record.pitch, precision: 2),
            format(record.roll, precision: 2),
            format(record.lateralAcceleration, precision: 3),
            format(record.normalAcceleration, precision: 3),
            format(record.headingBug),
            format(record.altitudeBug),
            "", // VS bug
            "", // IAS bug
            format(record.altimeterSetting, precision: 2),
            "", // COM1
            "", // COM2
            format(record.navFrequency, precision: 3),
            record.navSource ?? "",
            "", // NAV annunciation
            record.navIdentifier ?? "",
            format(record.navDistance, precision: 1),
            format(record.navBearing),
            format(record.navCourse),
            format(record.crossTrackDistance, precision: 3),
            format(record.horizontalCDIDeflection, precision: 2),
            "", // CDI full scale
            record.horizontalCDIScale ?? "",
            format(record.verticalCDIDeflection, precision: 2),
            "", // vert CDI scale
            "", // VNAV CDI deflection
            format(record.VNAVTargetAltitude),
            record.autopilotState ?? "",
            record.FDLateralMode ?? "",
            record.FDVerticalMode ?? "",
            format(record.FDRollCommand, precision: 1),
            format(record.FDPitchCommand, precision: 1),
            format(record.altitudeBug),
            format(record.APRollCommand, precision: 1),
            format(record.APPitchCommand, precision: 1),
            "", // AP VS command,
            "", // AP altitude command
            "", // AP roll torque
            "", // AP pitch torque
            "", // AP roll trim motor
            "", // AP pitch trim motor
            format(record.magneticVariation, precision: 1),
            format(record.outsideAirTemperature, precision: 1),
            format(record.densityAltitude),
            format(record.heightAGL),
            format(record.windSpeed, precision: 1),
            format(record.windDirection),
            "", // AHRS status
            "", // AHRS dev
            "", // magnetometer status
            "", // network status
            "", // transponder code
            "", // transponder mode
            format(record.oilTemperature),
            "", // L fuel qty
            "", // R fuel qty
            "", // fuel press
            format(record.oilPressure),
            format(record.RPM),
            format(record.manifoldPressure, precision: 1),
            format(record.potential1, precision: 1),
            format(record.potential2, precision: 1),
            "", // amps 1
            "", // amps 2
            format(record.fuelFlow, precision: 1),
            "", // pitch trim
            "", // roll trim
            format(record.CHTs[0]),
            format(record.CHTs[1]),
            format(record.CHTs[2]),
            format(record.CHTs[3]),
            format(record.CHTs[4]),
            format(record.CHTs[5]),
            format(record.EGTs[0]),
            format(record.EGTs[1]),
            format(record.EGTs[2]),
            format(record.EGTs[3]),
            format(record.EGTs[4]),
            format(record.EGTs[5]),
            format(record.percentPower),
            "", // CAS alert
            "", // terrain alert
            "" // engine cycle count
        ]
    }
    
    private func format(_ value: Float?, precision: UInt = 0) -> String {
        guard let value = value else { return "" }
        return String(format: "%.\(precision)f", value)
    }
    
    private func format<T>(_ value: T?, precision: UInt = 0) -> String where T: BinaryInteger, T: CVarArg {
        guard let value = value else { return "" }
        if precision > 0 {
            return format(Float(value), precision: precision)
        } else {
            return String(value)
        }
    }
}
