import Foundation

extension R9ToGarminConverter {
    private static let gpsStates: [String?] = ["NoSoln", "NoSoln", "NoSoln", "NoSoln", "3D-", "3D", "3DDiff"]
    private static let fmsModes: [String?] = ["OCN", "ENR", "TERM", "DEP", "GA", "APCH"]
    private static let fmsCDIScales: [Float] = [30380.6, 12152.2, 6076.12, 6076.12, 6076.12, 1822.83]
    private static let cdiSources: [String?] = ["GPS1", "LOC1", "LOC2"]
    private static let gpsSources: [String?] = ["AUTO", "GPS1", "GPS2"]
    private static let autopilotLateralModes: [String?] = [nil, "ROLL", "HDG", "LOC", "LOC-BC", "VOR", "VOR-APPR", "APPR", "NAV", "NAV-INTCPT"]
    private static let autopilotVerticalModes: [String?] = [nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, "PITCH", "IAS", "VS", "ALT", "GS", "ALT-GS", "VNAV-ALT", "VNAV-VS"]

    func r9RecordsToGarminRecord(_ r9Records: [R9Record], date: Date) async throws -> GarminRecord {
        let (engineRecord, flightRecord, systemRecord) = try recordCombiners(from: r9Records, date: date),

        baroAltitude = await zipOptionals(systemRecord.get(\.altimeterSetting), flightRecord.get(\.pressureAltitude))
            .map { altimeterSetting, pressureAltitude in
                pressureAltitude + Int(((altimeterSetting - 29.92) * 1000).rounded()) // TODO is this good enough?
            },

        autopilotOn = await zipOptionals(flightRecord.get(\.DFC100_activeLateralMode), flightRecord.get(\.DFC100_activeVerticalMode))
            .map { $0 != 0 && $1 != 0 } ?? false,

        navSource = await indexToStr(systemRecord.get(\.courseSelect, strategy: .mode), values: Self.cdiSources),
        FMS = navSource == "GPS1",

        GPSNavDeviation = await zipOptionals(systemRecord.get(\.crossTrackDeviation), systemRecord.get(\.courseSelect, strategy: .mode))
            .map { xdev, FMSMode in xdev / Self.fmsCDIScales[Int(FMSMode)] },

        latitude = await flightRecord.get(\.GPSLatitude),
        longitude = await flightRecord.get(\.GPSLongitude),
        altitudeGPS = await m_ft_i(systemRecord.get(\.GPSAltitudeMSL)),
        GPSFixStatus = await indexToStr(systemRecord.get(\.GPSState, strategy: .mode), values: Self.gpsStates),
        groundSpeed = await to_f(systemRecord.get(\.groundSpeed)),
        groundTrack = await magToTrue(systemRecord.get(\.groundTrack, strategy: .bearing), var: systemRecord.get(\.magneticVariation)),
        heading = await flightRecord.get(\.heading, strategy: .bearing),
        pressureAltitude = await flightRecord.get(\.pressureAltitude),
        verticalSpeed = await flightRecord.get(\.verticalSpeed),
        indicatedAirspeed = await to_f(flightRecord.get(\.indicatedAirspeed)),
        trueAirspeed = await to_f(flightRecord.get(\.trueAirspeed)),
        pitch = await flightRecord.get(\.pitch),
        roll = await flightRecord.get(\.roll),
        lateralAcceleration = await flightRecord.get(\.lateralAcceleration),
        unfilteredNormal = await flightRecord.get(\.normalAcceleration),
        normalAcceleration = await flightRecord.get(\.filteredNormalAcceleration) ?? unfilteredNormal,
        headingBug = await systemRecord.get(\.headingBug, strategy: .mode),
        altitudeBug = await systemRecord.get(\.altitudeBug, strategy: .mode),
        altimeterSetting = await systemRecord.get(\.altimeterSetting, strategy: .mode),

        navIdentifier = await systemRecord.get(\.activeWaypoint),
        navFrequency = await kHz_MHz(systemRecord.get(\.navFrequency, strategy: .mode)),
        navDistance = await systemRecord.get(\.distanceToWaypoint),
        navBearing = await FMS
        ? systemRecord.get(\.navaidBearing, strategy: .bearing)
        : nil,
        navCourse = await FMS
        ? systemRecord.get(\.desiredTrack, strategy: .bearing)
        : systemRecord.get(\.OBS, strategy: .bearing),
        crossTrackDistance = await FMS
        ? systemRecord.get(\.crossTrackDeviation)
        : nil,
        horizontalCDIDeflection = await FMS
        ? GPSNavDeviation
        : systemRecord.get(\.localizerDeviation),
        horizontalCDIScale = await FMS
        ? indexToStr(systemRecord.get(\.navigationMode, strategy: .mode), values: Self.fmsModes)
        : nil,
        verticalCDIDeflection = await FMS
        ? nil
        : systemRecord.get(\.glideslopeDeviation),
        VNAVTargetAltitude = await flightRecord.get(\.DFC100_altitudeTarget, strategy: .mode),
        FDLateralMode = await autopilotModeStr(active: flightRecord.get(\.DFC100_activeLateralMode, strategy: .mode),
                                               armed: flightRecord.get(\.DFC100_armedLateralMode, strategy: .mode),
                                               values: Self.autopilotLateralModes),
        FDVerticalMode = await autopilotModeStr(active: flightRecord.get(\.DFC100_activeVerticalMode, strategy: .mode),
                                                armed: flightRecord.get(\.DFC100_armedVerticalMode, strategy: .mode),
                                                values: Self.autopilotVerticalModes),
        FDRollCommand = await flightRecord.get(\.FDRoll),
        FDPitchCommand = await flightRecord.get(\.FDPitch),
        APRollCommand = await autopilotOn ? flightRecord.get(\.FDRoll) : nil,
        APPitchCommand = await autopilotOn ? flightRecord.get(\.FDPitch) : nil,
        magneticVariation = await systemRecord.get(\.magneticVariation),
        outsideAirTemperature = await to_f(systemRecord.get(\.oat)),
        heightAGL = await m_ft_i(systemRecord.get(\.GPSHeightAGL)),
        oilTemperature = await engineRecord.get(\.oilTemperature),
        oilPressure = await engineRecord.get(\.oilPressure),
        RPM = await engineRecord.get(\.RPM),
        manifoldPressure = await engineRecord.get(\.manifoldPressure),
        potential1 = await engineRecord.get(\.mainBus1Potential),
        potential2 = await engineRecord.get(\.emergencyBusPotential),
        fuelFlow = await engineRecord.get(\.fuelFlow),
        CHTs = await engineRecord.get(\.CHTs).map { to_i($0) },
        EGTs = await engineRecord.get(\.EGTs).map { to_i($0) },
        percentPower = await to_ui8(engineRecord.get(\.percentPower))

        return .init(date: date,
                     latitude: latitude,
                     longitude: longitude,
                     altitudeGPS: altitudeGPS,
                     GPSFixStatus: GPSFixStatus,
                     groundSpeed: groundSpeed,
                     groundTrack: groundTrack,
                     heading: heading,
                     pressureAltitude: pressureAltitude,
                     baroAltitude: baroAltitude,
                     verticalSpeed: verticalSpeed,
                     indicatedAirspeed: indicatedAirspeed,
                     trueAirspeed: trueAirspeed,
                     pitch: pitch,
                     roll: roll,
                     lateralAcceleration: lateralAcceleration,
                     normalAcceleration: normalAcceleration,
                     headingBug: headingBug,
                     altitudeBug: altitudeBug,
                     altimeterSetting: altimeterSetting,
                     navSource: navSource,
                     navIdentifier: navIdentifier,
                     navFrequency: navFrequency,
                     navDistance: navDistance,
                     navBearing: navBearing,
                     navCourse: navCourse,
                     crossTrackDistance: crossTrackDistance,
                     horizontalCDIDeflection: horizontalCDIDeflection,
                     horizontalCDIScale: horizontalCDIScale,
                     verticalCDIDeflection: verticalCDIDeflection,
                     VNAVTargetAltitude: VNAVTargetAltitude,
                     autopilotState: autopilotOn ? "AP" : nil,
                     FDLateralMode: FDLateralMode,
                     FDVerticalMode: FDVerticalMode,
                     FDRollCommand: FDRollCommand,
                     FDPitchCommand: FDPitchCommand,
                     APRollCommand: APRollCommand,
                     APPitchCommand: APPitchCommand,
                     magneticVariation: magneticVariation,
                     outsideAirTemperature: outsideAirTemperature,
                     densityAltitude: nil, // TODO calculate this>
                     heightAGL: heightAGL,
                     windSpeed: nil,
                     windDirection: nil, // TODO calculate these?
                     oilTemperature: oilTemperature,
                     oilPressure: oilPressure,
                     RPM: RPM,
                     manifoldPressure: manifoldPressure,
                     potential1: potential1,
                     potential2: potential2,
                     fuelFlow: fuelFlow,
                     CHTs: CHTs,
                     EGTs: EGTs,
                     percentPower: percentPower)
    }

    private func recordCombiners(from records: [R9Record], date: Date) throws -> (R9RecordCombiner<R9EngineRecord>, R9RecordCombiner<R9FlightRecord>, R9RecordCombiner<R9SystemRecord>) {
        let engineRecords: [R9EngineRecord] = records.filter { $0.type == .engine } as! [R9EngineRecord]
        guard !engineRecords.isEmpty else { throw AvidyneR9ToGarminError.incompleteRecordsForDate(date) }
        let engineCombiner = R9RecordCombiner(records: engineRecords)

        let flightRecords: [R9FlightRecord] = records.filter { $0.type == .flight } as! [R9FlightRecord]
        guard !flightRecords.isEmpty else { throw AvidyneR9ToGarminError.incompleteRecordsForDate(date) }
        let flightCombiner = R9RecordCombiner(records: flightRecords)

        let systemRecords: [R9SystemRecord] = records.filter { $0.type == .system } as! [R9SystemRecord]
        guard !systemRecords.isEmpty else { throw AvidyneR9ToGarminError.incompleteRecordsForDate(date) }
        let systemCombiner = R9RecordCombiner(records: systemRecords)

        return (engineCombiner, flightCombiner, systemCombiner)
    }

    private func m_ft(_ m: Float?) -> Float? {
        guard let m else { return nil }
        return m * 3.28084
    }

    private func m_ft(_ m: Int?) -> Float? {
        guard let m else { return nil }
        return m_ft(Float(m))
    }

    private func m_ft_i(_ m: Float?) -> Int? {
        guard let ft = m_ft(m) else { return nil }
        return Int(ft.rounded())
    }

    private func m_ft_i(_ m: Int?) -> Int? {
        guard let m else { return nil }
        guard let ft = m_ft_i(Float(m)) else { return nil }
        return Int(ft)
    }

    private func kHz_MHz(_ kHz: UInt?) -> Float? {
        guard let kHz else { return nil }
        return Float(kHz) / 1000
    }

    private func to_f(_ i: Int?) -> Float? {
        guard let i else { return nil }
        return Float(i)
    }

    private func to_f(_ i: UInt?) -> Float? {
        guard let i else { return nil }
        return Float(i)
    }

    private func to_i(_ f: Float?) -> Int? {
        guard let f else { return nil }
        return Int(f.rounded())
    }

    private func to_ui8(_ f: Float?) -> UInt8? {
        guard let f else { return nil }
        return UInt8(f.rounded())
    }

    private func magToTrue(_ mag: Int?, `var`: Float?) -> UInt16? {
        guard let mag, let `var` else { return nil }
        let `true` = Int((Float(mag) + `var`).rounded())
        return normalizedHeading(`true`)
    }

    private func magToTrue(_ mag: Float?, `var`: Float?) -> Float? {
        guard let mag, let `var` else { return nil }
        let `true` = Float(mag) + `var`
        return normalizedHeading(`true`)
    }

    private func normalizedHeading(_ heading: Int?) -> UInt16? {
        guard let heading else { return nil }

        var modHeading = heading % 360
        if modHeading < 0 { modHeading += 360 }

        return UInt16(modHeading)
    }

    private func normalizedHeading(_ heading: Float?) -> Float? {
        guard let heading else { return nil }

        var modHeading = heading.truncatingRemainder(dividingBy: 360)
        if modHeading < 0 { modHeading += 360 }

        return modHeading
    }

    private func indexToStr(_ index: UInt8?, values: [String?]) -> String? {
        guard let index else { return nil }
        return values[Int(index)]
    }

    private func autopilotModeStr(active: UInt8?, armed: UInt8?, values: [String?]) -> String? {
        guard let active else { return nil }
        guard let activeStr = values[Int(active)] else { return nil }

        if let armed, let armedStr = values[Int(armed)] {
            return "\(activeStr) (\(armedStr))"
        }
        return activeStr
    }
}
