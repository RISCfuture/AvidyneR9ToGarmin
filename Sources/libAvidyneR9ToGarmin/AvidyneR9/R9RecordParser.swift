import Foundation
import CodableCSV

class R9RecordParser<RecordType> where RecordType: R9Record {
    let row: CSVReader.Record
    var record: RecordType
    
    var isPowerOn: Bool { row[3] == powerOnSentinel }
    var isIncrementalExtract: Bool { row[3] == incrementalExtractSentinel }
    
    init(row: CSVReader.Record, record: RecordType) {
        self.row = row
        self.record = record // copies by value; you MUST use `self.record` to get the resulting modified record
    }
    
    @discardableResult
    func parse(field: String, into key: WritableKeyPath<RecordType, String?>, required: Bool = false) throws -> Bool {
        let parsed = try parseString(field: field, required: required)
        record[keyPath: key] = parsed
        return parsed != nil
    }
    
    @discardableResult
    func parse(field: String, into key: WritableKeyPath<RecordType, Int?>, radix: Int = 10, prefix: String = "", required: Bool = false) throws -> Bool {
        guard let value = try parseString(field: field, required: required) else {
            record[keyPath: key] = nil
            return false
        }
        record[keyPath: key] = try parseNumber(field: field, value: value, prefix: prefix) { Int($0, radix: radix) }
        return true
    }
    
    @discardableResult
    func parse(field: String, into key: WritableKeyPath<RecordType, UInt?>, radix: Int = 10, prefix: String = "", required: Bool = false) throws -> Bool {
        guard let value = try parseString(field: field, required: required) else {
            record[keyPath: key] = nil
            return false
        }
        record[keyPath: key] = try parseNumber(field: field, value: value, prefix: prefix) { UInt($0, radix: radix) }
        return true
    }
    
    @discardableResult
    func parse(field: String, into key: WritableKeyPath<RecordType, UInt8?>, radix: Int = 10, prefix: String = "", required: Bool = false) throws -> Bool {
        guard let value = try parseString(field: field, required: required) else {
            record[keyPath: key] = nil
            return false
        }
        record[keyPath: key] = try parseNumber(field: field, value: value, prefix: prefix) { UInt8($0, radix: radix) }
        return true
    }
    
    @discardableResult
    func parse(field: String, into key: WritableKeyPath<RecordType, UInt16?>, radix: Int = 10, prefix: String = "", required: Bool = false) throws -> Bool {
        guard let value = try parseString(field: field, required: required) else {
            record[keyPath: key] = nil
            return false
        }
        record[keyPath: key] = try parseNumber(field: field, value: value, prefix: prefix) { UInt16($0, radix: radix) }
        return true
    }
    
    @discardableResult
    func parse(field: String, into key: WritableKeyPath<RecordType, Float?>, zeroIsNull: Bool = false, required: Bool = false) throws -> Bool {
        guard let value = try parseString(field: field, required: required) else {
            record[keyPath: key] = nil
            return false
        }
        guard let num = Float(value) else {
            throw AvidyneR9ToGarminError.invalidValue(field: field, value: value)
        }
        if zeroIsNull && num.isZero {
            record[keyPath: key] = nil
            return false
        } else {
            record[keyPath: key] = num
            return true
        }
    }
    
    @discardableResult
    func parse(field: String, into key: WritableKeyPath<RecordType, Bool?>, required: Bool = false) throws -> Bool {
        guard let value = try parseString(field: field, required: required) else {
            record[keyPath: key] = nil
            return false
        }
        record[keyPath: key] = try parseBool(field: field, value: value)
        return true
    }
    
    func parse(dateField: String, timeField: String, into key: WritableKeyPath<RecordType, Date?>) throws {
        guard let dateValue = try parseString(field: dateField, required: true) else {
            throw AvidyneR9ToGarminError.missingField(dateField)
        }
        guard let timeValue = try parseString(field: timeField, required: true) else {
            throw AvidyneR9ToGarminError.missingField(timeField)
        }
        
        guard let year = UInt(dateValue.prefix(4)),
              let month = UInt(dateValue[dateValue.index(dateValue.startIndex, offsetBy: 4)..<dateValue.index(dateValue.endIndex, offsetBy: -2)]),
              let day = UInt(dateValue.suffix(2)) else {
            throw AvidyneR9ToGarminError.invalidDate(date: dateValue, time: timeValue)
        }
        
        if year < 2005 {
            throw AvidyneR9ToGarminError.invalidDate(date: dateValue, time: timeValue)
        }
        
        let timeParts = timeValue.components(separatedBy: ":")
        guard let hour = UInt(timeParts[0]),
              let minute = UInt(timeParts[1]),
              let second = UInt(timeParts[2]) else {
            throw AvidyneR9ToGarminError.invalidDate(date: dateValue, time: timeValue)
        }
        
        guard let date = Calendar.current.date(from: .init(timeZone: zulu,
                                                           year: Int(year),
                                                           month: Int(month),
                                                           day: Int(day),
                                                           hour: Int(hour),
                                                           minute: Int(minute),
                                                           second: Int(second))) else {
            throw AvidyneR9ToGarminError.invalidDate(date: dateValue, time: timeValue)
        }
        
        record[keyPath: key] = date
    }
    
    @discardableResult
    func parse(field: String, into key: WritableKeyPath<RecordType, Array<Float?>>, count: UInt, required: Bool = false) throws -> Bool {
        let values = try (0..<count).map { index -> Float? in
            let indexField = field.replacingOccurrences(of: "#", with: String(index+1))
            guard let value = try parseString(field: indexField, required: required) else { return nil }
            guard let num = Float(value) else {
                throw AvidyneR9ToGarminError.invalidValue(field: indexField, value: value)
            }
            return num
        }
        
        record[keyPath: key] = values
        return values.allSatisfy { $0 != nil }
    }
    
    private func parseString(field: String, required: Bool = false) throws -> String? {
        guard let value = row[field] else {
            if required { throw AvidyneR9ToGarminError.missingValue(field) }
            else { return nil }
        }
        
        if value.isEmpty { return nil }
        if value == "-" { return nil }
        
        return value
    }
    
    private func parseBool(field: String, value: String) throws -> Bool {
        guard let num = UInt8(value) else {
            throw AvidyneR9ToGarminError.invalidValue(field: field, value: value)
        }
        switch num {
            case 0: return false
            case 1: return true
            default: throw AvidyneR9ToGarminError.invalidValue(field: field, value: value)
        }
    }
    
    private func parseNumber<T: BinaryInteger>(field: String, value: String, prefix: String = "", parser: ((String) -> T?)) throws -> T {
        guard value.hasPrefix(prefix) else { throw AvidyneR9ToGarminError.invalidValue(field: field, value: value) }
        
        let stripped = value.suffix(from: value.index(value.startIndex, offsetBy: prefix.count))
        guard let num = parser(String(stripped)) else {
            throw AvidyneR9ToGarminError.invalidValue(field: field, value: value)
        }
        return num
    }
}
