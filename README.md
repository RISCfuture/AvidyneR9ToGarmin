# AvidyneR9ToGarmin

Converts Avidyne R9 log files to Garmin CSV format, for use with websites that
expect logs in Garmin format.

## Installation

AvidyneR9ToGarmin is a Swift Package Manager project with an executable target.
It has been developed for macOS 15 using Swift 6.

1. Edit the `Sources/libAvidyneR9ToGarmin/Converter/R9ToGarminConverter.swift`
   file. Change the `private static let headers` constant; in particular, the
   `aircraft_ident` and `system_id` fields. **Note that many websites use these
   fields to automatically assign log files to aircraft profiles.**
2. Run `swift build` (or `swift build -c release`) to create the
   `avidyne-r9-to-garmin` executable.

## Usage

```
USAGE: avidyne-r9-to-garmin <input> <output> [--verbose]

ARGUMENTS:
  <input>                 The directory where Avidyne R9 CSVs are stored.
  <output>                The directory to contain the generated Garmin CSV files.

OPTIONS:
  --verbose               Include extra information in the output.
  -h, --help              Show help information.
```

The executable will create a Garmin log file for each power-on cycle of the R9
system. Empty log files will automatically be deleted; however, a "long tail" of
trivially small log files will still be present. You can, at your discretion,
delete these irrelevant smaller log files.

## Limitations

- The R9 system does not log many of the data points present in Garmin logs
  (e.g., target vertical speed, GPS _U_/_X_/_W_, wind direction and speed, etc.)
  Some websites can interpolate some of these values, but others will
  necessarily be un-derivable.
