# Copilot Usage Data Analyzer

A Python script that compares GitHub Copilot usage data from the API against seat activity reports to identify discrepancies.

## Purpose

This tool helps GitHub Enterprise administrators identify inconsistencies between two Copilot data sources:

1. **Copilot Usage API Data** - Detailed per-user, per-day usage records exported as JSON
2. **Seat Activity Reports** - CSV exports showing last activity timestamps per user

The script detects two types of discrepancies:

- **Missing Users**: Users who appear in the seat activity report (with activity within the report window) but have no corresponding records in the usage API data
- **Date Mismatches**: Users who appear in both sources, but their "Last Activity At" date from the activity report does not match any activity day in the JSON data

## Requirements

- Python 3.6 or later
- No external dependencies (uses only standard library)

## Installation

Clone the repository and run the script directly:

```bash
git clone https://github.com/mpchenette/compare-copilot-activity-and-usage.git
cd compare-copilot-activity-and-usage
```

## Usage

```bash
python3 analyze_copilot_data.py \
    --json-files usage-data-1.json usage-data-2.json \
    --activity-report seat-activity.csv \
    --output-dir ./output
```

### Command Line Arguments

| Argument | Short | Required | Description |
|----------|-------|----------|-------------|
| `--json-files` | `-j` | Yes | One or more JSON/NDJSON files containing Copilot usage data |
| `--activity-report` | `-a` | Yes | Path to the seat activity report CSV file |
| `--output-dir` | `-o` | No | Directory for output files (default: current directory) |

### Examples

Analyze a single JSON file:

```bash
python3 analyze_copilot_data.py -j usage.json -a activity.csv
```

Analyze multiple JSON files with custom output directory:

```bash
python3 analyze_copilot_data.py \
    -j data-part1.json data-part2.json data-part3.json \
    -a activity-report.csv \
    -o ./analysis-results
```

Use glob patterns to match all JSON files:

```bash
python3 analyze_copilot_data.py -j *.json -a activity.csv
```

## Input Data Formats

### Copilot Usage JSON

The script expects JSON or NDJSON (newline-delimited JSON) files with records in this structure:

```json
{
  "report_start_day": "2025-11-01",
  "report_end_day": "2025-11-30",
  "day": "2025-11-15",
  "user_login": "octocat",
  "totals_by_ide": [
    {
      "ide": "vscode",
      "last_known_ide_version": {"ide_version": "1.85.0"},
      "last_known_plugin_version": {"plugin": "copilot", "plugin_version": "1.138.0"}
    }
  ]
}
```

The script handles:
- Standard NDJSON format (one JSON object per line)
- Concatenated JSON objects without separators (`}{` patterns)
- Large files (50MB+)

### Seat Activity CSV

The activity report CSV should contain these columns:

| Column | Description |
|--------|-------------|
| `Report Time` | Timestamp when the report was generated |
| `Login` | GitHub username |
| `Last Authenticated At` | Last authentication timestamp |
| `Last Activity At` | Last Copilot activity timestamp |
| `Last Surface Used` | IDE/editor where activity occurred (e.g., `vscode/1.85.0`) |

## Output Files

### discrepancies.csv

Contains all users with discrepancies, including all original columns from the activity report plus:

| Column | Description |
|--------|-------------|
| `Discrepancy Type` | Either `missing_from_json` or `date_mismatch` |
| `JSON Activity Dates` | Comma-separated list of activity dates found in JSON (empty for missing users) |
| `Latest JSON Date` | Most recent activity date from JSON data (empty for missing users) |

### summary.txt

A text file containing:
- Report window dates (extracted from JSON data)
- User counts from each data source
- Breakdown of discrepancies by type
- Surface/IDE breakdown for each discrepancy type

## How It Works

1. **Parse JSON Files**: Reads all provided JSON/NDJSON files and extracts user activity records, building a map of each user to their activity dates.

2. **Extract Report Window**: Determines the report date range from the `report_start_day` and `report_end_day` fields in the JSON data.

3. **Analyze Activity Report**: For each user in the CSV with a "Last Activity At" date within the report window:
   - If the user is not found in the JSON data: marked as `missing_from_json`
   - If the user is found but their last activity date does not match any JSON activity date: marked as `date_mismatch`

4. **Generate Output**: Writes the discrepancies to CSV and a summary to a text file.

## License

MIT License
