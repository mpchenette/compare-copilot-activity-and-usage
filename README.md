# Copilot Usage Data Analyzer

A Python script that compares GitHub Copilot usage data from the API against seat activity reports to identify discrepancies.

## Purpose

This tool helps GitHub Enterprise administrators identify inconsistencies between two Copilot data sources:

1. **Copilot Usage API Data** - Detailed per-user, per-timestamp usage records exported as JSON
2. **Seat Activity Reports** - CSV exports showing last activity timestamps per user

The script detects three types of discrepancies:

- **Missing Users**: Users who appear in the seat activity report (with activity within the report window) but have no corresponding records in the usage API data
- **Timestamp Mismatches**: Users who appear in both sources, but their "Last Activity At" timestamp from the activity report does not match (within 24 hours) any timestamp in the JSON data
- **IDE Mismatches**: Users where the timestamp matches exactly, but the IDE/surface differs between sources

## 72-Hour Buffer

The script automatically applies a 72-hour buffer to the analysis window because:
- The Copilot usage JSON export has a known delay before data populates
- Activity from the last 72 hours may not yet appear in the JSON export
- This reduces false positives from timing delays

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
python3 analyze_copilot_data.py --data-dir ./my-data
```

### Command Line Arguments

| Argument | Short | Required | Description |
|----------|-------|----------|-------------|
| `--data-dir` | `-d` | Yes | Directory containing JSON files and CSV activity report |

The script will automatically find:
- All `*.json` files in the directory (Copilot usage exports)
- The first `*.csv` file in the directory (seat activity report)

Output files (`discrepancies.csv` and `summary.txt`) are written to the same directory.

### Example

```bash
# Create a directory with your data files
mkdir my-analysis
cp usage-*.json my-analysis/
cp seat-activity.csv my-analysis/

# Run the analysis
python3 analyze_copilot_data.py --data-dir ./my-analysis

# View results
cat my-analysis/summary.txt
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
  "sampled_at": "2025-11-15T14:30:00Z",
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

Contains all users with discrepancies, including:

| Column | Description |
|--------|-------------|
| `Login` | GitHub username |
| `Last Activity At` | Timestamp from activity report |
| `Last Surface Used` | IDE/surface from activity report |
| `JSON Timestamps` | Comma-separated list of timestamps found in JSON |
| `Latest JSON Timestamp` | Most recent timestamp from JSON (empty for missing users) |
| `JSON IDE/Version` | IDE info from JSON on exact match (for IDE mismatch analysis) |
| `Issue` | `missing_from_json`, `timestamp_mismatch`, or `ide_mismatch` |

### summary.txt

A comprehensive text file containing:
- Report window dates (with 72-hour buffer applied)
- User counts from each data source
- Breakdown of discrepancies by type and surface/IDE
- Pattern analysis:
  - Discrepancies by date
  - Discrepancies by day of week
  - Timestamp gap analysis
  - Discrepancy rate by user activity level
- Key insights about data consistency

## How It Works

1. **Parse JSON Files**: Reads all JSON/NDJSON files from the data directory and extracts user activity records, building a map of each user to their timestamps and IDE info.

2. **Extract Report Window**: Determines the report date range from the JSON data, then applies a 72-hour buffer to exclude recent activity that may not have populated yet.

3. **Analyze Activity Report**: For each user in the CSV with a "Last Activity At" timestamp within the effective report window:
   - If the user is not found in the JSON data → `missing_from_json`
   - If the user is found but their timestamp doesn't match within 24 hours → `timestamp_mismatch`
   - If the timestamp matches exactly but the IDE differs → `ide_mismatch`

4. **Pattern Analysis**: Analyzes discrepancy patterns by date, day of week, and user activity level to identify systemic issues.

5. **Generate Output**: Writes detailed discrepancies to CSV and a comprehensive summary to a text file.

## IDE Matching Logic

The script uses intelligent IDE matching to reduce false positives:

- **JetBrains normalization**: All JetBrains IDEs (`JetBrains-IU`, `JetBrains-PY`, etc.) map to `intellij` (what the JSON reports)
- **Version comparison**: Checks if the major version number matches
- **Plugin tolerance**: Allows matches where JSON is missing plugin version info
- **Neovim exclusion**: Skips Neovim users (not expected in JSON export)

## License

MIT License
