# Copilot Usage Data Analyzer

A Python script that compares GitHub Copilot usage data from the API against seat activity reports to identify discrepancies.

## Purpose

This tool helps GitHub Enterprise administrators identify inconsistencies between two Copilot data sources:

1. **Copilot Usage API Data** - Detailed per-user, per-timestamp usage records exported as JSON
2. **Seat Activity Reports** - CSV exports showing last activity timestamps per user

The script detects two types of discrepancies:

- **Absent Users**: Users who appear in the seat activity report (with activity within the report window, on supported versions) but have no corresponding records in the usage API data
- **Stale Users**: Users who appear in both sources, but their "Last Activity At" timestamp from the activity report is >24 hours different from any timestamp in the JSON data

## 96-Hour Buffer

The script automatically applies a 96-hour buffer to the analysis window because:
- The Copilot usage JSON export has a known delay before data populates
- Activity from the last 96 hours may not yet appear in the JSON export
- This reduces false positives from timing delays

## Supported Version Filtering

The script automatically filters out users on unsupported IDE/extension versions that are not expected to appear in the JSON export:

- **VS Code**: Requires VS Code 1.101+ and copilot-chat 0.28.0+
- **JetBrains**: Requires build 242+ (2024.2.x) and copilot-intellij 1.5.52+
- **Visual Studio**: Requires VS 17.14.13+ and extension 18.0.471+

Users on older versions are excluded from discrepancy analysis since they won't appear in the JSON export by design.

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

Output files are written to an `output/` subdirectory within the data directory.

### Example

```bash
# Create a directory with your data files
mkdir my-analysis
cp usage-*.json my-analysis/
cp seat-activity.csv my-analysis/

# Run the analysis
python3 analyze_copilot_data.py --data-dir ./my-analysis

# View results (output is in the 'output' subdirectory)
cat my-analysis/output/*-summary.md
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
| `Last Surface Used` | IDE/editor where activity occurred (e.g., `vscode/1.85.0/copilot-chat/0.29.1`) |

## Output Files

### discrepancies.csv

Contains all users with discrepancies, including:

| Column | Description |
|--------|-------------|
| `Login` | GitHub username |
| `Status` | `Missing from JSON` (absent) or `Timestamp Mismatch` (stale) |
| `Last Activity At` | Timestamp from activity report |
| `Latest Export Activity` | Most recent timestamp from JSON (empty for absent users) |
| `Report Surface` | IDE/surface from activity report |
| `JSON IDE` | IDE from JSON data |
| `Report Generated` | When the activity report was generated |

### {customer}-summary.md

A comprehensive Markdown report containing:

- **Dashboard JSON**: Report window dates with 96-hour buffer applied
- **Activity Report**: User counts and version support breakdown
- **Analysis**:
  - % active users affected
  - % of events missing (absent vs stale breakdown)
  - IDE breakdown (VS Code vs JetBrains with absent/stale rates)
- **Patterns**:
  - Absent/Stale events by extension version
  - Discrepancies by date (ASCII chart)
  - Timestamp gap analysis
  - Interaction count distributions
  - Discrepancy rate by activity level

## How It Works

1. **Parse JSON Files**: Reads all JSON/NDJSON files from the data directory and extracts user activity records, building a map of each user to their timestamps and IDE info.

2. **Extract Report Window**: Determines the report date range from the JSON data, then applies a 96-hour buffer to exclude recent activity that may not have populated yet.

3. **Filter Unsupported Versions**: Excludes users on IDE/extension versions that don't support usage metrics export.

4. **Analyze Activity Report**: For each user in the CSV with a "Last Activity At" timestamp within the effective report window:
   - If the user is not found in the JSON data → `absent`
   - If the user is found but their timestamp doesn't match within 24 hours → `stale`

5. **Pattern Analysis**: Analyzes discrepancy patterns by date, IDE, extension version, and user activity level to identify systemic issues.

6. **Generate Output**: Writes detailed discrepancies to CSV and a comprehensive summary to Markdown.

## IDE Matching Logic

The script uses intelligent IDE matching:

- **JetBrains normalization**: All JetBrains IDEs (`JetBrains-IU`, `JetBrains-PY`, etc.) map to `intellij`
- **Unknown surface handling**: `unknown/GitHubCopilotChat/*` patterns are recognized as VS Code
- **Neovim exclusion**: Skips Neovim users (not expected in JSON export)

## License

MIT License
