#!/usr/bin/env python3
"""
Copilot Usage Data Analyzer

This script performs two main functions:
1. Ingests multiple JSON/NDJSON files containing Copilot usage data and combines 
   them into a single distilled CSV with key fields.
2. Compares the distilled data against an activity report CSV to identify 
   discrepancies - users who show activity in the activity report within the 
   report window but don't appear in the distilled usage data.

Usage:
    python analyze_copilot_data.py --json-files <file1.json> <file2.json> ... \
                                   --activity-report <activity.csv> \
                                   --output-dir <output_directory>

Output Files:
    - discrepancies.csv: Users with discrepancies (missing from JSON or date mismatches)
    - summary.txt: Summary statistics of the analysis
"""

import json
import csv
import os
import re
import argparse
from datetime import datetime, timedelta
from collections import defaultdict


# Mapping from activity report surface/IDE names to JSON IDE names
# Note: JSON export reports ALL JetBrains IDEs as 'intellij'
SURFACE_TO_JSON_IDE = {
    'vscode': 'vscode',
    'vscode-chat': 'vscode',
    'jetbrains-iu': 'intellij',
    'jetbrains-py': 'intellij',  # PyCharm reports as intellij
    'jetbrains-cl': 'intellij',  # CLion reports as intellij
    'jetbrains-go': 'intellij',  # GoLand reports as intellij
    'jetbrains-rm': 'intellij',  # RubyMine reports as intellij
    'jetbrains-ws': 'intellij',  # WebStorm reports as intellij
    'jetbrains-rd': 'intellij',  # Rider reports as intellij
    'jetbrains-ps': 'intellij',  # PhpStorm reports as intellij
    'jetbrains-db': 'intellij',  # DataGrip reports as intellij
    'jetbrains-jbc': 'intellij', # JetBrains Client reports as intellij
    'jetbrains-ai': 'intellij',  # JetBrains AI Assistant reports as intellij
    'jetbrains-pc': 'intellij',  # PyCharm Community reports as intellij
    'visualstudio': 'visualstudio',
    'neovim': 'neovim',
    'vim': 'vim',
    'emacs': 'emacs',
    'xcode': 'xcode',
    'unknown': 'unknown',
}

# Mapping for plugin names (activity report -> JSON)
# Empty - keep plugin names as-is since copilot-chat and copilot are different extensions
PLUGIN_NAME_MAP = {}

# Pattern for VS Code Copilot extension versions (0.XX.X format)
VSCODE_VERSION_PATTERN = re.compile(r'^0\.\d{1,2}\.\d+$')


def normalize_timestamp(ts):
    """
    Normalize a timestamp by removing milliseconds for comparison.
    
    Args:
        ts: ISO timestamp string (e.g., '2025-12-13T11:35:21.5230000Z')
        
    Returns:
        Normalized timestamp without milliseconds (e.g., '2025-12-13T11:35:21Z')
    """
    if not ts:
        return None
    # Remove milliseconds (.XXXXXXX) if present
    if '.' in ts:
        return ts.split('.')[0] + 'Z'
    return ts


def find_closest_timestamp(report_ts, json_timestamps, tolerance_hours=1):
    """
    Find the closest JSON timestamp to the report timestamp.
    
    Args:
        report_ts: Normalized timestamp from activity report (e.g., '2025-12-13T11:35:21Z')
        json_timestamps: Set of normalized timestamps from JSON
        tolerance_hours: Maximum hours difference to consider a match (default 1)
        
    Returns:
        Tuple of (closest_timestamp, is_within_tolerance, time_diff_seconds)
        - closest_timestamp: The closest JSON timestamp, or None if no timestamps
        - is_within_tolerance: True if within tolerance_hours
        - time_diff_seconds: Absolute difference in seconds (positive = JSON is later)
    """
    if not report_ts or not json_timestamps:
        return None, False, None
    
    try:
        # Parse report timestamp
        report_dt = datetime.strptime(report_ts.rstrip('Z'), '%Y-%m-%dT%H:%M:%S')
        
        closest_ts = None
        min_diff = None
        
        for json_ts in json_timestamps:
            try:
                json_dt = datetime.strptime(json_ts.rstrip('Z'), '%Y-%m-%dT%H:%M:%S')
                diff = abs((json_dt - report_dt).total_seconds())
                
                if min_diff is None or diff < min_diff:
                    min_diff = diff
                    closest_ts = json_ts
            except ValueError:
                continue
        
        if closest_ts is None:
            return None, False, None
        
        tolerance_seconds = tolerance_hours * 3600
        is_within_tolerance = min_diff <= tolerance_seconds
        
        return closest_ts, is_within_tolerance, min_diff
        
    except ValueError:
        return None, False, None


def normalize_surface_to_json_format(last_surface):
    """
    Convert an activity report surface string to the JSON format for comparison.
    
    Activity report format: surface/ide_version/plugin/plugin_version
    JSON format: ide/ide_version/plugin/plugin_version
    
    Args:
        last_surface: Full surface string from activity report 
                      (e.g., 'JetBrains-IU/252.25557.131/copilot-intellij/1.5.57-243')
        
    Returns:
        Normalized string in JSON format, or None if can't be normalized
    """
    if not last_surface:
        return None
    
    parts = last_surface.split('/')
    if not parts:
        return None
    
    surface = parts[0].lower()
    
    # Check if any part matches VS Code extension version pattern (0.XX.X)
    # This indicates VS Code even if surface says 'unknown'
    for part in parts[1:]:
        if VSCODE_VERSION_PATTERN.match(part):
            surface = 'vscode'
            break
    
    # Map surface name to JSON IDE name
    json_ide = SURFACE_TO_JSON_IDE.get(surface, surface)
    
    # Build normalized string
    normalized_parts = [json_ide]
    
    # Add remaining parts (ide_version, plugin, plugin_version)
    for i, part in enumerate(parts[1:], 1):
        # Skip 'GitHubCopilotChat' or similar intermediate identifiers
        if part.lower() in ('githubcopilotchat', 'githubcopilot'):
            continue
        # Map plugin names if this looks like a plugin name
        if part.lower() in PLUGIN_NAME_MAP:
            normalized_parts.append(PLUGIN_NAME_MAP[part.lower()])
        else:
            normalized_parts.append(part)
    
    return '/'.join(normalized_parts)


def ide_matches_partial(report_surface, json_ide):
    """
    Check if two IDE strings match, allowing for partial matches.
    
    A partial match means the IDE name and version match, even if one
    has additional plugin info that the other lacks.
    
    Examples that should match:
        - 'JetBrains-IU/233.15026.9/' and 'intellij/233.15026.9/copilot-intellij/1.5.8.5775'
        - 'vscode/1.105.1/' and 'vscode/1.105.1/copilot/1.387.0'
    
    Args:
        report_surface: Surface string from activity report
        json_ide: IDE string from JSON export
        
    Returns:
        True if IDE name and version match (partial match allowed)
    """
    if not report_surface or not json_ide:
        return False
    
    # Parse both into parts
    report_parts = report_surface.lower().split('/')
    json_parts = json_ide.lower().split('/')
    
    if len(report_parts) < 1 or len(json_parts) < 1:
        return False
    
    # Get IDE names
    report_ide = report_parts[0]
    json_ide_name = json_parts[0]
    
    # Normalize report IDE name to JSON format
    report_ide_normalized = SURFACE_TO_JSON_IDE.get(report_ide, report_ide)
    
    # Check if IDE names match
    if report_ide_normalized != json_ide_name:
        return False
    
    # Get versions (second part if available)
    report_version = report_parts[1] if len(report_parts) > 1 else ''
    json_version = json_parts[1] if len(json_parts) > 1 else ''
    
    # Strip trailing empty parts
    report_version = report_version.strip()
    json_version = json_version.strip()
    
    # If both have versions, they must match
    if report_version and json_version:
        return report_version == json_version
    
    # If only one has a version, still consider it a match (partial)
    # This handles cases where one source has more detail than the other
    return True


def parse_json_files(json_files):
    """
    Parse multiple JSON/NDJSON files and extract key fields.
    
    Args:
        json_files: List of paths to JSON files
        
    Returns:
        Tuple of (list of row dicts, user_timestamps dict, report_start_day, report_end_day)
        user_timestamps maps user_login -> dict with:
            'timestamps': set of normalized timestamps
            'timestamp_to_ide': dict mapping timestamp -> IDE string
    """
    rows = []
    user_timestamps = defaultdict(lambda: {'timestamps': set(), 'timestamp_to_ide': {}})
    report_start = None
    report_end = None
    
    for filepath in json_files:
        print(f"Processing: {filepath}")
        
        with open(filepath, 'r') as f:
            content = f.read()
            
        # Handle NDJSON format (newline-delimited JSON)
        lines = content.strip().split('\n')
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Handle case where multiple JSON objects are concatenated on one line
            if '}{' in line:
                parts = line.split('}{')
                json_strings = []
                for i, part in enumerate(parts):
                    if i == 0:
                        json_strings.append(part + '}')
                    elif i == len(parts) - 1:
                        json_strings.append('{' + part)
                    else:
                        json_strings.append('{' + part + '}')
            else:
                json_strings = [line]
            
            for json_str in json_strings:
                try:
                    record = json.loads(json_str)
                    
                    # Extract base fields
                    record_report_start = record.get('report_start_day', '')
                    record_report_end = record.get('report_end_day', '')
                    day = record.get('day', '')
                    user_login = record.get('user_login', '')
                    
                    # Track report window (should be consistent across all records)
                    if report_start is None and record_report_start:
                        report_start = record_report_start
                        report_end = record_report_end
                    
                    # Extract IDE info from totals_by_ide array
                    totals_by_ide = record.get('totals_by_ide', [])
                    
                    if totals_by_ide:
                        for ide_info in totals_by_ide:
                            ide = ide_info.get('ide', '')
                            
                            # Get IDE version and sampled_at timestamp
                            last_known_ide_version = ide_info.get('last_known_ide_version', {})
                            ide_version = last_known_ide_version.get('ide_version', '')
                            sampled_at = last_known_ide_version.get('sampled_at', '')
                            
                            # Get plugin info
                            last_known_plugin_version = ide_info.get('last_known_plugin_version', {})
                            plugin = last_known_plugin_version.get('plugin', '')
                            plugin_version = last_known_plugin_version.get('plugin_version', '')
                            plugin_sampled_at = last_known_plugin_version.get('sampled_at', '')
                            
                            # Build IDE string matching activity report format
                            if user_login and ide:
                                parts = [ide.lower()]
                                if ide_version:
                                    parts.append(ide_version)
                                if plugin:
                                    parts.append(plugin)
                                if plugin_version:
                                    parts.append(plugin_version)
                                ide_str = '/'.join(parts)
                                
                                # Track timestamps and map to IDE strings
                                # Use plugin sampled_at if available, otherwise IDE sampled_at
                                ts = normalize_timestamp(plugin_sampled_at or sampled_at)
                                if ts:
                                    user_timestamps[user_login]['timestamps'].add(ts)
                                    user_timestamps[user_login]['timestamp_to_ide'][ts] = ide_str
                            
                            rows.append({
                                'report_start_day': record_report_start,
                                'report_end_day': record_report_end,
                                'day': day,
                                'user_login': user_login,
                                'ide': ide,
                                'ide_version': ide_version,
                                'plugin': plugin,
                                'plugin_version': plugin_version
                            })
                    else:
                        # No IDE info, still add the row with empty IDE fields
                        rows.append({
                            'report_start_day': record_report_start,
                            'report_end_day': record_report_end,
                            'day': day,
                            'user_login': user_login,
                            'ide': '',
                            'ide_version': '',
                            'plugin': '',
                            'plugin_version': ''
                        })
                        
                except json.JSONDecodeError as e:
                    print(f"  Warning: Error parsing JSON in {filepath}: {e}")
                    continue
    
    return rows, user_timestamps, report_start, report_end


def find_discrepancies(distilled_rows, user_timestamps, activity_report_path, report_start, report_end):
    """
    Find users who are active in the activity report within the report window
    but have discrepancies with the JSON usage data.
    
    Args:
        distilled_rows: List of row dicts from distilled data
        user_timestamps: Dict mapping user_login -> dict with timestamps and IDE info
        activity_report_path: Path to activity report CSV
        report_start: Report start date (YYYY-MM-DD)
        report_end: Report end date (YYYY-MM-DD)
        
    Returns:
        Tuple of (list of all discrepancy dicts, fieldnames list, stats dict)
    """
    # Get unique users from distilled data
    distilled_users = set(row['user_login'] for row in distilled_rows)
    
    # Parse report dates
    report_start_dt = datetime.strptime(report_start, '%Y-%m-%d')
    report_end_dt = datetime.strptime(report_end, '%Y-%m-%d')
    
    # Find discrepancies
    all_discrepancies = []
    stats = {
        'total_activity_users': 0,
        'users_with_activity': 0,
        'users_active_in_window': 0,
        'missing_count': 0,
        'timestamp_mismatch_count': 0,
        'ide_mismatch_count': 0,
        'missing_surface_breakdown': defaultdict(int),
        'timestamp_mismatch_surface_breakdown': defaultdict(int)
    }
    
    with open(activity_report_path, 'r') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            stats['total_activity_users'] += 1
            
            login = row.get('Login', '')
            last_activity_at = row.get('Last Activity At', '')
            last_surface = row.get('Last Surface Used', '')
            
            if last_activity_at:
                stats['users_with_activity'] += 1
                
                try:
                    last_activity_date = last_activity_at[:10]  # YYYY-MM-DD
                    last_activity_dt = datetime.strptime(last_activity_date, '%Y-%m-%d')
                    
                    # Check if within report window
                    if report_start_dt <= last_activity_dt <= report_end_dt:
                        stats['users_active_in_window'] += 1
                        
                        # Track surface type
                        if last_surface:
                            parts = last_surface.split('/')
                            surface = parts[0]
                        else:
                            surface = 'unknown'
                        
                        # Skip Neovim users - they are not expected to appear in JSON
                        if surface.lower() == 'neovim':
                            continue
                        
                        # Normalize activity report surface and timestamp for comparison
                        normalized_surface = normalize_surface_to_json_format(last_surface)
                        normalized_report_ts = normalize_timestamp(last_activity_at)
                        
                        # Check if NOT in JSON data at all
                        if login not in distilled_users:
                            stats['missing_count'] += 1
                            stats['missing_surface_breakdown'][surface] += 1
                            
                            discrepancy_row = {
                                'Login': login,
                                'Status': 'Missing from JSON',
                                'Last Activity At': last_activity_at,
                                'Latest Export Activity': '',
                                'Report Surface': last_surface,
                                'JSON IDE': '',
                                'Report Generated': row.get('Report Time', '')
                            }
                            all_discrepancies.append(discrepancy_row)
                        
                        # User is in JSON - check timestamp match
                        elif login in user_timestamps:
                            user_data = user_timestamps[login]
                            json_timestamps = user_data['timestamps']
                            timestamp_to_ide = user_data['timestamp_to_ide']
                            
                            # Get most recent JSON timestamp for display
                            most_recent_json_ts = max(json_timestamps) if json_timestamps else ''
                            
                            # Find closest timestamp and check if within 1 day tolerance
                            closest_ts, within_tolerance, time_diff = find_closest_timestamp(
                                normalized_report_ts, json_timestamps, tolerance_hours=24
                            )
                            
                            # Check for exact timestamp match (for IDE comparison)
                            exact_match = normalized_report_ts in json_timestamps
                            
                            if not within_tolerance:
                                # No timestamp within 1 hour - flag as mismatch
                                stats['timestamp_mismatch_count'] += 1
                                stats['timestamp_mismatch_surface_breakdown'][surface] += 1
                                
                                discrepancy_row = {
                                    'Login': login,
                                    'Status': 'Timestamp Mismatch',
                                    'Last Activity At': last_activity_at,
                                    'Latest Export Activity': most_recent_json_ts,
                                    'Report Surface': last_surface,
                                    'JSON IDE': '',
                                    'Report Generated': row.get('Report Time', '')
                                }
                                all_discrepancies.append(discrepancy_row)
                            elif exact_match:
                                # Exact timestamp match - check IDE
                                json_ide = timestamp_to_ide.get(normalized_report_ts, '')
                                ide_matches = ide_matches_partial(last_surface, json_ide)
                                
                                if not ide_matches:
                                    stats['ide_mismatch_count'] += 1
                                    
                                    discrepancy_row = {
                                        'Login': login,
                                        'Status': 'IDE Mismatch',
                                        'Last Activity At': last_activity_at,
                                        'Latest Export Activity': most_recent_json_ts,
                                        'Report Surface': last_surface,
                                        'JSON IDE': json_ide,
                                        'Report Generated': row.get('Report Time', '')
                                    }
                                    all_discrepancies.append(discrepancy_row)
                            # else: within tolerance but not exact - no discrepancy
                            
                except ValueError:
                    continue
    
    # Define structured output columns
    output_fieldnames = [
        'Login',
        'Status',
        'Last Activity At',
        'Latest Export Activity',
        'Report Surface',
        'JSON IDE',
        'Report Generated'
    ]
    
    return all_discrepancies, output_fieldnames, stats


def write_discrepancies_csv(discrepancies, fieldnames, output_path):
    """
    Write discrepancy data to CSV, preserving all original activity report columns.
    
    Args:
        discrepancies: List of discrepancy dicts (full rows from activity report)
        fieldnames: List of column names from original activity report
        output_path: Path to output CSV file
    """
    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(discrepancies)
    
    print(f"Wrote {len(discrepancies)} discrepancies to {output_path}")


def write_summary(stats, report_start, report_end, distilled_users_count, output_path):
    """
    Write summary statistics to a text file.
    
    Args:
        stats: Statistics dict
        report_start: Report start date
        report_end: Report end date
        distilled_users_count: Number of unique users in JSON data
        output_path: Path to output summary file
    """
    with open(output_path, 'w') as f:
        f.write("=" * 60 + "\n")
        f.write("COPILOT USAGE DATA ANALYSIS SUMMARY\n")
        f.write("=" * 60 + "\n\n")
        
        f.write(f"Report Window: {report_start} to {report_end}\n\n")
        
        f.write("--- JSON Usage Data ---\n")
        f.write(f"Unique users in JSON data: {distilled_users_count:,}\n\n")
        
        f.write("--- Activity Report ---\n")
        f.write(f"Total users in activity report: {stats['total_activity_users']:,}\n")
        f.write(f"Users with activity data: {stats['users_with_activity']:,}\n")
        f.write(f"Users active within report window: {stats['users_active_in_window']:,}\n\n")
        
        f.write("--- Missing Users (in activity report but NOT in JSON) ---\n")
        f.write(f"Count: {stats['missing_count']:,}\n")
        f.write("Breakdown by Surface Type:\n")
        for surface, count in sorted(stats['missing_surface_breakdown'].items(), key=lambda x: -x[1]):
            f.write(f"  {surface}: {count:,}\n")
        
        f.write("\n--- Timestamp Mismatches (user in JSON but timestamp not found) ---\n")
        f.write(f"Count: {stats['timestamp_mismatch_count']:,}\n")
        f.write("Breakdown by Surface Type:\n")
        for surface, count in sorted(stats['timestamp_mismatch_surface_breakdown'].items(), key=lambda x: -x[1]):
            f.write(f"  {surface}: {count:,}\n")
        
        f.write("\n--- IDE Mismatches (timestamp matches but IDE differs) ---\n")
        f.write(f"Count: {stats['ide_mismatch_count']:,}\n")
    
    print(f"Wrote summary to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description='Analyze Copilot usage data and compare against activity reports.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze JSON files and compare with activity report
  python analyze_copilot_data.py \\
      --json-files data1.json data2.json data3.json \\
      --activity-report activity.csv \\
      --output-dir ./output

  # Process all JSON files in a directory
  python analyze_copilot_data.py \\
      --json-files *.json \\
      --activity-report activity.csv
        """
    )
    
    parser.add_argument(
        '--json-files', '-j',
        nargs='+',
        required=True,
        help='One or more JSON/NDJSON files containing Copilot usage data'
    )
    
    parser.add_argument(
        '--activity-report', '-a',
        required=True,
        help='Path to the activity report CSV file'
    )
    
    parser.add_argument(
        '--output-dir', '-o',
        default='.',
        help='Directory for output files (default: current directory)'
    )
    
    args = parser.parse_args()
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Output file paths
    discrepancies_csv_path = os.path.join(args.output_dir, 'discrepancies.csv')
    summary_path = os.path.join(args.output_dir, 'summary.txt')
    
    print("\n" + "=" * 60)
    print("COPILOT USAGE DATA ANALYZER")
    print("=" * 60 + "\n")
    
    # Step 1: Parse JSON files
    print("Step 1: Parsing JSON files...")
    rows, user_timestamps, report_start, report_end = parse_json_files(args.json_files)
    
    if not rows:
        print("Error: No data extracted from JSON files.")
        return 1
    
    if not report_start or not report_end:
        print("Error: Could not determine report window from JSON data.")
        return 1
    
    print(f"\nReport window: {report_start} to {report_end}")
    print(f"Total records extracted: {len(rows):,}")
    
    # Get unique users count
    distilled_users = set(row['user_login'] for row in rows)
    print(f"Unique users in JSON data: {len(distilled_users):,}")
    
    # Step 2: Find discrepancies
    print("\nStep 2: Finding discrepancies with activity report...")
    all_discrepancies, output_fieldnames, stats = find_discrepancies(
        rows,
        user_timestamps,
        args.activity_report, 
        report_start, 
        report_end
    )
    
    # Step 3: Write outputs
    print("\nStep 3: Writing output files...")
    write_discrepancies_csv(all_discrepancies, output_fieldnames, discrepancies_csv_path)
    write_summary(stats, report_start, report_end, len(distilled_users), summary_path)
    
    # Print summary to console
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Report Window: {report_start} to {report_end}")
    print(f"Unique users in JSON data: {len(distilled_users):,}")
    print(f"Total users in activity report: {stats['total_activity_users']:,}")
    print(f"Users active within report window: {stats['users_active_in_window']:,}")
    
    total_discrepancies = stats['missing_count'] + stats['timestamp_mismatch_count'] + stats['ide_mismatch_count']
    print(f"\nTotal discrepancies: {total_discrepancies:,}")
    
    print(f"\n  Missing users (in activity report but NOT in JSON): {stats['missing_count']:,}")
    print("  Breakdown by surface:")
    for surface, count in sorted(stats['missing_surface_breakdown'].items(), key=lambda x: -x[1]):
        print(f"    {surface}: {count:,}")
    
    print(f"\n  Timestamp mismatches (user in JSON but timestamp not found): {stats['timestamp_mismatch_count']:,}")
    print("  Breakdown by surface:")
    for surface, count in sorted(stats['timestamp_mismatch_surface_breakdown'].items(), key=lambda x: -x[1]):
        print(f"    {surface}: {count:,}")
    
    print(f"\n  IDE mismatches (timestamp matches but IDE differs): {stats['ide_mismatch_count']:,}")
    
    print("\nOutput files:")
    print(f"  - {discrepancies_csv_path}")
    print(f"  - {summary_path}")
    
    return 0


if __name__ == '__main__':
    exit(main())
