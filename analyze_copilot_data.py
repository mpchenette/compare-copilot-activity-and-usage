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
import argparse
from datetime import datetime
from collections import defaultdict


def parse_json_files(json_files):
    """
    Parse multiple JSON/NDJSON files and extract key fields.
    
    Args:
        json_files: List of paths to JSON files
        
    Returns:
        Tuple of (list of row dicts, user_dates dict, report_start_day, report_end_day)
        user_dates maps user_login -> set of activity dates (YYYY-MM-DD)
    """
    rows = []
    user_dates = defaultdict(set)  # Track all activity dates per user
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
                    
                    # Track activity dates per user
                    if user_login and day:
                        user_dates[user_login].add(day)
                    
                    # Extract IDE info from totals_by_ide array
                    totals_by_ide = record.get('totals_by_ide', [])
                    
                    if totals_by_ide:
                        for ide_info in totals_by_ide:
                            ide = ide_info.get('ide', '')
                            
                            # Get IDE version
                            last_known_ide_version = ide_info.get('last_known_ide_version', {})
                            ide_version = last_known_ide_version.get('ide_version', '')
                            
                            # Get plugin info
                            last_known_plugin_version = ide_info.get('last_known_plugin_version', {})
                            plugin = last_known_plugin_version.get('plugin', '')
                            plugin_version = last_known_plugin_version.get('plugin_version', '')
                            
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
    
    return rows, user_dates, report_start, report_end


def find_discrepancies(distilled_rows, user_dates, activity_report_path, report_start, report_end):
    """
    Find users who are active in the activity report within the report window
    but have discrepancies with the JSON usage data.
    
    Args:
        distilled_rows: List of row dicts from distilled data
        user_dates: Dict mapping user_login -> set of activity dates from JSON
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
    fieldnames = None
    stats = {
        'total_activity_users': 0,
        'users_with_activity': 0,
        'users_active_in_window': 0,
        'missing_count': 0,
        'date_mismatch_count': 0,
        'missing_surface_breakdown': defaultdict(int),
        'date_mismatch_surface_breakdown': defaultdict(int)
    }
    
    with open(activity_report_path, 'r') as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames)  # Preserve original column names
        
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
                        surface = last_surface.split('/')[0] if last_surface else 'unknown'
                        
                        # Check if NOT in JSON data at all
                        if login not in distilled_users:
                            stats['missing_count'] += 1
                            stats['missing_surface_breakdown'][surface] += 1
                            
                            # Create row with discrepancy info
                            discrepancy_row = dict(row)
                            discrepancy_row['Discrepancy Type'] = 'missing_from_json'
                            discrepancy_row['JSON Activity Dates'] = ''
                            discrepancy_row['Latest JSON Date'] = ''
                            all_discrepancies.append(discrepancy_row)
                        
                        # Check if user IS in JSON data but date doesn't match
                        elif login in user_dates:
                            user_json_dates = user_dates[login]
                            if last_activity_date not in user_json_dates:
                                stats['date_mismatch_count'] += 1
                                stats['date_mismatch_surface_breakdown'][surface] += 1
                                
                                # Create row with discrepancy info
                                discrepancy_row = dict(row)
                                discrepancy_row['Discrepancy Type'] = 'date_mismatch'
                                discrepancy_row['JSON Activity Dates'] = ', '.join(sorted(user_json_dates))
                                discrepancy_row['Latest JSON Date'] = max(user_json_dates) if user_json_dates else ''
                                all_discrepancies.append(discrepancy_row)
                            
                except ValueError:
                    continue
    
    # Output fieldnames include discrepancy columns
    output_fieldnames = fieldnames + ['Discrepancy Type', 'JSON Activity Dates', 'Latest JSON Date']
    
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
        
        f.write("\n--- Date Mismatches (in both but dates don't match) ---\n")
        f.write(f"Count: {stats['date_mismatch_count']:,}\n")
        f.write("Breakdown by Surface Type:\n")
        for surface, count in sorted(stats['date_mismatch_surface_breakdown'].items(), key=lambda x: -x[1]):
            f.write(f"  {surface}: {count:,}\n")
    
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
    rows, user_dates, report_start, report_end = parse_json_files(args.json_files)
    
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
        user_dates,
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
    print(f"\nTotal discrepancies: {stats['missing_count'] + stats['date_mismatch_count']:,}")
    print(f"\n  Missing users (in activity report but NOT in JSON): {stats['missing_count']:,}")
    print("  Breakdown by surface:")
    for surface, count in sorted(stats['missing_surface_breakdown'].items(), key=lambda x: -x[1]):
        print(f"    {surface}: {count:,}")
    print(f"\n  Date mismatches (in both but dates don't align): {stats['date_mismatch_count']:,}")
    print("  Breakdown by surface:")
    for surface, count in sorted(stats['date_mismatch_surface_breakdown'].items(), key=lambda x: -x[1]):
        print(f"    {surface}: {count:,}")
    print("\nOutput files:")
    print(f"  - {discrepancies_csv_path}")
    print(f"  - {summary_path}")
    
    return 0


if __name__ == '__main__':
    exit(main())
