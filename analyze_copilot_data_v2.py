#!/usr/bin/env python3
"""
Copilot Usage Data Analyzer (v2)

This script compares a single activity report CSV against JSON usage data
for the corresponding date. It finds JSON files where the CSV's report date
falls within their report window and compares user data directly.

Usage:
    python analyze_copilot_data_v2.py --csv <activity.csv> --json-dir <json_directory>

Output Files:
    - discrepancies.csv: Users with discrepancies (missing from JSON or mismatches)
    - summary.md: Summary statistics of the analysis
"""

import json
import csv
import os
import re
import glob
import argparse
from datetime import datetime
from collections import defaultdict


# IDE categories for grouping surfaces in reports
IDE_CATEGORIES = {
    'vscode': 'VS Code',
    'unknown': 'VS Code',  # unknown/GitHubCopilotChat is VS Code
    'intellij': 'JetBrains',
    'jetbrains-iu': 'JetBrains',
    'jetbrains-py': 'JetBrains',
    'jetbrains-pc': 'JetBrains',
    'jetbrains-ws': 'JetBrains',
    'jetbrains-go': 'JetBrains',
    'jetbrains-rm': 'JetBrains',
    'jetbrains-cl': 'JetBrains',
    'jetbrains-rd': 'JetBrains',
    'jetbrains-jbc': 'JetBrains',
    'jetbrains-ai': 'JetBrains',
    'jetbrains-ic': 'JetBrains',
    'visualstudio': 'Visual Studio',
    'zed': 'Zed',
    'vim': 'Vim',
    'neovim': 'Neovim',
}

# Non-IDE surfaces (web, CLI, mobile, platform features)
# These surfaces don't have version info and won't appear in JSON export
# JSON data today only includes IDE usage
NON_IDE_SURFACES = {
    'copilot-chat',           # GitHub.com web chat
    'copilot-chat-platform',  # Platform chat
    'copilot-cli',            # CLI tool
    'copilot_pr_review',      # PR review feature
    'copilot-pr-reviews',     # PR reviews (alternate name)
    'copilot-developer',      # Developer feature
    'copilot-mobile-ios',     # iOS mobile app
    'copilot-mobile-android', # Android mobile app
    'github_spark',           # GitHub Spark
    'github.com',             # GitHub.com
    'none',                   # No surface
    '/unknown',               # Invalid format
    '',                       # Empty
}

# Minimum supported versions for Copilot usage metrics
# Users on older versions will NOT appear in the JSON export by design
# Source: https://docs.github.com/en/enterprise-cloud@latest/copilot/rolling-out-github-copilot-at-scale/analyzing-usage-over-time-with-the-copilot-metrics-api
MIN_VERSIONS = {
    'vscode': {
        'ide': (1, 101),           # VS Code 1.101
        'extension': (0, 28, 0),    # copilot-chat 0.28.0
    },
    'visualstudio': {
        'ide': (17, 14, 13),        # Visual Studio 17.14.13
        'extension': (18, 0, 471),  # 18.0.471.29466
    },
    'jetbrains': {
        # JetBrains 2024.2.6 = build 242.xxxxx
        # Build number format: YYR.xxxxx where YY=year-2000, R=release (1,2,3)
        # 2024.2.x → 242.xxxxx, 2024.3.x → 243.xxxxx, 2025.1.x → 251.xxxxx
        'ide_build': 242,           # JetBrains 2024.2.x (build 242.xxxxx)
        'extension': (1, 5, 52),    # 1.5.52-241
    },
    'eclipse': {
        'ide': (4, 31),             # Eclipse 4.31
        'extension': (0, 9, 3),     # 0.9.3.202507240902
    },
    'xcode': {
        'ide': (13, 2, 1),          # Xcode 13.2.1
        'extension': (0, 40, 0),    # 0.40.0
    },
}




def is_ide_surface(surface_str):
    """Check if a surface string represents an IDE (vs web, CLI, mobile, etc).
    
    Returns True if the surface is an IDE that should have version info.
    Returns False for non-IDE surfaces like web chat, CLI, mobile apps.
    """
    if not surface_str:
        return False
    
    # Get the base surface name (first part before /)
    parts = surface_str.split('/')
    base_surface = parts[0].lower().strip()
    
    # Check if it's a known non-IDE surface
    if base_surface in NON_IDE_SURFACES:
        return False
    
    # Check if it looks like an IDE surface (has version info structure)
    # IDE surfaces typically have format: ide_name/version/... with at least 2 parts
    if len(parts) >= 2 and parts[1]:  # Has version component
        return True
    
    # "unknown/GitHubCopilotChat/..." is VS Code with unidentified IDE
    if base_surface == 'unknown' and len(parts) >= 2:
        if 'copilot' in parts[1].lower():
            return True
    
    # Single-word surfaces without version info are non-IDE
    return False


def parse_version(version_str):
    """Parse a version string into a tuple of integers for comparison."""
    if not version_str:
        return None
    # Extract numeric parts only
    match = re.match(r'^(\d+)(?:\.(\d+))?(?:\.(\d+))?', version_str)
    if not match:
        return None
    parts = [int(p) for p in match.groups() if p is not None]
    return tuple(parts) if parts else None


def is_version_supported(surface_str):
    """Check if IDE/extension version meets minimum requirements. Returns (is_supported, reason)."""
    if not surface_str:
        return False, "No surface info"
    
    parts = surface_str.split('/')
    if len(parts) < 2:
        return False, "Invalid surface format"
    
    ide_name = parts[0].lower()
    ide_version_str = parts[1] if len(parts) > 1 else ''
    
    # Get extension info if available
    ext_name = parts[2] if len(parts) > 2 else ''
    ext_version_str = parts[3] if len(parts) > 3 else ''
    
    # Determine which minimum version set to use
    if ide_name in ['vscode', 'vscode-chat']:
        min_ver = MIN_VERSIONS.get('vscode')
        ide_type = 'vscode'
    elif ide_name.startswith('jetbrains-') or ide_name in ['eclipse ide', 'eclipse']:
        min_ver = MIN_VERSIONS.get('jetbrains')
        ide_type = 'jetbrains'
    elif ide_name in ['visualstudio', 'vs']:
        min_ver = MIN_VERSIONS.get('visualstudio')
        ide_type = 'visualstudio'
    elif ide_name == 'xcode':
        min_ver = MIN_VERSIONS.get('xcode')
        ide_type = 'xcode'
    else:
        # Unknown IDE - can't validate, assume supported
        return True, None
    
    if not min_ver:
        return True, None
    
    # Parse IDE version
    ide_version = parse_version(ide_version_str)
    if not ide_version:
        return False, f"Cannot parse IDE version: {ide_version_str}"
    
    # Check IDE version - special handling for JetBrains build numbers
    if ide_type == 'jetbrains':
        # JetBrains uses build numbers like 242.xxxxx, 243.xxxxx, 251.xxxxx
        # Format: YYR.xxxxx where YY=year-2000, R=release (1,2,3)
        # 2024.2.x → 242.xxxxx, 2024.3.x → 243.xxxxx, 2025.1.x → 251.xxxxx
        min_build = min_ver.get('ide_build')
        if min_build and ide_version:
            build_prefix = ide_version[0]  # e.g., 242, 243, 251
            if build_prefix < min_build:
                return False, f"IDE build {ide_version_str} < minimum build {min_build}.x"
    else:
        # Standard version comparison for other IDEs
        min_ide = min_ver.get('ide')
        if min_ide:
            # Pad versions to same length for comparison
            ide_padded = ide_version + (0,) * (len(min_ide) - len(ide_version))
            min_padded = min_ide + (0,) * (len(ide_version) - len(min_ide))
            
            if ide_padded[:len(min_ide)] < min_ide:
                return False, f"IDE version {ide_version_str} < minimum {'.'.join(map(str, min_ide))}"
    
    # Check extension version if available
    if ext_version_str:
        ext_version = parse_version(ext_version_str)
        min_ext = min_ver.get('extension')
        
        if ext_version and min_ext:
            ext_padded = ext_version + (0,) * (len(min_ext) - len(ext_version))
            min_ext_padded = min_ext + (0,) * (len(ext_version) - len(min_ext))
            
            if ext_padded[:len(min_ext)] < min_ext:
                return False, f"Extension version {ext_version_str} < minimum {'.'.join(map(str, min_ext))}"
    
    return True, None


def main():
    parser = argparse.ArgumentParser(
        description='Analyze Copilot usage data by comparing an activity report CSV against JSON data for the same date.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Compare activity report against JSON data in customer directory
  python analyze_copilot_data_v2.py --csv ./github/2026-01-10/github-seat-activity.csv --json-dir ./github

  # The script will:
  #   1. Extract the report date from the CSV
  #   2. Find JSON files where that date falls within their report window
  #   3. Compare users from CSV against JSON data for that date
        """
    )
    
    parser.add_argument(
        '--csv', '-c',
        required=True,
        help='Path to the activity report CSV file'
    )
    
    parser.add_argument(
        '--json-dir', '-j',
        required=True,
        help='Directory containing JSON usage files (searches recursively)'
    )
    
    parser.add_argument(
        '--output-dir', '-o',
        help='Output directory (default: same directory as CSV)'
    )
    
    args = parser.parse_args()
    
    # Validate inputs
    if not os.path.isfile(args.csv):
        print(f"Error: CSV file not found: {args.csv}")
        return 1
    
    if not os.path.isdir(args.json_dir):
        print(f"Error: JSON directory not found: {args.json_dir}")
        return 1
    
    activity_report_path = args.csv
    
    # Extract report date from CSV
    report_date = None
    with open(activity_report_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            report_time = row.get('Report Time', '')
            if report_time:
                report_date = report_time[:10]  # YYYY-MM-DD
                break
    
    if not report_date:
        print("Error: Could not find Report Time in activity report CSV")
        return 1
    
    print("\n" + "=" * 60)
    print("COPILOT USAGE DATA ANALYZER v2")
    print("=" * 60 + "\n")
    
    print(f"Activity report: {activity_report_path}")
    print(f"Report date: {report_date}")
    
    # Find all JSON files in json-dir (recursively)
    json_files = glob.glob(os.path.join(args.json_dir, '**', '*.json'), recursive=True)
    
    if not json_files:
        print(f"Error: No JSON files found in {args.json_dir}")
        return 1
    
    print(f"Found {len(json_files)} JSON files in {args.json_dir}")
    
    # Find JSON files that cover the report date
    # CSV is typically generated 1-2 days after JSON report_end_day
    # So we match if report_date is within 3 days after report_end_day
    from datetime import datetime, timedelta
    matching_json_files = []
    json_end_date = None
    
    for jf in json_files:
        try:
            with open(jf, 'r') as f:
                first_line = f.readline().strip()
                if first_line:
                    record = json.loads(first_line)
                    start = record.get('report_start_day', '')
                    end = record.get('report_end_day', '')
                    if start and end:
                        end_date = datetime.strptime(end, '%Y-%m-%d').date()
                        report_dt = datetime.strptime(report_date, '%Y-%m-%d').date()
                        # CSV can be generated up to 3 days after JSON end date
                        if start <= report_date and report_dt <= end_date + timedelta(days=3):
                            matching_json_files.append(jf)
                            # Track the JSON end date for comparison
                            if json_end_date is None or end > json_end_date:
                                json_end_date = end
        except (json.JSONDecodeError, IOError):
            continue
    
    if not matching_json_files:
        print(f"Error: No JSON files found that cover report date {report_date}")
        print(f"  Searched {len(json_files)} files in {args.json_dir}")
        return 1
    
    # Use JSON end date for comparison (CSV may be generated after JSON data ends)
    comparison_date = json_end_date if json_end_date else report_date
    print(f"Found {len(matching_json_files)} JSON files (data ends: {json_end_date})")
    print(f"Comparing activity for date: {comparison_date}")
    
    # Setup output directory
    if args.output_dir:
        output_dir = args.output_dir
    else:
        output_dir = os.path.join(os.path.dirname(activity_report_path), 'output-v2')
    os.makedirs(output_dir, exist_ok=True)
    
    # Extract customer name from CSV filename
    csv_basename = os.path.basename(activity_report_path)
    customer_name = csv_basename.split('-seat-activity')[0] if '-seat-activity' in csv_basename else 'unknown'
    customer_name_display = customer_name.replace('-', ' ').title()
    
    discrepancies_csv_path = os.path.join(output_dir, 'discrepancies.csv')
    summary_path = os.path.join(output_dir, f'{customer_name}-summary.md')
    
    # Parse JSON files - only extract data for the comparison date
    print(f"\nParsing JSON files for date {comparison_date}...")
    json_users = {}  # user_login -> record data for comparison_date
    
    for jf in matching_json_files:
        with open(jf, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    day = record.get('day', '')
                    user_login = record.get('user_login', '')
                    
                    # Only include records for the exact comparison date
                    if day == comparison_date and user_login:
                        json_users[user_login] = record
                except json.JSONDecodeError:
                    continue
    
    print(f"Found {len(json_users)} users with activity on {comparison_date} in JSON")
    
    # Compare against CSV
    print(f"\nComparing against activity report...")
    
    discrepancies = []
    stats = {
        'total_csv_users': 0,
        'users_active_on_date': 0,
        'users_in_json': 0,
        'users_missing_from_json': 0,
        'users_non_ide': 0,
        'users_unsupported_version': 0,
        'missing_by_ide': defaultdict(int),
        'missing_by_extension': defaultdict(int),
        'total_by_ide': defaultdict(int),
        'total_by_extension': defaultdict(int),
    }
    
    with open(activity_report_path, 'r') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            stats['total_csv_users'] += 1
            
            login = row.get('Login', '')
            last_activity_at = row.get('Last Activity At', '')
            last_surface = row.get('Last Surface Used', '')
            
            # Check if user was active on the report date
            if not last_activity_at or last_activity_at.lower() == 'none':
                continue
            
            activity_date = last_activity_at[:10]
            if activity_date != comparison_date:
                continue
            
            stats['users_active_on_date'] += 1
            
            # Skip non-IDE surfaces
            if not is_ide_surface(last_surface):
                stats['users_non_ide'] += 1
                continue
            
            # Skip unsupported versions
            is_supported, _ = is_version_supported(last_surface)
            if not is_supported:
                stats['users_unsupported_version'] += 1
                continue
            
            # Extract IDE category and extension version
            surface_parts = last_surface.split('/') if last_surface else []
            surface_name = surface_parts[0].lower() if surface_parts else 'unknown'
            
            # Normalize "unknown/GitHubCopilotChat/..." to vscode
            if surface_name == 'unknown' and len(surface_parts) >= 2:
                if 'copilot' in surface_parts[1].lower():
                    surface_name = 'vscode'
            
            ide_category = IDE_CATEGORIES.get(surface_name, 'Other')
            stats['total_by_ide'][ide_category] += 1
            
            # Extract extension version
            ext_version = 'unknown'
            if len(surface_parts) >= 4:
                ext_name = surface_parts[2]
                ext_ver = surface_parts[3]
                ext_version = f"{ext_name}/{ext_ver}"
            elif len(surface_parts) >= 3:
                ext_name = surface_parts[1]
                ext_ver = surface_parts[2]
                if ext_name.lower() == 'githubcopilotchat':
                    ext_name = 'copilot-chat'
                ext_version = f"{ext_name}/{ext_ver}"
            
            stats['total_by_extension'][ext_version] += 1
            
            # Check if user is in JSON
            if login in json_users:
                stats['users_in_json'] += 1
            else:
                stats['users_missing_from_json'] += 1
                stats['missing_by_ide'][ide_category] += 1
                stats['missing_by_extension'][ext_version] += 1
                discrepancies.append({
                    'Login': login,
                    'Last Activity At': last_activity_at,
                    'Last Surface Used': last_surface,
                    'Status': 'Missing from JSON'
                })
    
    # Write discrepancies CSV
    if discrepancies:
        fieldnames = ['Login', 'Last Activity At', 'Last Surface Used', 'Status']
        with open(discrepancies_csv_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(discrepancies)
        print(f"Wrote {len(discrepancies)} discrepancies to {discrepancies_csv_path}")
    
    # Write summary
    with open(summary_path, 'w') as f:
        f.write(f"# {customer_name_display.upper()} - COPILOT USAGE DATA ANALYSIS\n\n")
        f.write(f"**Report Date:** {report_date}  \n")
        f.write(f"**Comparison Date:** {comparison_date}\n\n")
        
        f.write("## Activity Report\n\n")
        f.write(f"- Total users in CSV: {stats['total_csv_users']:,}\n")
        f.write(f"- Users active on {comparison_date}: {stats['users_active_on_date']:,}\n")
        f.write(f"- Users with non-IDE activity (skipped): {stats['users_non_ide']:,}\n")
        f.write(f"- Users on unsupported versions (skipped): {stats['users_unsupported_version']:,}\n\n")
        
        ide_users = stats['users_active_on_date'] - stats['users_non_ide'] - stats['users_unsupported_version']
        f.write("NOTE: JSON data is IDE-only today\n\n")
        
        f.write("## Analysis\n\n")
        f.write(f"- Users found in JSON: {stats['users_in_json']:,}\n")
        f.write(f"- **Users missing from JSON: {stats['users_missing_from_json']:,}**\n")
        
        if ide_users > 0:
            missing_pct = stats['users_missing_from_json'] / ide_users * 100
            f.write(f"\n**Missing rate:** {missing_pct:.1f}% ({stats['users_missing_from_json']:,} / {ide_users:,} IDE users on supported versions)\n")
        
        # IDE breakdown
        f.write("\n### IDEs\n\n")
        sorted_ides = sorted(stats['missing_by_ide'].items(), key=lambda x: x[1], reverse=True)
        for ide, missing_count in sorted_ides:
            total_ide_users = stats['total_by_ide'].get(ide, 0)
            if total_ide_users > 0:
                pct = missing_count / total_ide_users * 100
                f.write(f"- % missing from {ide}: **{pct:.1f}%** ({missing_count:,} / {total_ide_users:,})\n")
        
        # Extension version breakdown
        f.write("\n### Extension Versions (Missing Events)\n\n")
        
        # Sort extensions by version (newest first) - extract copilot-chat versions
        def sort_key(item):
            ext, _ = item
            if ext.startswith('copilot-chat/'):
                ver = ext.replace('copilot-chat/', '')
                parts = ver.split('.')
                try:
                    return (1, int(parts[0]) if parts[0].isdigit() else 0, 
                           int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0,
                           int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0)
                except:
                    return (0, 0, 0, 0)
            return (0, 0, 0, 0)
        
        sorted_extensions = sorted(stats['missing_by_extension'].items(), key=sort_key, reverse=True)
        for ext, missing_count in sorted_extensions[:20]:
            total_ext_users = stats['total_by_extension'].get(ext, 0)
            if total_ext_users > 0:
                pct = missing_count / total_ext_users * 100
                f.write(f"- {ext}: {missing_count:,} ({pct:.1f}% of {total_ext_users:,} users)\n")
    
    print(f"Wrote summary to {summary_path}")
    
    # Print summary to console
    print("\n" + "=" * 60)
    print(f"SUMMARY - {comparison_date}")
    print("="*60)
    print(f"Users active on {comparison_date}: {stats['users_active_on_date']:,}")
    print(f"  - Non-IDE (skipped): {stats['users_non_ide']:,}")
    print(f"  - Unsupported version (skipped): {stats['users_unsupported_version']:,}")
    print(f"  - Found in JSON: {stats['users_in_json']:,}")
    print(f"  - Missing from JSON: {stats['users_missing_from_json']:,}")
    
    ide_users = stats['users_active_on_date'] - stats['users_non_ide'] - stats['users_unsupported_version']
    if ide_users > 0:
        missing_pct = stats['users_missing_from_json'] / ide_users * 100
        print(f"\nMissing rate: {missing_pct:.1f}%")
    
    return 0


if __name__ == '__main__':
    exit(main())
