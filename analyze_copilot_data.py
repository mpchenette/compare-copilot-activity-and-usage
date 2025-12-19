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
    - summary.md: Summary statistics of the analysis
"""

import json
import csv
import os
import re
import glob
import argparse
from datetime import datetime, timedelta
from collections import defaultdict


# JSON export has up to a 4-day delay before data is populated
# (3 full UTC days normally, up to 4 days with weekend processing delays)
JSON_EXPORT_DELAY_HOURS = 96


# Mapping from activity report surface/IDE names to JSON IDE names
# Note: JSON export reports ALL JetBrains IDEs as 'intellij'
# Note: Eclipse IDE with Copilot plugin also reports as 'intellij' in JSON
SURFACE_TO_JSON_IDE = {
    'vscode': 'vscode',
    'vscode-chat': 'vscode',
    'jetbrains-iu': 'intellij',   # IntelliJ IDEA Ultimate
    'jetbrains-ic': 'intellij',   # IntelliJ IDEA Community
    'jetbrains-py': 'intellij',   # PyCharm Professional
    'jetbrains-pc': 'intellij',   # PyCharm Community
    'jetbrains-cl': 'intellij',   # CLion
    'jetbrains-go': 'intellij',   # GoLand
    'jetbrains-rm': 'intellij',   # RubyMine
    'jetbrains-ws': 'intellij',   # WebStorm
    'jetbrains-rd': 'intellij',   # Rider
    'jetbrains-ps': 'intellij',   # PhpStorm
    'jetbrains-db': 'intellij',   # DataGrip
    'jetbrains-jbc': 'intellij',  # JetBrains Client
    'jetbrains-ai': 'intellij',   # JetBrains AI Assistant
    'jetbrains-equivalent-eclipse ide': 'intellij',  # Eclipse with JetBrains equivalent
    'jetbrains-equivalent-ibm developer for z': 'intellij',  # IBM with JetBrains equivalent
    'eclipse ide': 'intellij',    # Eclipse IDE with Copilot uses JetBrains plugin -> reports as intellij
    'eclipse': 'intellij',        # Eclipse with Copilot uses JetBrains plugin -> reports as intellij
    'ibm developer for z': 'intellij',  # IBM Developer for z with Copilot -> reports as intellij
    'visualstudio': 'visualstudio',
    'vs': 'visualstudio',         # Visual Studio shorthand
    'neovim': 'neovim',
    'vim': 'vim',
    'emacs': 'emacs',
    'xcode': 'xcode',
    'unknown': 'unknown',
}

# Pattern for VS Code Copilot extension versions (0.XX.X format)
VSCODE_VERSION_PATTERN = re.compile(r'^0\.\d{1,2}\.\d+$')

# IDE categories for grouping surfaces in reports
IDE_CATEGORIES = {
    'vscode': 'VS Code',
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

# All copilot-chat extension versions (reverse chronological order - newest first)
COPILOT_CHAT_VERSIONS = [
    '0.35.1', '0.35.0',
    '0.33.5', '0.33.4', '0.33.3', '0.33.2', '0.33.1', '0.33.0',
    '0.32.5', '0.32.4', '0.32.3', '0.32.2', '0.32.1', '0.32.0',
    '0.31.5', '0.31.4', '0.31.3', '0.31.2', '0.31.1', '0.31.0',
    '0.30.3', '0.30.2', '0.30.1', '0.30.0',
    '0.29.1', '0.29.0',
    '0.28.5', '0.28.4', '0.28.3', '0.28.2', '0.28.1', '0.28.0',
]


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


def normalize_timestamp(ts):
    """Normalize a timestamp by removing milliseconds."""
    if not ts:
        return None
    # Remove milliseconds (.XXXXXXX) if present
    if '.' in ts:
        return ts.split('.')[0] + 'Z'
    return ts


def find_closest_timestamp(report_ts, json_timestamps, tolerance_hours=1):
    """Find the closest JSON timestamp to the report timestamp. Returns (closest_ts, within_tolerance, time_diff)."""
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
    """Convert an activity report surface string to JSON format for comparison."""
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
    for part in parts[1:]:
        # Skip 'GitHubCopilotChat' or similar intermediate identifiers
        if part.lower() not in ('githubcopilotchat', 'githubcopilot'):
            normalized_parts.append(part)
    
    return '/'.join(normalized_parts)


def parse_json_files(json_files):
    """
    Parse multiple JSON/NDJSON files and extract key fields.
    
    Args:
        json_files: List of paths to JSON files
        
    Returns:
        Tuple of (list of row dicts, user_timestamps dict, user_interactions dict, report_start_day, report_end_day)
        user_timestamps maps user_login -> dict with:
            'timestamps': set of normalized timestamps
            'timestamp_to_ide': dict mapping timestamp -> IDE string
        user_interactions maps user_login -> total user_initiated_interaction_count
    """
    rows = []
    user_timestamps = defaultdict(lambda: {'timestamps': set(), 'timestamp_to_ide': {}})
    user_interactions = defaultdict(int)  # Track total interactions per user
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
                    
                    # Track user_initiated_interaction_count per user
                    interaction_count = record.get('user_initiated_interaction_count', 0)
                    if user_login and interaction_count:
                        user_interactions[user_login] += interaction_count
                    
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
    
    return rows, user_timestamps, user_interactions, report_start, report_end


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
        'users_active_before_window': 0,
        'users_active_in_window': 0,
        'users_active_after_window': 0,
        'users_unsupported_version': 0,
        'missing_count': 0,
        'timestamp_mismatch_count': 0,
        'missing_surface_breakdown': defaultdict(int),
        'timestamp_mismatch_surface_breakdown': defaultdict(int),
        'missing_extension_breakdown': defaultdict(int),
        'timestamp_mismatch_extension_breakdown': defaultdict(int),
        'unsupported_version_breakdown': defaultdict(int),
        'all_copilot_chat_versions': set()  # Track all versions seen in activity report
    }
    
    with open(activity_report_path, 'r') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            stats['total_activity_users'] += 1
            
            login = row.get('Login', '')
            last_activity_at = row.get('Last Activity At', '')
            last_surface = row.get('Last Surface Used', '')
            
            # Check for actual activity (not "None" or empty)
            if last_activity_at and last_activity_at.lower() != 'none':
                stats['users_with_activity'] += 1
                
                try:
                    last_activity_date = last_activity_at[:10]  # YYYY-MM-DD
                    last_activity_dt = datetime.strptime(last_activity_date, '%Y-%m-%d')
                    
                    # Check if within report window
                    if last_activity_dt < report_start_dt:
                        stats['users_active_before_window'] += 1
                    elif last_activity_dt > report_end_dt:
                        stats['users_active_after_window'] += 1
                    else:
                        stats['users_active_in_window'] += 1
                    
                    # Only process users within the report window for discrepancy analysis
                    if report_start_dt <= last_activity_dt <= report_end_dt:
                        # Track surface type
                        if last_surface:
                            parts = last_surface.split('/')
                            surface = parts[0]
                            # Convert "unknown/GitHubCopilotChat/X.X.X" to vscode
                            # These are VS Code users where IDE wasn't properly identified
                            if surface.lower() == 'unknown' and len(parts) >= 2:
                                if 'copilot' in parts[1].lower():
                                    surface = 'vscode'
                        else:
                            surface = 'unknown'
                        
                        # Skip Neovim users - they are not expected to appear in JSON
                        if surface.lower() == 'neovim':
                            continue
                        
                        # Skip users on unsupported IDE/extension versions
                        # These users will NOT appear in JSON export by design
                        is_supported, unsupported_reason = is_version_supported(last_surface)
                        if not is_supported:
                            stats['users_unsupported_version'] += 1
                            stats['unsupported_version_breakdown'][surface] += 1
                            continue
                        
                        # Normalize activity report surface and timestamp for comparison
                        normalized_surface = normalize_surface_to_json_format(last_surface)
                        normalized_report_ts = normalize_timestamp(last_activity_at)
                        
                        # Extract extension version from surface string
                        ext_version = 'unknown'
                        if last_surface:
                            surface_parts = last_surface.split('/')
                            if len(surface_parts) >= 4:
                                ext_name = surface_parts[2]
                                ext_ver = surface_parts[3]
                                ext_version = f"{ext_name}/{ext_ver}"
                                # Track copilot-chat versions seen in activity report
                                if ext_name == 'copilot-chat':
                                    stats['all_copilot_chat_versions'].add(ext_ver)
                            elif len(surface_parts) >= 3:
                                # Handle "unknown/GitHubCopilotChat/0.33.3" format
                                ext_name = surface_parts[1]
                                ext_ver = surface_parts[2]
                                # Normalize GitHubCopilotChat -> copilot-chat
                                if ext_name.lower() == 'githubcopilotchat':
                                    ext_name = 'copilot-chat'
                                ext_version = f"{ext_name}/{ext_ver}"
                                if ext_name == 'copilot-chat':
                                    stats['all_copilot_chat_versions'].add(ext_ver)
                            elif len(surface_parts) >= 2:
                                ext_version = surface_parts[0]
                        
                        # Check if NOT in JSON data at all
                        if login not in distilled_users:
                            stats['missing_count'] += 1
                            stats['missing_surface_breakdown'][surface] += 1
                            stats['missing_extension_breakdown'][ext_version] += 1
                            
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
                                stats['timestamp_mismatch_extension_breakdown'][ext_version] += 1
                                
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
                            # else: exact match or within tolerance - no discrepancy
                            # (IDE differences are not tracked as discrepancies)
                            
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


def analyze_patterns(discrepancies, user_timestamps, user_interactions, activity_report_path, report_start, report_end):
    """
    Analyze patterns in discrepancies by date, day of week, hour, and user activity level.
    """
    patterns = {
        'by_date': defaultdict(lambda: {'Missing': 0, 'Timestamp': 0, 'IDE': 0}),
        'by_day_of_week': defaultdict(int),
        'by_hour': defaultdict(int),
        'by_activity_level': {},
        'timestamp_gaps': {'json_older': 0, 'json_newer': 0, 'gaps': []},
        'stale_user_interactions': [],
        'healthy_user_interactions': [],
    }
    
    dow_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    
    def get_activity_bucket(interactions):
        """Bucket users by interaction count."""
        if interactions == 0: return '0 (absent)'
        elif interactions <= 5: return '1-5'
        elif interactions <= 20: return '6-20'
        elif interactions <= 50: return '21-50'
        elif interactions <= 100: return '51-100'
        elif interactions <= 500: return '101-500'
        else: return '500+'
    
    # Initialize activity buckets
    for bucket in ['0 (absent)', '1-5', '6-20', '21-50', '51-100', '101-500', '500+']:
        patterns['by_activity_level'][bucket] = {
            'count': 0, 
            'gaps': [], 
            'status': defaultdict(int)
        }
    
    # Analyze each discrepancy
    for d in discrepancies:
        login = d.get('Login', '')
        status = d.get('Status', '')
        last_activity = d.get('Last Activity At', '')
        json_ts = d.get('Latest Export Activity', '')
        
        # By date
        if last_activity:
            date = last_activity[:10]
            if 'Missing' in status:
                patterns['by_date'][date]['Missing'] += 1
            elif 'Timestamp' in status:
                patterns['by_date'][date]['Timestamp'] += 1
            elif 'IDE' in status:
                patterns['by_date'][date]['IDE'] += 1
            
            # By day of week
            try:
                dt = datetime.strptime(date, '%Y-%m-%d')
                patterns['by_day_of_week'][dow_names[dt.weekday()]] += 1
            except:
                pass
            
            # By hour
            try:
                hour = int(last_activity[11:13])
                patterns['by_hour'][hour] += 1
            except:
                pass
        
        # By activity level (based on interaction count)
        interactions = user_interactions.get(login, 0)
        bucket = get_activity_bucket(interactions)
        patterns['by_activity_level'][bucket]['count'] += 1
        patterns['by_activity_level'][bucket]['status'][status] += 1
        
        # Track interaction counts for stale users
        if 'Timestamp' in status:
            patterns['stale_user_interactions'].append(interactions)
        
        # Timestamp gap analysis
        if last_activity and json_ts and 'Timestamp' in status:
            try:
                report_dt = datetime.strptime(last_activity[:19], '%Y-%m-%dT%H:%M:%S')
                json_dt = datetime.strptime(json_ts[:19], '%Y-%m-%dT%H:%M:%S')
                gap_days = (report_dt - json_dt).total_seconds() / 86400
                patterns['timestamp_gaps']['gaps'].append(gap_days)
                if json_dt < report_dt:
                    patterns['timestamp_gaps']['json_older'] += 1
                else:
                    patterns['timestamp_gaps']['json_newer'] += 1
                
                # Add gap to activity bucket
                patterns['by_activity_level'][bucket]['gaps'].append(abs(gap_days))
            except:
                pass
    
    # Build set of users with discrepancies for comparison
    discrepancy_users = set(d.get('Login', '') for d in discrepancies)
    
    # Calculate discrepancy rates by activity level
    # Count users in each activity bucket from activity report
    activity_users_by_bucket = defaultdict(int)
    with open(activity_report_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            login = row.get('Login', '')
            last_activity = row.get('Last Activity At', '')
            surface = row.get('Last Surface Used', '')
            
            # Skip neovim
            if surface and surface.lower().startswith('neovim'):
                continue
            
            if last_activity and last_activity.lower() != 'none':
                activity_date = last_activity[:10]
                if activity_date >= report_start and activity_date <= report_end:
                    interactions = user_interactions.get(login, 0)
                    bucket = get_activity_bucket(interactions)
                    activity_users_by_bucket[bucket] += 1
                    
                    # Track healthy user interaction counts (not in discrepancy list and has JSON data)
                    if login not in discrepancy_users and interactions > 0:
                        patterns['healthy_user_interactions'].append(interactions)
    
    patterns['activity_users_by_bucket'] = dict(activity_users_by_bucket)
    patterns['user_interactions'] = dict(user_interactions)
    return patterns


def generate_ascii_line_graph(data_by_date, graph_height=10, graph_width=60):
    """
    Generate an ASCII line graph for discrepancies over time.
    
    Args:
        data_by_date: Dict mapping date string -> {'Missing': int, 'Timestamp': int}
        graph_height: Height of the graph in lines
        graph_width: Width of the graph in characters
        
    Returns:
        List of strings representing the graph
    """
    if not data_by_date:
        return ["  No data available"]
    
    # Get sorted dates and totals
    sorted_dates = sorted(data_by_date.keys())
    totals = [data_by_date[d].get('Missing', 0) + data_by_date[d].get('Timestamp', 0) for d in sorted_dates]
    
    if not totals:
        return ["  No data available"]
    
    max_val = max(totals) if totals else 1
    min_val = min(totals) if totals else 0
    
    # Calculate how many data points to show (compress if needed)
    num_points = len(totals)
    
    # Build the graph
    lines = []
    
    # Y-axis labels width
    y_label_width = len(str(max_val)) + 1
    
    # Create the graph rows
    for row in range(graph_height, -1, -1):
        threshold = min_val + (max_val - min_val) * row / graph_height if graph_height > 0 else 0
        
        # Y-axis label
        if row == graph_height:
            label = str(max_val).rjust(y_label_width)
        elif row == 0:
            label = str(min_val).rjust(y_label_width)
        elif row == graph_height // 2:
            mid_val = (max_val + min_val) // 2
            label = str(mid_val).rjust(y_label_width)
        else:
            label = ' ' * y_label_width
        
        # Build the row
        row_chars = []
        for i, val in enumerate(totals):
            if val >= threshold:
                # Check if this is peak (local maximum)
                is_peak = (i == 0 or val >= totals[i-1]) and (i == len(totals)-1 or val >= totals[i+1])
                if row == graph_height and val == max_val:
                    row_chars.append('█')
                elif val == max_val and is_peak:
                    row_chars.append('█')
                else:
                    row_chars.append('▓')
            else:
                row_chars.append(' ')
        
        lines.append(f"  {label} │{' '.join(row_chars)}")
    
    # X-axis
    x_axis = f"  {' ' * y_label_width} └{'──' * len(totals)}"
    lines.append(x_axis)
    
    # Date labels (start, middle, end)
    if sorted_dates:
        start_date = sorted_dates[0][5:]  # MM-DD
        end_date = sorted_dates[-1][5:]    # MM-DD
        mid_idx = len(sorted_dates) // 2
        mid_date = sorted_dates[mid_idx][5:] if mid_idx < len(sorted_dates) else ''
        
        # Position the labels (account for spaces between columns)
        graph_width = len(totals) * 2 - 1  # Each column + space between
        mid_pos = graph_width // 2 - 2
        
        # Simple label line
        label_line = f"  {' ' * y_label_width}  {start_date}{' ' * (mid_pos - 5)}{mid_date}{' ' * (mid_pos - 5)}{end_date}"
        lines.append(label_line)
    
    # Summary line
    total_discrepancies = sum(totals)
    avg_per_day = total_discrepancies / len(totals) if totals else 0
    lines.append(f"")
    lines.append(f"  Total: {total_discrepancies:,} discrepancies over {len(totals)} days (avg: {avg_per_day:.1f}/day, max: {max_val})")
    
    return lines


def format_copilot_chat_breakdown(extension_breakdown, all_versions_in_report):
    """
    Format copilot-chat extension breakdown in reverse chronological order.
    Only includes versions that exist in the customer's activity report.
    
    Args:
        extension_breakdown: Dict mapping extension string -> count (discrepancies)
        all_versions_in_report: Set of all copilot-chat versions seen in activity report
        
    Returns:
        List of (version_string, count) tuples in reverse chronological order
    """
    # Extract copilot-chat versions and their discrepancy counts
    discrepancy_counts = {}
    for ext, count in extension_breakdown.items():
        if ext.startswith('copilot-chat/'):
            version = ext.replace('copilot-chat/', '')
            discrepancy_counts[version] = count
    
    if not all_versions_in_report:
        return []
    
    # Build result with only versions present in customer data, in reverse chron order
    result = []
    for ver in COPILOT_CHAT_VERSIONS:
        if ver in all_versions_in_report:
            count = discrepancy_counts.get(ver, 0)
            result.append((f"copilot-chat/{ver}", count))
    
    return result


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


def write_summary(stats, report_start, report_end_original, report_end_analysis, distilled_users_count, distilled_users_analysis_count, output_path, patterns=None, customer_name=None):
    """
    Write summary statistics to a text file.
    
    Args:
        stats: Statistics dict
        report_start: Report start date
        report_end_original: Original report end date from JSON
        report_end_analysis: Analysis end date (96 hours before activity report)
        distilled_users_count: Number of unique users in original JSON window
        distilled_users_analysis_count: Number of unique users in analysis window
        output_path: Path to output summary file
        patterns: Optional pattern analysis dict
        customer_name: Customer name for the report header
    """
    with open(output_path, 'w') as f:
        if customer_name:
            f.write(f"# {customer_name.upper()} - COPILOT USAGE DATA ANALYSIS\n\n")
        else:
            f.write("# COPILOT USAGE DATA ANALYSIS SUMMARY\n\n")
        
        f.write("## Dashboard JSON\n\n")
        f.write("### Report Window\n\n")
        f.write(f"- Original: {report_start} to {report_end_original}\n")
        f.write(f"- Trimmed: **{report_start} to {report_end_analysis}** ← ANALYSIS\n\n")
        f.write("NOTE: Trimmed 96 hours before activity report generation for analysis\n\n")
        f.write(f"- Unique users in original window: **{distilled_users_count:,}**\n")
        f.write(f"- Unique users in analysis window: **{distilled_users_analysis_count:,}** ← ANALYSIS\n")
        
        f.write("\n## Activity Report\n\n")
        active_pct = (stats['users_with_activity'] / stats['total_activity_users'] * 100) if stats['total_activity_users'] > 0 else 0
        f.write(f"% active users: {active_pct:.1f}% ({stats['users_with_activity']:,} / {stats['total_activity_users']:,})\n\n")
        
        total_active = stats['users_with_activity']
        before_pct = (stats['users_active_before_window'] / total_active * 100) if total_active > 0 else 0
        within_pct = (stats['users_active_in_window'] / total_active * 100) if total_active > 0 else 0
        after_pct = (stats['users_active_after_window'] / total_active * 100) if total_active > 0 else 0
        f.write(f"- % active before JSON report window: {before_pct:.1f}% ({stats['users_active_before_window']:,} / {total_active:,})\n")
        f.write(f"- % active within JSON report window: {within_pct:.1f}% ({stats['users_active_in_window']:,} / {total_active:,}) ← ANALYSIS\n")
        f.write(f"- % active after JSON report window: {after_pct:.1f}% ({stats['users_active_after_window']:,} / {total_active:,})\n\n")
        
        # Determine if majority/plurality is outside analysis window
        if within_pct < before_pct or within_pct < after_pct:
            if before_pct > after_pct:
                f.write("NOTE: Scope of analysis is limited as majority of active users fall outside of analysis window (before)\n\n")
            else:
                f.write("NOTE: Scope of analysis is limited as majority of active users fall outside of analysis window (after)\n\n")
        
        # Supported vs unsupported version breakdown for users in analysis window
        users_in_window = stats['users_active_in_window']
        unsupported_count = stats.get('users_unsupported_version', 0)
        supported_count = users_in_window - unsupported_count
        if users_in_window > 0:
            unsupported_pct = (unsupported_count / users_in_window * 100)
            supported_pct = (supported_count / users_in_window * 100)
            f.write(f"- % of analysis users on unsupported versions: {unsupported_pct:.1f}% ({unsupported_count:,} / {users_in_window:,})\n")
            f.write(f"- % of analysis users on supported versions: **{supported_pct:.1f}%** ({supported_count:,} / {users_in_window:,}) ← ANALYSIS\n")
        
        # Calculate total discrepancies and affected user percentage
        # Use total users with activity (not just window) since 96-hour buffer excludes recent active users
        total_discrepancies = stats['missing_count'] + stats['timestamp_mismatch_count']
        affected_pct = (total_discrepancies / stats['users_with_activity'] * 100) if stats['users_with_activity'] > 0 else 0
        
        # Calculate VS Code percentage
        vscode_missing = stats['missing_surface_breakdown'].get('vscode', 0)
        vscode_timestamp = stats['timestamp_mismatch_surface_breakdown'].get('vscode', 0)
        vscode_total = vscode_missing + vscode_timestamp
        vscode_pct = (vscode_total / total_discrepancies * 100) if total_discrepancies > 0 else 0
        
        # Calculate supported users for event missing percentages
        unsupported_in_window = stats.get('users_unsupported_version', 0)
        supported_in_window = stats['users_active_in_window'] - unsupported_in_window
        
        f.write("\n## Analysis\n\n")
        
        f.write(f"- % active users affected: **{affected_pct:.1f}%** ({total_discrepancies:,} / {stats['users_with_activity']:,})\n")
        
        if supported_in_window > 0:
            events_missing_pct = (total_discrepancies / supported_in_window * 100)
            absent_pct = (stats['missing_count'] / supported_in_window * 100)
            stale_pct = (stats['timestamp_mismatch_count'] / supported_in_window * 100)
            f.write(f"\n- % of events missing: **{events_missing_pct:.1f}%** ({total_discrepancies:,} / {supported_in_window:,})\n")
            f.write(f"  - % absent: {absent_pct:.1f}% ({stats['missing_count']:,} / {supported_in_window:,})\n")
            f.write(f"  - % stale: {stale_pct:.1f}% ({stats['timestamp_mismatch_count']:,} / {supported_in_window:,})\n\n")
            f.write("NOTE: Stale meaning >24 hours between activity report data and dashboard JSON data\n")
        
        # Build IDE stats using global IDE_CATEGORIES constant
        ide_stats = defaultdict(lambda: {'absent': 0, 'stale': 0})
        for surface, count in stats['missing_surface_breakdown'].items():
            ide = IDE_CATEGORIES.get(surface.lower(), 'Other')
            ide_stats[ide]['absent'] += count
        for surface, count in stats['timestamp_mismatch_surface_breakdown'].items():
            ide = IDE_CATEGORIES.get(surface.lower(), 'Other')
            ide_stats[ide]['stale'] += count
        
        # Add IDE breakdown section
        f.write("\n### IDEs\n\n")
        
        # Sort by total descending
        sorted_ides = sorted(ide_stats.items(), key=lambda x: x[1]['absent'] + x[1]['stale'], reverse=True)
        for ide, counts in sorted_ides:
            absent = counts['absent']
            stale = counts['stale']
            total = absent + stale
            if total > 0:
                pct_of_total = total / total_discrepancies * 100 if total_discrepancies > 0 else 0
                pct_absent = absent / total * 100
                pct_stale = stale / total * 100
                f.write(f"- % issues from {ide}: **{pct_of_total:.1f}%** ({total:,} / {total_discrepancies:,})\n")
                f.write(f"  - % absent: {pct_absent:.1f}% ({absent:,} / {total:,})\n")
                f.write(f"  - % stale: {pct_stale:.1f}% ({stale:,} / {total:,})\n")
        
        f.write("\n### Patterns\n\n")
        f.write("#### Absent Events\n\n")
        for ext, count in format_copilot_chat_breakdown(stats['missing_extension_breakdown'], stats['all_copilot_chat_versions']):
            f.write(f"- {ext}: {count:,}\n")
        
        f.write("\n#### Stale Events\n\n")
        for ext, count in format_copilot_chat_breakdown(stats['timestamp_mismatch_extension_breakdown'], stats['all_copilot_chat_versions']):
            f.write(f"- {ext}: {count:,}\n")
        
        # Add pattern analysis if available
        if patterns:
            
            # By date - ASCII line graph
            f.write("\n#### Discrepancies by Date\n\n")
            f.write("```\n")
            for line in generate_ascii_line_graph(patterns['by_date'], graph_height=28, graph_width=28):
                f.write(line + "\n")
            f.write("```\n")
            
            # Timestamp gap analysis
            f.write("\n#### Timestamp Gap Analysis\n\n")
            gaps = patterns['timestamp_gaps']
            f.write(f"- JSON most recent is OLDER than report: {gaps['json_older']}\n")
            f.write(f"- JSON most recent is NEWER than report: {gaps['json_newer']}\n")
            
            bar_width = 40  # Width of the bar in characters
            
            # Stale user interaction distribution
            stale_interactions = patterns.get('stale_user_interactions', [])
            if stale_interactions:
                f.write("\n#### Interaction Count per Stale User\n\n")
                f.write("*(How many interactions do stale users have in JSON?)*\n\n")
                f.write("```\n")
                
                # Bucket the stale users by their interaction counts
                stale_buckets = defaultdict(int)
                for interactions in stale_interactions:
                    if interactions <= 5: stale_buckets['1-5'] += 1
                    elif interactions <= 20: stale_buckets['6-20'] += 1
                    elif interactions <= 50: stale_buckets['21-50'] += 1
                    elif interactions <= 100: stale_buckets['51-100'] += 1
                    elif interactions <= 500: stale_buckets['101-500'] += 1
                    else: stale_buckets['500+'] += 1
                
                total_stale = len(stale_interactions)
                for bucket in ['1-5', '6-20', '21-50', '51-100', '101-500', '500+']:
                    count = stale_buckets.get(bucket, 0)
                    pct = (count / total_stale * 100) if total_stale > 0 else 0
                    filled = int(pct / 100 * bar_width)
                    bar = '█' * filled + '░' * (bar_width - filled)
                    label = f"{bucket:12}"
                    f.write(f"  {label} |{bar}| {pct:5.1f}% ({count}/{total_stale})\n")
                f.write("```\n")
                
                avg_interactions = sum(stale_interactions) / len(stale_interactions) if stale_interactions else 0
                f.write(f"\n- Average interactions for stale users: **{avg_interactions:.1f}**\n")
                
                # Also show healthy user stats for comparison
                healthy_interactions = patterns.get('healthy_user_interactions', [])
                if healthy_interactions:
                    avg_healthy = sum(healthy_interactions) / len(healthy_interactions)
                    f.write(f"- Average interactions for healthy users: **{avg_healthy:.1f}**\n")
            
            # By activity level - horizontal bar chart (moved after stale distribution)
            f.write("\n#### Discrepancy Rate by Interaction Count\n\n")
            f.write("*(Based on interaction count, how many users have stale or absent data in JSON?)*\n\n")
            f.write("```\n")
            
            activity_users = patterns.get('activity_users_by_bucket', {})
            
            for bucket in ['0 (absent)', '1-5', '6-20', '21-50', '51-100', '101-500', '500+']:
                data = patterns['by_activity_level'].get(bucket, {})
                count = data.get('count', 0)
                total_in_bucket = activity_users.get(bucket, 0)
                bucket_gaps = data.get('gaps', [])
                
                if total_in_bucket > 0:
                    rate = count / total_in_bucket * 100
                    # Create horizontal bar (scale to 100%)
                    filled = int(rate / 100 * bar_width)
                    bar = '█' * filled + '░' * (bar_width - filled)
                    
                    # Format the label to fixed width for alignment
                    label = f"{bucket:12}"
                    
                    f.write(f"  {label} |{bar}| {rate:5.1f}% ({count}/{total_in_bucket})\n")
            f.write("```\n")
    
    print(f"Wrote summary to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description='Analyze Copilot usage data and compare against activity reports.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze data in a directory
  python analyze_copilot_data.py --data-dir ./gs-data

  # The directory should contain:
  #   - One or more .json files (Copilot usage export)
  #   - One .csv file (activity report)
  # Output will be written to the same directory
        """
    )
    
    parser.add_argument(
        '--data-dir', '-d',
        required=True,
        help='Directory containing JSON usage files and activity report CSV'
    )
    
    args = parser.parse_args()
    
    # Validate directory exists
    if not os.path.isdir(args.data_dir):
        print(f"Error: Directory not found: {args.data_dir}")
        return 1
    
    # Find JSON files and CSV file in directory
    json_files = glob.glob(os.path.join(args.data_dir, '*.json'))
    csv_files = glob.glob(os.path.join(args.data_dir, '*.csv'))
    
    # Filter out output files (discrepancies.csv)
    csv_files = [f for f in csv_files if 'discrepancies' not in os.path.basename(f).lower()]
    
    if not json_files:
        print(f"Error: No JSON files found in {args.data_dir}")
        return 1
    
    if not csv_files:
        print(f"Error: No CSV activity report found in {args.data_dir}")
        return 1
    
    if len(csv_files) > 1:
        print(f"Warning: Multiple CSV files found, using: {csv_files[0]}")
    
    activity_report_path = csv_files[0]
    
    # Extract customer name from CSV filename (e.g., "goldman-sachs-seat-activity-123.csv" -> "goldman-sachs")
    csv_basename = os.path.basename(activity_report_path)
    customer_name = csv_basename.split('-seat-activity')[0] if '-seat-activity' in csv_basename else 'unknown'
    customer_name_display = customer_name.replace('-', ' ').title()  # e.g., "Goldman Sachs"
    
    # Output files go in an 'output' subdirectory
    output_dir = os.path.join(args.data_dir, 'output')
    os.makedirs(output_dir, exist_ok=True)
    discrepancies_csv_path = os.path.join(output_dir, 'discrepancies.csv')
    summary_path = os.path.join(output_dir, f'{customer_name}-summary.md')
    
    # Extract report generation date from activity report CSV
    report_generated_date = None
    with open(activity_report_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            report_time = row.get('Report Time', '')
            if report_time:
                # Parse the report timestamp (e.g., '2025-12-17T20:15:05Z')
                report_generated_date = report_time[:10]  # YYYY-MM-DD
                break
    
    if not report_generated_date:
        print("Warning: Could not find Report Time in activity report, using current date")
        report_generated_date = datetime.now().strftime('%Y-%m-%d')
    
    # Calculate cutoff date (96 hours before report generation) - JSON export has a delay
    report_datetime = datetime.strptime(report_generated_date, '%Y-%m-%d')
    cutoff_datetime = report_datetime - timedelta(hours=JSON_EXPORT_DELAY_HOURS)
    cutoff_date = cutoff_datetime.strftime('%Y-%m-%d')
    
    print("\n" + "=" * 60)
    print("COPILOT USAGE DATA ANALYZER")
    print("=" * 60 + "\n")
    
    print(f"Data directory: {args.data_dir}")
    print(f"Output directory: {output_dir}")
    print(f"JSON files: {len(json_files)}")
    print(f"Activity report: {os.path.basename(activity_report_path)}")
    print(f"Activity report generated: {report_generated_date}")
    print(f"\nNote: Only analyzing activity > 96 hours before report generation (before {cutoff_date})")
    print(f"      JSON export has a {JSON_EXPORT_DELAY_HOURS}-hour delay before data populates.")
    
    # Step 1: Parse JSON files
    print("\nStep 1: Parsing JSON files...")
    rows, user_timestamps, user_interactions, report_start, report_end = parse_json_files(json_files)
    
    if not rows:
        print("Error: No data extracted from JSON files.")
        return 1
    
    if not report_start or not report_end:
        print("Error: Could not determine report window from JSON data.")
        return 1
    
    # Adjust report_end to be the cutoff date if it's more recent
    effective_end = min(report_end, cutoff_date)
    
    print(f"\nJSON report window: {report_start} to {report_end}")
    print(f"Effective analysis window: {report_start} to {effective_end}")
    print(f"Total records extracted: {len(rows):,}")
    
    # Get unique users count (original window)
    distilled_users = set(row['user_login'] for row in rows)
    print(f"Unique users in JSON data: {len(distilled_users):,}")
    
    # Get unique users in analysis window (before effective_end)
    effective_end_dt = datetime.strptime(effective_end, '%Y-%m-%d')
    distilled_users_analysis = set(
        row['user_login'] for row in rows 
        if datetime.strptime(row['day'], '%Y-%m-%d') <= effective_end_dt
    )
    print(f"Unique users in analysis window: {len(distilled_users_analysis):,}")
    
    # Step 2: Find discrepancies (using effective_end as the cutoff)
    print("\nStep 2: Finding discrepancies with activity report...")
    all_discrepancies, output_fieldnames, stats = find_discrepancies(
        rows,
        user_timestamps,
        activity_report_path, 
        report_start, 
        effective_end
    )
    
    # Step 3: Analyze patterns
    print("\nStep 3: Analyzing patterns...")
    patterns = analyze_patterns(
        all_discrepancies,
        user_timestamps,
        user_interactions,
        activity_report_path,
        report_start,
        effective_end
    )
    
    # Step 4: Write outputs
    print("\nStep 4: Writing output files...")
    write_discrepancies_csv(all_discrepancies, output_fieldnames, discrepancies_csv_path)
    write_summary(stats, report_start, report_end, effective_end, len(distilled_users), len(distilled_users_analysis), summary_path, patterns, customer_name_display)
    
    # Print summary to console
    print("\n" + "=" * 60)
    print(f"{customer_name_display.upper()} - SUMMARY")
    print("=" * 60)
    print(f"Analysis Window: {report_start} to {effective_end} (96-hour buffer applied)")
    print(f"Unique users in JSON data: {len(distilled_users):,}")
    print(f"Total users in activity report: {stats['total_activity_users']:,}")
    print(f"Users active within report window: {stats['users_active_in_window']:,}")
    
    total_discrepancies = stats['missing_count'] + stats['timestamp_mismatch_count']
    print(f"\nTotal discrepancies: {total_discrepancies:,}")
    
    print(f"\n  Missing users (in activity report but NOT in JSON): {stats['missing_count']:,}")
    
    print(f"\n  Timestamp mismatches (user in JSON but timestamp not found): {stats['timestamp_mismatch_count']:,}")
    
    # Print pattern insights
    print("\n" + "-" * 60)
    print("PATTERN INSIGHTS")
    print("-" * 60)
    
    # Timestamp gap analysis
    gaps = patterns['timestamp_gaps']
    if gaps['gaps']:
        print(f"\nTimestamp Gap Analysis:")
        print(f"  JSON is older than report: {gaps['json_older']} ({gaps['json_older']/(gaps['json_older']+gaps['json_newer'])*100:.0f}%)")
        print(f"  JSON is newer than report: {gaps['json_newer']}")
    
    # Activity level insights - horizontal bar chart
    print(f"\nDiscrepancy Rate by User Activity Level:")
    activity_users = patterns.get('activity_users_by_bucket', {})
    bar_width = 30
    for bucket in ['0 days (missing)', '1-2 days', '3-5 days', '6-10 days', '11-20 days', '21+ days']:
        data = patterns['by_activity_level'].get(bucket, {})
        count = data.get('count', 0)
        total = activity_users.get(bucket, 0)
        if total > 0:
            rate = count / total * 100
            filled = int(rate / 100 * bar_width)
            empty = bar_width - filled
            bar = '█' * filled + '░' * empty
            print(f"  {bucket:17} |{bar}| {rate:5.1f}% ({count}/{total})")
    
    print("\nOutput files:")
    print(f"  - {discrepancies_csv_path}")
    print(f"  - {summary_path}")
    
    return 0


if __name__ == '__main__':
    exit(main())
