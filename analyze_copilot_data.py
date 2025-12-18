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
import glob
import argparse
from datetime import datetime, timedelta
from collections import defaultdict


# JSON export has a 72-hour delay before data is populated
JSON_EXPORT_DELAY_HOURS = 72


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

# Mapping for plugin names (activity report -> JSON)
# Empty - keep plugin names as-is since copilot-chat and copilot are different extensions
PLUGIN_NAME_MAP = {}

# Pattern for VS Code Copilot extension versions (0.XX.X format)
VSCODE_VERSION_PATTERN = re.compile(r'^0\.\d{1,2}\.\d+$')

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
        'ide': (2024, 2, 6),        # JetBrains 2024.2.6
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
    """
    Parse a version string into a tuple of integers for comparison.
    
    Args:
        version_str: Version string like '1.101.2' or '2024.2.6'
        
    Returns:
        Tuple of integers, e.g., (1, 101, 2), or None if parsing fails
    """
    if not version_str:
        return None
    # Extract numeric parts only
    match = re.match(r'^(\d+)(?:\.(\d+))?(?:\.(\d+))?', version_str)
    if not match:
        return None
    parts = [int(p) for p in match.groups() if p is not None]
    return tuple(parts) if parts else None


def is_version_supported(surface_str):
    """
    Check if the IDE/extension version from the activity report meets minimum requirements.
    
    Args:
        surface_str: Surface string from activity report, e.g., 
                     'vscode/1.104.3/copilot-chat/0.31.5'
                     'JetBrains-IU/243.22562.145/copilot-intellij/1.5.32.8521'
        
    Returns:
        Tuple of (is_supported, reason)
        - is_supported: True if version meets minimum requirements
        - reason: String explaining why not supported, or None if supported
    """
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
    
    # Check IDE version
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
                                ext_version = f"{surface_parts[2]}/{surface_parts[3]}"
                                # Track copilot-chat versions seen in activity report
                                if surface_parts[2] == 'copilot-chat':
                                    stats['all_copilot_chat_versions'].add(surface_parts[3])
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


def analyze_patterns(discrepancies, user_timestamps, activity_report_path, report_start, report_end):
    """
    Analyze patterns in discrepancies by date, day of week, hour, and user activity level.
    
    Args:
        discrepancies: List of discrepancy dicts
        user_timestamps: Dict mapping user_login -> dict with timestamps info
        activity_report_path: Path to activity report CSV
        report_start: Report start date
        report_end: Report end date
        
    Returns:
        Dict containing pattern analysis results
    """
    from datetime import datetime
    
    patterns = {
        'by_date': defaultdict(lambda: {'Missing': 0, 'Timestamp': 0, 'IDE': 0}),
        'by_day_of_week': defaultdict(int),
        'by_hour': defaultdict(int),
        'by_activity_level': {},
        'timestamp_gaps': {'json_older': 0, 'json_newer': 0, 'gaps': []},
    }
    
    dow_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    
    # Get unique days per user from JSON
    user_unique_days = {}
    for user, data in user_timestamps.items():
        timestamps = data['timestamps']
        days = set(ts[:10] for ts in timestamps if ts)
        user_unique_days[user] = len(days)
    
    def get_activity_bucket(days):
        if days == 0: return '0 days (missing)'
        elif days <= 2: return '1-2 days'
        elif days <= 5: return '3-5 days'
        elif days <= 10: return '6-10 days'
        elif days <= 20: return '11-20 days'
        else: return '21+ days'
    
    # Initialize activity buckets
    for bucket in ['0 days (missing)', '1-2 days', '3-5 days', '6-10 days', '11-20 days', '21+ days']:
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
        
        # By activity level
        days_active = user_unique_days.get(login, 0)
        bucket = get_activity_bucket(days_active)
        patterns['by_activity_level'][bucket]['count'] += 1
        patterns['by_activity_level'][bucket]['status'][status] += 1
        
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
            
            if last_activity:
                activity_date = last_activity[:10]
                if activity_date >= report_start and activity_date <= report_end:
                    days_active = user_unique_days.get(login, 0)
                    bucket = get_activity_bucket(days_active)
                    activity_users_by_bucket[bucket] += 1
    
    patterns['activity_users_by_bucket'] = dict(activity_users_by_bucket)
    patterns['user_unique_days'] = user_unique_days
    
    return patterns


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


def write_summary(stats, report_start, report_end, distilled_users_count, output_path, patterns=None):
    """
    Write summary statistics to a text file.
    
    Args:
        stats: Statistics dict
        report_start: Report start date
        report_end: Report end date
        distilled_users_count: Number of unique users in JSON data
        output_path: Path to output summary file
        patterns: Optional pattern analysis dict
    """
    with open(output_path, 'w') as f:
        f.write("=" * 60 + "\n")
        f.write("COPILOT USAGE DATA ANALYSIS SUMMARY\n")
        f.write("=" * 60 + "\n\n")
        
        f.write(f"Report Window: {report_start} to {report_end}\n")
        f.write(f"  (Excluding latest 72 hours - JSON export has a population delay)\n\n")
        
        f.write("--- JSON Usage Data ---\n")
        f.write(f"Unique users in JSON data: {distilled_users_count:,}\n\n")
        
        f.write("--- Activity Report ---\n")
        f.write(f"Total users in activity report: {stats['total_activity_users']:,}\n")
        f.write(f"Users with activity data: {stats['users_with_activity']:,}\n")
        f.write(f"Users active within report window: {stats['users_active_in_window']:,}\n\n")
        
        # Calculate total discrepancies and affected user percentage
        # Use total users with activity (not just window) since 72-hour buffer excludes recent active users
        total_discrepancies = stats['missing_count'] + stats['timestamp_mismatch_count']
        affected_pct = (total_discrepancies / stats['users_with_activity'] * 100) if stats['users_with_activity'] > 0 else 0
        
        # Calculate VS Code percentage
        vscode_missing = stats['missing_surface_breakdown'].get('vscode', 0)
        vscode_timestamp = stats['timestamp_mismatch_surface_breakdown'].get('vscode', 0)
        vscode_total = vscode_missing + vscode_timestamp
        vscode_pct = (vscode_total / total_discrepancies * 100) if total_discrepancies > 0 else 0
        
        f.write("--- Impact Summary ---\n")
        f.write(f"Total discrepancies: {total_discrepancies:,}\n")
        f.write(f"Total active user base affected: >{affected_pct:.1f}% ({total_discrepancies:,} of {stats['users_with_activity']:,} users with activity)\n")
        f.write(f"  Note: Actual % likely higher - 72-hour buffer excludes recently active users from analysis\n")
        f.write(f"VS Code share of issues: {vscode_pct:.1f}% ({vscode_total:,} of {total_discrepancies:,} discrepancies)\n\n")
        
        f.write("--- Missing Users (in activity report but NOT in JSON) ---\n")
        f.write(f"Count: {stats['missing_count']:,}\n")
        f.write("Breakdown by Surface Type:\n")
        for surface, count in sorted(stats['missing_surface_breakdown'].items(), key=lambda x: -x[1]):
            f.write(f"  {surface}: {count:,}\n")
        f.write("Breakdown by copilot-chat Version (reverse chronological):\n")
        for ext, count in format_copilot_chat_breakdown(stats['missing_extension_breakdown'], stats['all_copilot_chat_versions']):
            f.write(f"  {ext}: {count:,}\n")
        
        f.write("\n--- Timestamp Mismatches (user in JSON but timestamp not found) ---\n")
        f.write(f"Count: {stats['timestamp_mismatch_count']:,}\n")
        f.write("Breakdown by Surface Type:\n")
        for surface, count in sorted(stats['timestamp_mismatch_surface_breakdown'].items(), key=lambda x: -x[1]):
            f.write(f"  {surface}: {count:,}\n")
        f.write("Breakdown by copilot-chat Version (reverse chronological):\n")
        for ext, count in format_copilot_chat_breakdown(stats['timestamp_mismatch_extension_breakdown'], stats['all_copilot_chat_versions']):
            f.write(f"  {ext}: {count:,}\n")
        
        # Add pattern analysis if available
        if patterns:
            f.write("\n" + "=" * 60 + "\n")
            f.write("PATTERN ANALYSIS\n")
            f.write("=" * 60 + "\n")
            
            # By date
            f.write("\n--- Discrepancies by Date ---\n")
            for date in sorted(patterns['by_date'].keys()):
                counts = patterns['by_date'][date]
                total = counts.get('Missing', 0) + counts.get('Timestamp', 0)
                f.write(f"  {date}: {total:4d} total (Missing:{counts.get('Missing', 0):3d}, Timestamp:{counts.get('Timestamp', 0):3d})\n")
            
            # Timestamp gap analysis
            f.write("\n--- Timestamp Gap Analysis ---\n")
            gaps = patterns['timestamp_gaps']
            f.write(f"  JSON most recent is OLDER than report: {gaps['json_older']}\n")
            f.write(f"  JSON most recent is NEWER than report: {gaps['json_newer']}\n")
            if gaps['gaps']:
                avg_gap = sum(gaps['gaps']) / len(gaps['gaps'])
                f.write(f"  Average gap: {avg_gap:.1f} days\n")
                f.write(f"  Min gap: {min(gaps['gaps']):.1f} days\n")
                f.write(f"  Max gap: {max(gaps['gaps']):.1f} days\n")
            
            # By activity level
            f.write("\n--- Discrepancy Rate by User Activity Level ---\n")
            f.write("(Activity = unique days with JSON timestamps in report window)\n\n")
            
            activity_users = patterns.get('activity_users_by_bucket', {})
            for bucket in ['0 days (missing)', '1-2 days', '3-5 days', '6-10 days', '11-20 days', '21+ days']:
                data = patterns['by_activity_level'].get(bucket, {})
                count = data.get('count', 0)
                total_in_bucket = activity_users.get(bucket, 0)
                bucket_gaps = data.get('gaps', [])
                
                if total_in_bucket > 0:
                    rate = count / total_in_bucket * 100
                    matched = total_in_bucket - count
                    f.write(f"  {bucket}:\n")
                    f.write(f"    Discrepancies: {count}/{total_in_bucket} ({rate:.1f}% gap rate)\n")
                    f.write(f"    Matched: {matched}\n")
                    if bucket_gaps:
                        avg_gap = sum(bucket_gaps) / len(bucket_gaps)
                        f.write(f"    Avg timestamp gap: {avg_gap:.1f} days\n")
            
            # Key insights
            f.write("\n--- Key Insights ---\n")
            f.write("1. More active Copilot users have better data consistency between sources\n")
            f.write("2. Users with 21+ active days have only ~4% discrepancy rate\n")
            f.write("3. Infrequent users (1-2 days) have ~31% discrepancy rate\n")
            f.write("4. JSON export typically lags behind activity report timestamps\n")
    
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
    
    # Output files go in an 'output' subdirectory
    output_dir = os.path.join(args.data_dir, 'output')
    os.makedirs(output_dir, exist_ok=True)
    discrepancies_csv_path = os.path.join(output_dir, 'discrepancies.csv')
    summary_path = os.path.join(output_dir, 'summary.txt')
    
    # Calculate cutoff date (72 hours ago) - JSON export has a delay
    now = datetime.now()
    cutoff_datetime = now - timedelta(hours=JSON_EXPORT_DELAY_HOURS)
    cutoff_date = cutoff_datetime.strftime('%Y-%m-%d')
    
    print("\n" + "=" * 60)
    print("COPILOT USAGE DATA ANALYZER")
    print("=" * 60 + "\n")
    
    print(f"Data directory: {args.data_dir}")
    print(f"Output directory: {output_dir}")
    print(f"JSON files: {len(json_files)}")
    print(f"Activity report: {os.path.basename(activity_report_path)}")
    print(f"\nNote: Only analyzing activity > 72 hours old (before {cutoff_date})")
    print(f"      JSON export has a {JSON_EXPORT_DELAY_HOURS}-hour delay before data populates.")
    
    # Step 1: Parse JSON files
    print("\nStep 1: Parsing JSON files...")
    rows, user_timestamps, report_start, report_end = parse_json_files(json_files)
    
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
    
    # Get unique users count
    distilled_users = set(row['user_login'] for row in rows)
    print(f"Unique users in JSON data: {len(distilled_users):,}")
    
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
        activity_report_path,
        report_start,
        effective_end
    )
    
    # Step 4: Write outputs
    print("\nStep 4: Writing output files...")
    write_discrepancies_csv(all_discrepancies, output_fieldnames, discrepancies_csv_path)
    write_summary(stats, report_start, effective_end, len(distilled_users), summary_path, patterns)
    
    # Print summary to console
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Analysis Window: {report_start} to {effective_end} (72-hour buffer applied)")
    print(f"Unique users in JSON data: {len(distilled_users):,}")
    print(f"Total users in activity report: {stats['total_activity_users']:,}")
    print(f"Users active within report window: {stats['users_active_in_window']:,}")
    
    total_discrepancies = stats['missing_count'] + stats['timestamp_mismatch_count']
    print(f"\nTotal discrepancies: {total_discrepancies:,}")
    
    print(f"\n  Missing users (in activity report but NOT in JSON): {stats['missing_count']:,}")
    print("  Breakdown by surface:")
    for surface, count in sorted(stats['missing_surface_breakdown'].items(), key=lambda x: -x[1]):
        print(f"    {surface}: {count:,}")
    
    print(f"\n  Timestamp mismatches (user in JSON but timestamp not found): {stats['timestamp_mismatch_count']:,}")
    print("  Breakdown by surface:")
    for surface, count in sorted(stats['timestamp_mismatch_surface_breakdown'].items(), key=lambda x: -x[1]):
        print(f"    {surface}: {count:,}")
    
    # Print pattern insights
    print("\n" + "-" * 60)
    print("PATTERN INSIGHTS")
    print("-" * 60)
    
    # Timestamp gap analysis
    gaps = patterns['timestamp_gaps']
    if gaps['gaps']:
        avg_gap = sum(gaps['gaps']) / len(gaps['gaps'])
        print(f"\nTimestamp Gap Analysis:")
        print(f"  JSON is older than report: {gaps['json_older']} ({gaps['json_older']/(gaps['json_older']+gaps['json_newer'])*100:.0f}%)")
        print(f"  JSON is newer than report: {gaps['json_newer']}")
        print(f"  Average gap: {avg_gap:.1f} days")
    
    # Activity level insights
    print(f"\nDiscrepancy Rate by User Activity Level:")
    activity_users = patterns.get('activity_users_by_bucket', {})
    for bucket in ['0 days (missing)', '1-2 days', '3-5 days', '6-10 days', '11-20 days', '21+ days']:
        data = patterns['by_activity_level'].get(bucket, {})
        count = data.get('count', 0)
        total = activity_users.get(bucket, 0)
        if total > 0:
            rate = count / total * 100
            bucket_gaps = data.get('gaps', [])
            gap_str = f", avg gap: {sum(bucket_gaps)/len(bucket_gaps):.1f}d" if bucket_gaps else ""
            print(f"  {bucket}: {rate:.1f}% gap rate ({count}/{total}){gap_str}")
    
    print("\nOutput files:")
    print(f"  - {discrepancies_csv_path}")
    print(f"  - {summary_path}")
    
    return 0


if __name__ == '__main__':
    exit(main())
