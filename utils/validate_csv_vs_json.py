#!/usr/bin/env python3
"""
Validate CSV activity data against JSON dashboard export data.

For each activity record in the CSV, checks if there's a corresponding 
record in the JSON (matching user_login and day).

Usage:
    python utils/validate_csv_vs_json.py <company>
    python utils/validate_csv_vs_json.py --all

Examples:
    python utils/validate_csv_vs_json.py bofa
    python utils/validate_csv_vs_json.py --all
"""

import argparse
import csv
import json
import os
import re
import sys
from datetime import datetime


# Minimum Copilot Chat extension versions for usage metrics (from GitHub docs)
# https://docs.github.com/en/copilot/rolling-out-copilot-at-scale/analyzing-usage-over-time/dashboard-supported-ides
MIN_EXTENSION_VERSIONS = {
    'copilot-chat': '0.28.0',      # VS Code
    'copilot-intellij': '1.5.52',  # JetBrains (ignoring -241 suffix)
    'copilot-vs-chat': '18.0.471', # Visual Studio
    'copilot-eclipse': '0.9.3',    # Eclipse
    'copilot-xcode': '0.40.0',     # Xcode
}


def parse_version(version_str: str) -> tuple:
    """
    Parse a version string into comparable tuple.
    Examples:
        '0.28.0' -> (0, 28, 0)
        '1.5.52-241' -> (1, 5, 52)  (ignores suffix)
        '18.0.471.29466' -> (18, 0, 471, 29466)
    """
    if not version_str:
        return (0,)
    # Extract numeric parts, ignoring suffixes like -241
    version_str = version_str.split('-')[0]
    parts = re.findall(r'\d+', version_str)
    return tuple(int(p) for p in parts) if parts else (0,)


def is_version_supported(extension_name: str, version: str) -> bool:
    """
    Check if an extension version meets the minimum requirement.
    Returns True if version is >= minimum, or if extension is not in the known list.
    """
    ext_lower = extension_name.lower()
    
    # Find matching minimum version
    min_version = None
    for ext_key, min_ver in MIN_EXTENSION_VERSIONS.items():
        if ext_key in ext_lower:
            min_version = min_ver
            break
    
    # If not a tracked extension (like github_spark, copilot-cli), include it
    if min_version is None:
        return True
    
    current = parse_version(version)
    minimum = parse_version(min_version)
    
    return current >= minimum


def get_available_companies(consolidated_dir: str = 'consolidated-data') -> list[str]:
    """Get list of companies with both CSV and JSON consolidated files."""
    companies = []
    for filename in os.listdir(consolidated_dir):
        if filename.endswith('-consolidated.csv'):
            company = filename.replace('-consolidated.csv', '')
            json_file = os.path.join(consolidated_dir, f'{company}-consolidated.json')
            if os.path.exists(json_file):
                companies.append(company)
    return sorted(companies)


def extract_date(timestamp: str) -> str | None:
    """Extract date (YYYY-MM-DD) from a timestamp string."""
    if not timestamp or timestamp.lower() == 'none':
        return None
    try:
        # Handle ISO format with Z suffix
        if 'T' in timestamp:
            return timestamp.split('T')[0]
        return timestamp[:10] if len(timestamp) >= 10 else None
    except (ValueError, IndexError):
        return None


def extract_extension_version(surface: str) -> str:
    """
    Extract extension name and version from Last Surface Used.
    Normalizes equivalent extension names to a canonical form.
    
    Examples:
        'vscode/1.104.1/copilot-chat/0.31.2' -> 'copilot-chat/0.31.2'
        'unknown/GitHubCopilotChat/0.35.3' -> 'copilot-chat/0.35.3'  (normalized)
        'unknown/GithubCopilot/1.399.0' -> 'copilot/1.399.0'  (normalized)
        'JetBrains-IC/251.26927.53/copilot-intellij/1.5.52-243' -> 'copilot-intellij/1.5.52-243'
        'Eclipse IDE/4.34.0.20241128-0756/copilot-intellij/1.5.0.0' -> 'copilot-intellij/1.5.0.0'
        'github_spark' -> 'github_spark'
    """
    if not surface or surface.lower() == 'none':
        return 'unknown'
    
    parts = surface.split('/')
    
    # Look for copilot extension in the parts
    ext_name = None
    ext_version = None
    for i, part in enumerate(parts):
        if 'copilot' in part.lower() and i + 1 < len(parts):
            ext_name = part
            ext_version = parts[i+1]
            break
        elif 'copilot' in part.lower():
            ext_name = part
            break
    
    if ext_name and ext_version:
        # Normalize extension names to canonical forms
        ext_name_lower = ext_name.lower()
        
        # Map GitHubCopilotChat -> copilot-chat (same VS Code extension)
        if ext_name_lower == 'githubcopilotchat':
            ext_name = 'copilot-chat'
        # Map GithubCopilot -> copilot (same VS Code extension)
        elif ext_name_lower == 'githubcopilot':
            ext_name = 'copilot'
        
        return f"{ext_name}/{ext_version}"
    elif ext_name:
        return ext_name
    
    # If no copilot extension found, return the whole surface or last meaningful part
    if len(parts) >= 2:
        return f"{parts[-2]}/{parts[-1]}" if parts[-1] else parts[-2]
    return surface if surface else 'unknown'


def split_extension_version(ext_str: str) -> tuple[str, str]:
    """
    Split extension string into (name, version).
    Examples:
        'copilot-chat/0.31.2' -> ('copilot-chat', '0.31.2')
        'github_spark' -> ('github_spark', '')
    """
    if '/' in ext_str:
        parts = ext_str.split('/', 1)
        return (parts[0], parts[1])
    return (ext_str, '')


def load_json_keys(json_file: str) -> tuple[set[tuple[str, str]], str | None, str | None]:
    """
    Load all (user_login, day) pairs from JSON file.
    Returns a tuple of (day_keys set, min_date, max_date).
    """
    day_keys = set()
    min_date: str | None = None
    max_date: str | None = None
    
    with open(json_file, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                user_login = record.get('user_login', '').strip()
                day = record.get('day', '').strip()
                
                if user_login and day:
                    # Normalize: lowercase user_login for case-insensitive matching
                    user_lower = user_login.lower()
                    day_keys.add((user_lower, day))
                    
                    # Track date range
                    if min_date is None or day < min_date:
                        min_date = day
                    if max_date is None or day > max_date:
                        max_date = day
                            
            except json.JSONDecodeError:
                continue
    return day_keys, min_date, max_date


def validate_company(company: str, consolidated_dir: str = 'consolidated-data') -> dict:
    """
    Validate CSV records against JSON records for a company.
    
    Checks if each (user, day) pair from CSV exists in JSON (daily binary match).
    Only includes records with supported extension versions per GitHub docs.
    
    Args:
        company: Company name
        consolidated_dir: Path to consolidated data directory
    
    Returns a dict with validation results.
    """
    csv_file = os.path.join(consolidated_dir, f'{company}-consolidated.csv')
    json_file = os.path.join(consolidated_dir, f'{company}-consolidated.json')
    
    if not os.path.exists(csv_file):
        print(f"Error: CSV file not found: {csv_file}")
        return {'error': 'csv_not_found'}
    
    if not os.path.exists(json_file):
        print(f"Error: JSON file not found: {json_file}")
        return {'error': 'json_not_found'}
    
    print(f"\nValidating {company}...")
    print(f"  Loading JSON data...")
    
    # Load all JSON keys and date range
    json_day_keys, min_date, max_date = load_json_keys(json_file)
    print(f"  Loaded {len(json_day_keys):,} unique (user, day) pairs from JSON")
    print(f"  JSON date range: {min_date} to {max_date}")
    
    if not min_date or not max_date:
        print(f"  Error: Could not determine date range from JSON")
        return {'error': 'no_date_range'}
    
    # Track results
    total_csv_records = 0
    in_range = 0
    matched = 0
    unmatched = 0
    no_activity_date = 0
    outside_range = 0
    unsupported_version = 0
    unmatched_samples: list[dict] = []
    
    # Track by extension version: {version: {'matched': n, 'unmatched': n, 'supported': bool}}
    extension_stats: dict[str, dict[str, int | bool]] = {}
    
    # Track unique (user, day) pairs to avoid counting duplicates
    seen_user_days: set[tuple[str, str]] = set()
    
    print(f"  Checking CSV records (only dates {min_date} to {max_date})...")
    
    with open(csv_file, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_csv_records += 1
            
            login = row.get('Login', '').strip()
            last_activity = row.get('Last Activity At', '').strip()
            surface = row.get('Last Surface Used', '').strip()
            activity_date = extract_date(last_activity)
            extension_version = extract_extension_version(surface)
            ext_name, ext_ver = split_extension_version(extension_version)
            
            # Check if version is supported
            version_supported = is_version_supported(ext_name, ext_ver)
            
            if not activity_date:
                no_activity_date += 1
                continue
            
            # Skip records outside the JSON date range
            if activity_date < min_date or activity_date > max_date:
                outside_range += 1
                continue
            
            # Track extension stats (before filtering)
            if extension_version not in extension_stats:
                extension_stats[extension_version] = {
                    'matched': 0, 
                    'unmatched': 0, 
                    'supported': version_supported
                }
            
            # Skip unsupported versions (per GitHub docs minimum requirements)
            if not version_supported:
                unsupported_version += 1
                # Still track in extension stats
                extension_stats[extension_version]['unmatched'] += 1
                continue
            
            in_range += 1
            user_lower = login.lower()
            
            # Check day match (daily binary)
            day_key = (user_lower, activity_date)
            is_matched = day_key in json_day_keys
            
            if is_matched:
                matched += 1
                extension_stats[extension_version]['matched'] += 1
            else:
                unmatched += 1
                extension_stats[extension_version]['unmatched'] += 1
                if len(unmatched_samples) < 10 and day_key not in seen_user_days:
                    unmatched_samples.append({
                        'login': login,
                        'activity_date': activity_date,
                        'extension': extension_version,
                        'supported': version_supported
                    })
            
            seen_user_days.add(day_key)
    
    # Calculate percentages
    match_pct = (matched / in_range * 100) if in_range > 0 else 0
    
    results = {
        'company': company,
        'total_csv_records': total_csv_records,
        'no_activity_date': no_activity_date,
        'outside_range': outside_range,
        'unsupported_version': unsupported_version,
        'in_range': in_range,
        'matched': matched,
        'unmatched': unmatched,
        'match_percentage': match_pct,
        'json_day_pairs': len(json_day_keys),
        'json_min_date': min_date,
        'json_max_date': max_date,
        'unmatched_samples': unmatched_samples,
        'extension_stats': extension_stats
    }
    
    # Print summary
    print(f"\n  Results for {company}:")
    print(f"    Total CSV records:        {total_csv_records:,}")
    print(f"    No activity date:         {no_activity_date:,}")
    print(f"    Outside JSON date range:  {outside_range:,}")
    print(f"    Unsupported version:      {unsupported_version:,}")
    print(f"    In range ({min_date} to {max_date}): {in_range:,}")
    print(f"")
    print(f"    DAILY MATCHING (user + date):")
    print(f"      Matched in JSON:        {matched:,} ({match_pct:.1f}%)")
    print(f"      Not found in JSON:      {unmatched:,} ({100-match_pct:.1f}%)")
    
    # Print extension version breakdown (VS Code copilot-chat only)
    print(f"\n    VS CODE EXTENSION VERSIONS (copilot-chat, >100 entries):")
    print(f"    {'Version':<20} {'Supp':>5} {'Total':>8} {'Matched':>10} {'Missing':>10} {'Missing%':>10}")
    print(f"    {'-'*20} {'-'*5} {'-'*8} {'-'*10} {'-'*10} {'-'*10}")
    
    # Filter to copilot-chat only, exclude preview versions, require >100 entries
    def is_stable_version(ext: str) -> bool:
        version = ext.replace('copilot-chat/', '')
        parts = version.split('.')
        if len(parts) < 2:
            return False
        for part in parts[1:]:
            part = part.split('-')[0]
            if len(part) > 3:
                return False
        return True
    
    vscode_extensions = [
        (ext, stats) for ext, stats in extension_stats.items()
        if ext.startswith('copilot-chat/') and is_stable_version(ext)
        and (stats['matched'] + stats['unmatched']) > 100
    ]
    
    sorted_extensions = sorted(
        vscode_extensions,
        key=lambda x: parse_version(x[0].replace('copilot-chat/', '')),
        reverse=True
    )
    
    for ext, stats in sorted_extensions[:15]:  # Top 15 versions
        total = stats['matched'] + stats['unmatched']
        missing_pct = (stats['unmatched'] / total * 100) if total > 0 else 0
        version = ext.replace('copilot-chat/', '')
        supp = '✓' if stats['supported'] else '✗'
        print(f"    {version:<20} {supp:>5} {total:>8,} {stats['matched']:>10,} {stats['unmatched']:>10,} {missing_pct:>9.1f}%")
    
    if unmatched_samples:
        print(f"\n  Sample unmatched (user, day) pairs:")
        for sample in unmatched_samples[:5]:
            supp_marker = '✓' if sample['supported'] else '✗'
            print(f"    - {sample['login']} on {sample['activity_date']} ({sample['extension']}) [{supp_marker}]")
    
    return results


def generate_markdown_summary(all_results: list[dict], output_path: str) -> None:
    """
    Generate a markdown summary file from validation results.
    
    Args:
        all_results: List of result dicts from validate_company()
        output_path: Path to write the markdown file
    """
    lines = []
    
    # Header
    lines.append("# CSV vs JSON Validation Summary")
    lines.append("")
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    
    # Supported versions info
    lines.append("## Supported Extension Versions")
    lines.append("")
    lines.append("Only records with supported extension versions are included (per GitHub docs):")
    lines.append("")
    lines.append("| Extension | Minimum Version |")
    lines.append("|-----------|-----------------|")
    for ext, ver in sorted(MIN_EXTENSION_VERSIONS.items()):
        lines.append(f"| {ext} | {ver} |")
    lines.append("")
    
    # Overall summary (if multiple companies)
    if len(all_results) > 1:
        lines.append("## Overall Summary")
        lines.append("")
        
        total_csv = sum(r['total_csv_records'] for r in all_results)
        total_in_range = sum(r['in_range'] for r in all_results)
        total_matched = sum(r['matched'] for r in all_results)
        total_unmatched = sum(r['unmatched'] for r in all_results)
        match_pct = (total_matched / total_in_range * 100) if total_in_range > 0 else 0
        
        lines.append("| Metric | Count |")
        lines.append("|--------|-------|")
        lines.append(f"| Total CSV records | {total_csv:,} |")
        lines.append(f"| In JSON date range | {total_in_range:,} |")
        lines.append(f"| **Matched in JSON** | **{total_matched:,} ({match_pct:.1f}%)** |")
        lines.append(f"| Not found in JSON | {total_unmatched:,} ({100-match_pct:.1f}%) |")
        lines.append("")
    
    # Company comparison table
    lines.append("## Company Comparison")
    lines.append("")
    lines.append("| Company | CSV Records | In Range | Matched | Match % |")
    lines.append("|---------|-------------|----------|---------|---------|")
    
    for r in sorted(all_results, key=lambda x: x['match_percentage'], reverse=True):
        lines.append(
            f"| {r['company']} | {r['total_csv_records']:,} | {r['in_range']:,} | "
            f"{r['matched']:,} | {r['match_percentage']:.1f}% |"
        )
    lines.append("")
    
    # Per-company details
    for r in all_results:
        lines.append(f"## {r['company'].upper()}")
        lines.append("")
        lines.append(f"**Date Range:** {r['json_min_date']} to {r['json_max_date']}")
        lines.append("")
        
        lines.append("### Summary")
        lines.append("")
        lines.append("| Metric | Count |")
        lines.append("|--------|-------|")
        lines.append(f"| Total CSV records | {r['total_csv_records']:,} |")
        lines.append(f"| No activity date | {r['no_activity_date']:,} |")
        lines.append(f"| Outside JSON date range | {r['outside_range']:,} |")
        lines.append(f"| Unsupported version | {r['unsupported_version']:,} |")
        lines.append(f"| In range | {r['in_range']:,} |")
        lines.append(f"| **Matched in JSON** | **{r['matched']:,} ({r['match_percentage']:.1f}%)** |")
        lines.append(f"| Not found in JSON | {r['unmatched']:,} ({100-r['match_percentage']:.1f}%) |")
        lines.append("")
        
        # Extension breakdown (VS Code copilot-chat only, >100 entries)
        lines.append("### VS Code Extension Versions (copilot-chat, >100 entries)")
        lines.append("")
        lines.append("| Version | Supported | Total | Matched | Missing | Missing % |")
        lines.append("|---------|-----------|-------|---------|---------|-----------|")
        
        # Filter to copilot-chat only, exclude preview versions, require >100 entries
        def is_stable_version(ext: str) -> bool:
            version = ext.replace('copilot-chat/', '')
            parts = version.split('.')
            if len(parts) < 2:
                return False
            for part in parts[1:]:
                part = part.split('-')[0]
                if len(part) > 3:
                    return False
            return True
        
        vscode_extensions = [
            (ext, stats) for ext, stats in r['extension_stats'].items()
            if ext.startswith('copilot-chat/') and is_stable_version(ext)
            and (stats['matched'] + stats['unmatched']) > 100
        ]
        
        def version_sort_key(item):
            ext_str = item[0]
            version = ext_str.replace('copilot-chat/', '')
            if not version:
                return (0,)
            version = version.split('-')[0]
            import re
            parts = re.findall(r'\d+', version)
            return tuple(int(p) for p in parts) if parts else (0,)
        
        sorted_extensions = sorted(
            vscode_extensions,
            key=version_sort_key,
            reverse=True
        )
        
        for ext, stats in sorted_extensions[:15]:
            total = stats['matched'] + stats['unmatched']
            missing_pct = (stats['unmatched'] / total * 100) if total > 0 else 0
            version = ext.replace('copilot-chat/', '')
            supp = '✓' if stats['supported'] else '✗'
            lines.append(f"| {version} | {supp} | {total:,} | {stats['matched']:,} | {stats['unmatched']:,} | {missing_pct:.1f}% |")
        
        lines.append("")
    
    # Write to file
    with open(output_path, 'w') as f:
        f.write('\n'.join(lines))
    
    print(f"\nMarkdown summary written to: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description='Validate CSV activity data against JSON dashboard exports.'
    )
    parser.add_argument(
        'company',
        nargs='?',
        help='Company name (e.g., bofa, github) or --all for all companies'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Validate all companies'
    )
    parser.add_argument(
        '--consolidated-dir',
        default='consolidated-data',
        help='Path to consolidated data directory (default: consolidated-data)'
    )
    
    args = parser.parse_args()
    
    available = get_available_companies(args.consolidated_dir)
    
    if not available:
        print(f"Error: No companies with both CSV and JSON found in {args.consolidated_dir}")
        return 1
    
    print("\n=== SUPPORTED EXTENSION VERSIONS ===")
    print("Minimum versions (from GitHub docs):")
    for ext, ver in sorted(MIN_EXTENSION_VERSIONS.items()):
        print(f"  {ext}: {ver}")
    
    if args.all:
        companies = available
    elif args.company:
        if args.company not in available:
            print(f"Error: Company '{args.company}' not found or missing CSV/JSON files")
            print(f"Available companies: {', '.join(available)}")
            return 1
        companies = [args.company]
    else:
        print("Usage: python utils/validate_csv_vs_json.py <company> | --all")
        print(f"\nAvailable companies: {', '.join(available)}")
        return 1
    
    all_results = []
    for company in companies:
        result = validate_company(company, args.consolidated_dir)
        if 'error' not in result:
            all_results.append(result)
    
    # Generate markdown summary
    if all_results:
        output_path = os.path.join(args.consolidated_dir, "validation-summary.md")
        generate_markdown_summary(all_results, output_path)
    
    # Print overall summary
    if len(all_results) > 1:
        print(f"\n{'='*60}")
        print("OVERALL SUMMARY")
        print(f"{'='*60}")
        
        total_csv = sum(r['total_csv_records'] for r in all_results)
        total_in_range = sum(r['in_range'] for r in all_results)
        total_matched = sum(r['matched'] for r in all_results)
        total_unmatched = sum(r['unmatched'] for r in all_results)
        
        match_pct = (total_matched / total_in_range * 100) if total_in_range > 0 else 0
        
        print(f"Total CSV records:     {total_csv:,}")
        print(f"In JSON date range:    {total_in_range:,}")
        print(f"")
        print(f"DAILY MATCHING (user + date):")
        print(f"  Matched:             {total_matched:,} ({match_pct:.1f}%)")
        print(f"  Not found:           {total_unmatched:,} ({100-match_pct:.1f}%)")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
