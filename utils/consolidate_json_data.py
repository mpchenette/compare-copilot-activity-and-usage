#!/usr/bin/env python3
"""
Consolidate JSON data files for a given company into a single NDJSON file.

Usage:
    python utils/consolidate_data.py <company>
    python utils/consolidate_data.py --all

Examples:
    python utils/consolidate_data.py microsoft
    python utils/consolidate_data.py github
    python utils/consolidate_data.py --all
"""

import argparse
import glob
import json
import os
import sys


def get_available_companies(data_dir: str = 'data') -> list[str]:
    """Get list of companies with dashboard exports."""
    companies = []
    for company_dir in glob.glob(os.path.join(data_dir, '*')):
        if os.path.isdir(company_dir):
            exports_dir = os.path.join(company_dir, 'dashboard_exports')
            if os.path.isdir(exports_dir):
                json_files = glob.glob(os.path.join(exports_dir, '*.json'))
                if json_files:
                    companies.append(os.path.basename(company_dir))
    return sorted(companies)


def consolidate_company(company: str, data_dir: str = 'data', output_dir: str = 'consolidated-data') -> int:
    """
    Consolidate all JSON files for a company into a single NDJSON file.
    
    Returns the number of records consolidated.
    """
    exports_dir = os.path.join(data_dir, company, 'dashboard_exports')
    
    if not os.path.isdir(exports_dir):
        print(f"Error: No dashboard_exports directory found for {company}")
        return 0
    
    json_files = glob.glob(os.path.join(exports_dir, '*.json'))
    
    if not json_files:
        print(f"Error: No JSON files found for {company}")
        return 0
    
    os.makedirs(output_dir, exist_ok=True)
    
    output_file = os.path.join(output_dir, f'{company}-consolidated.json')
    
    # Collect all records, deduplicating by (user_login, day)
    records = {}  # key: (user_login, day) -> record
    
    print(f"\nConsolidating {company}...")
    print(f"  Found {len(json_files)} JSON files")
    
    for json_file in sorted(json_files):
        with open(json_file, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    user_login = record.get('user_login', '')
                    day = record.get('day', '')
                    
                    if user_login and day:
                        key = (user_login, day)
                        # Keep the most recent record (later files override earlier)
                        records[key] = record
                except json.JSONDecodeError as e:
                    print(f"  Warning: Skipping invalid JSON in {os.path.basename(json_file)}: {e}")
                    continue
    
    # Write consolidated file
    with open(output_file, 'w') as f:
        # Sort by day, then user_login for consistent output
        for key in sorted(records.keys(), key=lambda x: (x[1], x[0])):
            f.write(json.dumps(records[key]) + '\n')
    
    print(f"  Consolidated {len(records):,} unique records")
    print(f"  Output: {output_file}")
    
    return len(records)


def main():
    parser = argparse.ArgumentParser(
        description='Consolidate JSON data files for a company into a single file.'
    )
    parser.add_argument(
        'company',
        nargs='?',
        help='Company name (e.g., microsoft, github) or --all for all companies'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Consolidate data for all companies'
    )
    parser.add_argument(
        '--data-dir',
        default='data',
        help='Path to data directory (default: data)'
    )
    parser.add_argument(
        '--output-dir',
        default='consolidated-data',
        help='Path to output directory (default: consolidated-data)'
    )
    
    args = parser.parse_args()
    
    available = get_available_companies(args.data_dir)
    
    if not available:
        print(f"Error: No companies found in {args.data_dir}")
        return 1
    
    if args.all:
        companies = available
    elif args.company:
        if args.company not in available:
            print(f"Error: Company '{args.company}' not found")
            print(f"Available companies: {', '.join(available)}")
            return 1
        companies = [args.company]
    else:
        print("Usage: python utils/consolidate_data.py <company> | --all")
        print(f"\nAvailable companies: {', '.join(available)}")
        return 1
    
    total_records = 0
    for company in companies:
        total_records += consolidate_company(company, args.data_dir, args.output_dir)
    
    print(f"\n{'='*60}")
    print(f"Total: {total_records:,} records across {len(companies)} companies")
    print(f"Output directory: {args.output_dir}")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
