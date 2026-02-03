#!/usr/bin/env python3
"""
Consolidate CSV activity report files for a given company into a single CSV file.

Usage:
    python utils/consolidate_csv_data.py <company>
    python utils/consolidate_csv_data.py --all

Examples:
    python utils/consolidate_csv_data.py microsoft
    python utils/consolidate_csv_data.py github
    python utils/consolidate_csv_data.py --all
"""

import argparse
import csv
import glob
import os
import sys
from datetime import datetime


def get_available_companies(data_dir: str = 'data') -> list[str]:
    """Get list of companies with activity reports."""
    companies = []
    for company_dir in glob.glob(os.path.join(data_dir, '*')):
        if os.path.isdir(company_dir):
            reports_dir = os.path.join(company_dir, 'activity_reports')
            if os.path.isdir(reports_dir):
                csv_files = glob.glob(os.path.join(reports_dir, '*.csv'))
                if csv_files:
                    companies.append(os.path.basename(company_dir))
    return sorted(companies)


def parse_datetime(dt_str: str) -> datetime | None:
    """Parse datetime string, handling various formats and None values."""
    if not dt_str or dt_str.lower() == 'none':
        return None
    try:
        # Handle ISO format with Z suffix
        if dt_str.endswith('Z'):
            dt_str = dt_str[:-1] + '+00:00'
        return datetime.fromisoformat(dt_str)
    except ValueError:
        return None


def consolidate_company(company: str, data_dir: str = 'data', output_dir: str = 'consolidated-data') -> int:
    """
    Consolidate all CSV files for a company into a single CSV file.
    
    Deduplicates by (Login, Last Activity At), preserving all unique activity 
    timestamps per user along with their associated IDE/extension version info.
    
    Returns the number of records consolidated.
    """
    reports_dir = os.path.join(data_dir, company, 'activity_reports')
    
    if not os.path.isdir(reports_dir):
        print(f"Error: No activity_reports directory found for {company}")
        return 0
    
    csv_files = glob.glob(os.path.join(reports_dir, '*.csv'))
    
    if not csv_files:
        print(f"Error: No CSV files found for {company}")
        return 0
    
    os.makedirs(output_dir, exist_ok=True)
    
    output_file = os.path.join(output_dir, f'{company}-consolidated.csv')
    
    # Collect all records, deduplicating by (Login, Last Activity At)
    # This preserves all unique activity timestamps per user
    records: dict[tuple[str, str], dict] = {}  # key: (Login, Last Activity At) -> record dict
    all_fieldnames: set[str] = set()
    
    print(f"\nConsolidating {company}...")
    print(f"  Found {len(csv_files)} CSV files")
    
    for csv_file in sorted(csv_files):
        try:
            with open(csv_file, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                if reader.fieldnames:
                    all_fieldnames.update(reader.fieldnames)
                
                for row in reader:
                    login = row.get('Login', '').strip()
                    last_activity = row.get('Last Activity At', '').strip()
                    if not login:
                        continue
                    
                    # Use (Login, Last Activity At) as the dedup key
                    key = (login, last_activity)
                    
                    # If duplicate key, keep the one with most recent Report Time
                    if key in records:
                        existing_dt = parse_datetime(records[key].get('Report Time', ''))
                        current_dt = parse_datetime(row.get('Report Time', ''))
                        if current_dt and existing_dt and current_dt > existing_dt:
                            records[key] = dict(row)
                        elif current_dt and not existing_dt:
                            records[key] = dict(row)
                    else:
                        records[key] = dict(row)
                        
        except Exception as e:
            print(f"  Warning: Error reading {os.path.basename(csv_file)}: {e}")
            continue
    
    if not records:
        print(f"  No records found for {company}")
        return 0
    
    # Define column order (common columns first, then any extras)
    preferred_order = ['Report Time', 'Login', 'Last Authenticated At', 'Last Activity At', 'Last Surface Used', 'Organization']
    fieldnames = [col for col in preferred_order if col in all_fieldnames]
    fieldnames.extend(sorted(col for col in all_fieldnames if col not in preferred_order))
    
    # Write consolidated file
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        
        # Sort by Login, then by Last Activity At for consistent output
        for key in sorted(records.keys(), key=lambda x: (x[0].lower(), x[1] or '')):
            writer.writerow(records[key])
    
    print(f"  Consolidated {len(records):,} unique activity records")
    print(f"  Output: {output_file}")
    
    return len(records)


def main():
    parser = argparse.ArgumentParser(
        description='Consolidate CSV activity report files for a company into a single file.'
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
        print(f"Error: No companies with activity reports found in {args.data_dir}")
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
        print("Usage: python utils/consolidate_csv_data.py <company> | --all")
        print(f"\nAvailable companies: {', '.join(available)}")
        return 1
    
    total_records = 0
    for company in companies:
        total_records += consolidate_company(company, args.data_dir, args.output_dir)
    
    print(f"\n{'='*60}")
    print(f"Total: {total_records:,} unique activity records across {len(companies)} companies")
    print(f"Output directory: {args.output_dir}")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
