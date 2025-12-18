#!/usr/bin/env python3
"""
Compare Discrepancy Snapshots

Compares two discrepancies.csv files from different analysis runs to determine
if the same users are affected (systematic) or different users (transient).

Usage:
    python compare_snapshots.py --old <old_discrepancies.csv> --new <new_discrepancies.csv>
    
    # Or compare full data directories (will also check if old absent users now appear in new JSON)
    python compare_snapshots.py --old-dir <old_data_dir> --new-dir <new_data_dir>
"""

import argparse
import csv
import json
import glob
import os
from collections import defaultdict
from datetime import datetime


def load_discrepancies(csv_path):
    """Load discrepancies from CSV file."""
    discrepancies = {}
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            login = row.get('Login', '')
            status = row.get('Status', '')
            discrepancies[login] = {
                'status': status,
                'last_activity': row.get('Last Activity At', ''),
                'latest_export': row.get('Latest Export Activity', ''),
                'surface': row.get('Report Surface', ''),
            }
    return discrepancies


def load_json_users(data_dir):
    """Load all unique users from JSON files in a directory."""
    users = set()
    json_files = glob.glob(os.path.join(data_dir, '*.json'))
    
    for json_path in json_files:
        try:
            with open(json_path, 'r') as f:
                content = f.read().strip()
                
            # Try parsing as regular JSON first
            try:
                data = json.loads(content)
                if isinstance(data, list):
                    for record in data:
                        # Handle both formats: user_login (flat) and assignee.login (nested)
                        if 'user_login' in record:
                            users.add(record['user_login'])
                        elif 'assignee' in record and 'login' in record['assignee']:
                            users.add(record['assignee']['login'])
                elif isinstance(data, dict):
                    if 'user_login' in data:
                        users.add(data['user_login'])
                    elif 'assignee' in data and 'login' in data['assignee']:
                        users.add(data['assignee']['login'])
            except json.JSONDecodeError:
                # Try NDJSON format
                for line in content.split('\n'):
                    if line.strip():
                        try:
                            record = json.loads(line)
                            if 'user_login' in record:
                                users.add(record['user_login'])
                            elif 'assignee' in record and 'login' in record['assignee']:
                                users.add(record['assignee']['login'])
                        except:
                            pass
        except Exception as e:
            print(f"Warning: Could not parse {json_path}: {e}")
    
    return users


def compare_discrepancies(old_disc, new_disc, old_json_users=None, new_json_users=None):
    """Compare two sets of discrepancies."""
    
    old_users = set(old_disc.keys())
    new_users = set(new_disc.keys())
    
    # Basic set operations
    still_affected = old_users & new_users
    recovered = old_users - new_users
    new_issues = new_users - old_users
    
    # Break down by status
    old_absent = {u for u, d in old_disc.items() if 'Missing' in d['status']}
    old_stale = {u for u, d in old_disc.items() if 'Timestamp' in d['status']}
    new_absent = {u for u, d in new_disc.items() if 'Missing' in d['status']}
    new_stale = {u for u, d in new_disc.items() if 'Timestamp' in d['status']}
    
    results = {
        'old_total': len(old_users),
        'new_total': len(new_users),
        'still_affected': len(still_affected),
        'recovered': len(recovered),
        'new_issues': len(new_issues),
        
        'old_absent': len(old_absent),
        'old_stale': len(old_stale),
        'new_absent': len(new_absent),
        'new_stale': len(new_stale),
        
        # Status transitions
        'absent_to_absent': len(old_absent & new_absent),
        'absent_to_stale': len(old_absent & new_stale),
        'absent_to_ok': len(old_absent - new_users),
        'stale_to_stale': len(old_stale & new_stale),
        'stale_to_absent': len(old_stale & new_absent),
        'stale_to_ok': len(old_stale - new_users),
        
        'still_affected_users': still_affected,
        'recovered_users': recovered,
        'new_issues_users': new_issues,
    }
    
    # If we have JSON user data, check if old absent users now appear in new JSON
    if new_json_users and old_absent:
        old_absent_now_in_json = old_absent & new_json_users
        results['old_absent_now_in_json'] = len(old_absent_now_in_json)
        results['old_absent_now_in_json_users'] = old_absent_now_in_json
    
    return results


def print_report(results, old_path, new_path):
    """Print comparison report."""
    
    print("=" * 60)
    print("DISCREPANCY SNAPSHOT COMPARISON")
    print("=" * 60)
    print(f"\nOld: {old_path}")
    print(f"New: {new_path}")
    
    print("\n" + "-" * 60)
    print("SUMMARY")
    print("-" * 60)
    
    print(f"\nOld snapshot: {results['old_total']:,} discrepancies")
    print(f"  + Absent: {results['old_absent']:,}")
    print(f"  + Stale: {results['old_stale']:,}")
    
    print(f"\nNew snapshot: {results['new_total']:,} discrepancies")
    print(f"  + Absent: {results['new_absent']:,}")
    print(f"  + Stale: {results['new_stale']:,}")
    
    print("\n" + "-" * 60)
    print("USER OVERLAP ANALYSIS")
    print("-" * 60)
    
    if results['old_total'] > 0:
        overlap_pct = results['still_affected'] / results['old_total'] * 100
        recovery_pct = results['recovered'] / results['old_total'] * 100
    else:
        overlap_pct = 0
        recovery_pct = 0
    
    print(f"\nStill affected (in both): {results['still_affected']:,} ({overlap_pct:.1f}%)")
    print(f"Recovered (old only):     {results['recovered']:,} ({recovery_pct:.1f}%)")
    print(f"New issues (new only):    {results['new_issues']:,}")
    
    # Visual bar for overlap
    bar_width = 40
    filled = int(overlap_pct / 100 * bar_width)
    bar = '█' * filled + '░' * (bar_width - filled)
    print(f"\nOverlap: |{bar}| {overlap_pct:.1f}%")
    
    print("\n" + "-" * 60)
    print("STATUS TRANSITIONS")
    print("-" * 60)
    
    print(f"\nFrom ABSENT ({results['old_absent']:,} users):")
    print(f"  → Still absent:  {results['absent_to_absent']:,}")
    print(f"  → Now stale:     {results['absent_to_stale']:,}")
    print(f"  → Now OK:        {results['absent_to_ok']:,}")
    
    print(f"\nFrom STALE ({results['old_stale']:,} users):")
    print(f"  → Still stale:   {results['stale_to_stale']:,}")
    print(f"  → Now absent:    {results['stale_to_absent']:,}")
    print(f"  → Now OK:        {results['stale_to_ok']:,}")
    
    # JSON recovery check
    if 'old_absent_now_in_json' in results:
        print("\n" + "-" * 60)
        print("JSON DATA ARRIVAL CHECK")
        print("-" * 60)
        print(f"\nOf {results['old_absent']:,} users absent from old JSON:")
        print(f"  Now appear in new JSON: {results['old_absent_now_in_json']:,}")
        
        if results['old_absent'] > 0:
            arrival_pct = results['old_absent_now_in_json'] / results['old_absent'] * 100
            print(f"  Data arrival rate: {arrival_pct:.1f}%")
    
    print("\n" + "-" * 60)
    print("INTERPRETATION")
    print("-" * 60)
    
    if overlap_pct > 70:
        print("\n⚠️  HIGH OVERLAP (>70%): Same users consistently affected.")
        print("   This suggests a SYSTEMATIC issue - possibly user-specific")
        print("   configuration, network, or account problems.")
    elif overlap_pct > 30:
        print("\n⚡ MODERATE OVERLAP (30-70%): Mix of persistent and transient issues.")
        print("   Some users have ongoing problems, others are random drops.")
    else:
        print("\n✓ LOW OVERLAP (<30%): Different users affected each time.")
        print("   This is consistent with TRANSIENT/RANDOM data pipeline issues.")
    
    if results['recovered'] > results['still_affected']:
        print("\n✓ More users recovered than remained affected - data is catching up.")
    
    if 'old_absent_now_in_json' in results and results['old_absent'] > 0:
        arrival_pct = results['old_absent_now_in_json'] / results['old_absent'] * 100
        if arrival_pct > 50:
            print(f"\n✓ {arrival_pct:.0f}% of previously absent users now in JSON.")
            print("   Data is arriving with delay - this is a LATENCY issue, not data loss.")


def main():
    parser = argparse.ArgumentParser(
        description='Compare two discrepancy snapshots to analyze user overlap.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument('--old', help='Path to old discrepancies.csv')
    parser.add_argument('--new', help='Path to new discrepancies.csv')
    parser.add_argument('--old-dir', help='Path to old data directory (will find discrepancies.csv and JSON files)')
    parser.add_argument('--new-dir', help='Path to new data directory (will find discrepancies.csv and JSON files)')
    
    args = parser.parse_args()
    
    # Determine paths
    if args.old_dir and args.new_dir:
        old_csv = os.path.join(args.old_dir, 'output', 'discrepancies.csv')
        new_csv = os.path.join(args.new_dir, 'output', 'discrepancies.csv')
        old_json_users = None  # Not needed for old
        new_json_users = load_json_users(args.new_dir)
        print(f"Loaded {len(new_json_users):,} users from new JSON files")
    elif args.old and args.new:
        old_csv = args.old
        new_csv = args.new
        old_json_users = None
        new_json_users = None
    else:
        parser.error("Must provide either --old and --new, or --old-dir and --new-dir")
    
    # Validate files exist
    if not os.path.exists(old_csv):
        print(f"Error: Old file not found: {old_csv}")
        return 1
    if not os.path.exists(new_csv):
        print(f"Error: New file not found: {new_csv}")
        return 1
    
    # Load and compare
    old_disc = load_discrepancies(old_csv)
    new_disc = load_discrepancies(new_csv)
    
    results = compare_discrepancies(old_disc, new_disc, old_json_users, new_json_users)
    print_report(results, old_csv, new_csv)
    
    return 0


if __name__ == '__main__':
    exit(main())
