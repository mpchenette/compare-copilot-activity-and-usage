#!/usr/bin/env python3
"""
Cohort Tracking Analysis

Tracks absent users across multiple snapshots to determine:
- Recovery rates over time (do users eventually appear in JSON?)
- True permanent loss rate (data that never arrives)
- Trends in data pipeline reliability

Usage:
    python track_cohorts.py --customer bofa
    
    This will look for directories matching: bofa-data-v1, bofa-data-v2, etc.
    Or: bofa-data/2025-12-17, bofa-data/2025-12-18, etc.

Directory structure expected:
    <customer>-data-v1/
        *.json
        *.csv (activity report)
        output/
            discrepancies.csv
    <customer>-data-v2/
        ...
"""

import argparse
import csv
import json
import glob
import os
import re
from datetime import datetime, timedelta
from collections import defaultdict


def find_snapshot_dirs(base_path, customer):
    """Find all snapshot directories for a customer."""
    snapshots = []
    
    # Pattern 1: customer-data-v1, customer-data-v2, etc.
    pattern1 = glob.glob(os.path.join(base_path, f'{customer}-data-v*'))
    
    # Pattern 2: customer-data/2025-12-17, etc.
    pattern2 = glob.glob(os.path.join(base_path, f'{customer}-data', '2025-*'))
    
    dirs = pattern1 + pattern2
    
    for d in dirs:
        if os.path.isdir(d):
            # Try to extract date or version
            disc_path = os.path.join(d, 'output', 'discrepancies.csv')
            if os.path.exists(disc_path):
                # Get activity report date from CSV filename
                csv_files = glob.glob(os.path.join(d, '*seat-activity*.csv'))
                if csv_files:
                    # Extract date from activity report
                    with open(csv_files[0], 'r') as f:
                        reader = csv.DictReader(f)
                        row = next(reader, None)
                        if row and 'Report Time' in row:
                            report_date = row['Report Time'][:10]
                            snapshots.append({
                                'dir': d,
                                'date': report_date,
                                'disc_path': disc_path,
                                'activity_path': csv_files[0]
                            })
    
    # Sort by date
    snapshots.sort(key=lambda x: x['date'])
    return snapshots


def load_discrepancies(disc_path):
    """Load discrepancies with full details."""
    users = {}
    with open(disc_path, 'r') as f:
        for row in csv.DictReader(f):
            login = row.get('Login', '')
            users[login] = {
                'status': row.get('Status', ''),
                'last_activity': row.get('Last Activity At', ''),
                'surface': row.get('Report Surface', ''),
            }
    return users


def load_activity_report(activity_path):
    """Load activity report to get user timestamps."""
    users = {}
    with open(activity_path, 'r') as f:
        for row in csv.DictReader(f):
            login = row.get('Login', '')
            users[login] = {
                'last_activity': row.get('Last Activity At', ''),
                'surface': row.get('Last Surface Used', ''),
            }
    return users


def load_json_users(data_dir):
    """Load all unique users from JSON files."""
    users = set()
    json_files = glob.glob(os.path.join(data_dir, '*.json'))
    
    for json_path in json_files:
        try:
            with open(json_path, 'r') as f:
                content = f.read().strip()
            
            try:
                data = json.loads(content)
                if isinstance(data, list):
                    for record in data:
                        if 'user_login' in record:
                            users.add(record['user_login'])
                        elif 'assignee' in record and 'login' in record['assignee']:
                            users.add(record['assignee']['login'])
            except json.JSONDecodeError:
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
            pass
    
    return users


def analyze_cohorts(snapshots):
    """Analyze cohorts of absent users across snapshots."""
    
    if len(snapshots) < 2:
        print("Need at least 2 snapshots for cohort analysis")
        return None
    
    results = {
        'snapshots': [],
        'cohorts': [],
        'recovery_matrix': {},
    }
    
    # Load all snapshot data
    snapshot_data = []
    for snap in snapshots:
        disc = load_discrepancies(snap['disc_path'])
        activity = load_activity_report(snap['activity_path'])
        json_users = load_json_users(snap['dir'])
        
        absent = {u: d for u, d in disc.items() if 'Missing' in d['status']}
        stale = {u: d for u, d in disc.items() if 'Timestamp' in d['status']}
        
        snapshot_data.append({
            'date': snap['date'],
            'dir': snap['dir'],
            'discrepancies': disc,
            'activity': activity,
            'json_users': json_users,
            'absent': absent,
            'stale': stale,
        })
        
        results['snapshots'].append({
            'date': snap['date'],
            'total_disc': len(disc),
            'absent': len(absent),
            'stale': len(stale),
        })
    
    # Track cohorts: for each snapshot, track where those absent users end up
    for i, snap in enumerate(snapshot_data):
        cohort = {
            'origin_date': snap['date'],
            'origin_absent_count': len(snap['absent']),
            'origin_users': set(snap['absent'].keys()),
            'tracking': [],
        }
        
        # Track this cohort through subsequent snapshots
        for j in range(i, len(snapshot_data)):
            future_snap = snapshot_data[j]
            days_later = j - i
            
            # Check status of cohort users in this future snapshot
            still_absent = 0
            now_stale = 0
            now_ok = 0
            now_in_json = 0
            had_new_activity = 0
            
            for user in cohort['origin_users']:
                # Is user still in discrepancies?
                if user in future_snap['absent']:
                    still_absent += 1
                elif user in future_snap['stale']:
                    now_stale += 1
                else:
                    now_ok += 1
                
                # Is user now in JSON?
                if user in future_snap['json_users']:
                    now_in_json += 1
                
                # Did user have new activity since cohort origin?
                if user in future_snap['activity']:
                    future_ts = future_snap['activity'][user]['last_activity'][:10]
                    origin_ts = snap['absent'].get(user, {}).get('last_activity', '')[:10]
                    if future_ts > origin_ts:
                        had_new_activity += 1
            
            total = len(cohort['origin_users'])
            cohort['tracking'].append({
                'date': future_snap['date'],
                'days_later': days_later,
                'still_absent': still_absent,
                'still_absent_pct': still_absent / total * 100 if total > 0 else 0,
                'now_stale': now_stale,
                'now_ok': now_ok,
                'recovered_pct': (now_ok + now_stale) / total * 100 if total > 0 else 0,
                'now_in_json': now_in_json,
                'in_json_pct': now_in_json / total * 100 if total > 0 else 0,
                'had_new_activity': had_new_activity,
            })
        
        results['cohorts'].append(cohort)
    
    return results


def print_report(results, customer):
    """Print cohort tracking report."""
    
    print("=" * 70)
    print(f"{customer.upper()} - COHORT TRACKING ANALYSIS")
    print("=" * 70)
    
    print(f"\nSnapshots analyzed: {len(results['snapshots'])}")
    print(f"Date range: {results['snapshots'][0]['date']} to {results['snapshots'][-1]['date']}")
    
    # Summary table
    print("\n" + "-" * 70)
    print("SNAPSHOT SUMMARY")
    print("-" * 70)
    print(f"\n  {'Date':<12} {'Total Disc':>12} {'Absent':>10} {'Stale':>10}")
    print(f"  {'-'*12} {'-'*12} {'-'*10} {'-'*10}")
    for snap in results['snapshots']:
        print(f"  {snap['date']:<12} {snap['total_disc']:>12,} {snap['absent']:>10,} {snap['stale']:>10,}")
    
    # Cohort tracking
    print("\n" + "-" * 70)
    print("COHORT RECOVERY TRACKING")
    print("-" * 70)
    print("\nEach cohort = users who were ABSENT on a given date")
    print("Tracking shows what happened to them in subsequent snapshots\n")
    
    for cohort in results['cohorts']:
        if len(cohort['tracking']) < 2:
            continue
            
        print(f"\n  Cohort: {cohort['origin_date']} ({cohort['origin_absent_count']:,} absent users)")
        print(f"  {'-'*60}")
        print(f"  {'Days Later':>10} {'Still Absent':>14} {'Recovered':>12} {'In JSON':>12} {'New Activity':>12}")
        
        for track in cohort['tracking']:
            if track['days_later'] == 0:
                continue
            print(f"  {'+' + str(track['days_later']) + ' day(s)':<10} "
                  f"{track['still_absent']:>8,} ({track['still_absent_pct']:>4.1f}%) "
                  f"{track['now_ok'] + track['now_stale']:>6,} ({track['recovered_pct']:>4.1f}%) "
                  f"{track['now_in_json']:>6,} ({track['in_json_pct']:>4.1f}%) "
                  f"{track['had_new_activity']:>8,}")
    
    # Recovery rate summary
    print("\n" + "-" * 70)
    print("RECOVERY RATE ANALYSIS")
    print("-" * 70)
    
    # Calculate average recovery rates by days elapsed
    recovery_by_day = defaultdict(list)
    json_arrival_by_day = defaultdict(list)
    
    for cohort in results['cohorts']:
        for track in cohort['tracking']:
            if track['days_later'] > 0:
                recovery_by_day[track['days_later']].append(track['recovered_pct'])
                json_arrival_by_day[track['days_later']].append(track['in_json_pct'])
    
    if recovery_by_day:
        print("\n  Average recovery rate by days elapsed:")
        print(f"  {'Days':>6} {'Avg Recovery %':>16} {'Avg In JSON %':>16} {'Samples':>10}")
        print(f"  {'-'*6} {'-'*16} {'-'*16} {'-'*10}")
        
        for day in sorted(recovery_by_day.keys()):
            avg_recovery = sum(recovery_by_day[day]) / len(recovery_by_day[day])
            avg_json = sum(json_arrival_by_day[day]) / len(json_arrival_by_day[day])
            samples = len(recovery_by_day[day])
            print(f"  {day:>6} {avg_recovery:>15.1f}% {avg_json:>15.1f}% {samples:>10}")
    
    # Key insights
    print("\n" + "-" * 70)
    print("KEY INSIGHTS")
    print("-" * 70)
    
    if len(results['cohorts']) >= 1 and len(results['cohorts'][0]['tracking']) >= 2:
        first_cohort = results['cohorts'][0]
        latest_track = first_cohort['tracking'][-1]
        
        days_tracked = latest_track['days_later']
        final_recovery = latest_track['recovered_pct']
        final_in_json = latest_track['in_json_pct']
        
        print(f"\n  Oldest cohort ({first_cohort['origin_date']}) tracked for {days_tracked} day(s):")
        print(f"    - Recovery rate: {final_recovery:.1f}%")
        print(f"    - Now in JSON: {final_in_json:.1f}%")
        print(f"    - Still missing: {100 - final_recovery:.1f}%")
        
        if days_tracked >= 4:
            if final_recovery < 20:
                print(f"\n  ⚠️  Low recovery rate after {days_tracked} days suggests DATA LOSS, not delay")
            elif final_recovery > 80:
                print(f"\n  ✓ High recovery rate suggests DELAYED data arriving")
            else:
                print(f"\n  ⚡ Mixed recovery - some data lost, some delayed")
    
    if len(results['snapshots']) < 5:
        print(f"\n  📊 Only {len(results['snapshots'])} snapshots - need 5+ for confident trends")
        print(f"     Continue collecting daily snapshots for better analysis")


def main():
    parser = argparse.ArgumentParser(
        description='Track cohorts of absent users across multiple snapshots.',
    )
    
    parser.add_argument('--customer', '-c', required=True,
                        help='Customer name prefix (e.g., "bofa" for bofa-data-v1, bofa-data-v2)')
    parser.add_argument('--base-path', '-p', default='.',
                        help='Base path to look for snapshot directories (default: current dir)')
    
    args = parser.parse_args()
    
    # Find snapshot directories
    snapshots = find_snapshot_dirs(args.base_path, args.customer)
    
    if not snapshots:
        print(f"No snapshot directories found for customer '{args.customer}'")
        print(f"Looking for: {args.customer}-data-v1, {args.customer}-data-v2, etc.")
        print(f"Or: {args.customer}-data/2025-12-17, etc.")
        return 1
    
    print(f"Found {len(snapshots)} snapshots for {args.customer}")
    for snap in snapshots:
        print(f"  - {snap['date']}: {snap['dir']}")
    
    # Run the main analyzer on any directories missing output
    for snap in snapshots:
        if not os.path.exists(snap['disc_path']):
            print(f"\nRunning analyzer on {snap['dir']}...")
            os.system(f"python3 analyze_copilot_data.py --data-dir {snap['dir']}")
    
    # Analyze cohorts
    results = analyze_cohorts(snapshots)
    
    if results:
        print("\n")
        print_report(results, args.customer)
    
    return 0


if __name__ == '__main__':
    exit(main())
