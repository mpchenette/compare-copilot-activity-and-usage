#!/usr/bin/env python3
"""Quick analysis of user_initiated_interaction_count for healthy vs stale users."""

import json
import csv
import glob
import sys
from collections import defaultdict

def main(data_dir):
    # Load JSON and sum user_initiated_interaction_count per user
    user_interactions = defaultdict(int)
    json_files = glob.glob(f'{data_dir}/*.json')

    for json_path in json_files:
        with open(json_path, 'r') as f:
            for line in f:
                if line.strip():
                    try:
                        record = json.loads(line)
                        login = record.get('user_login', '')
                        interactions = record.get('user_initiated_interaction_count', 0)
                        if login and interactions:
                            user_interactions[login] += interactions
                    except:
                        pass

    print(f"Users in JSON with interaction data: {len(user_interactions):,}")
    print(f"Total interactions: {sum(user_interactions.values()):,}")

    # Load discrepancies to identify stale users
    stale_users = set()
    absent_users = set()
    disc_path = f'{data_dir}/output/discrepancies.csv'
    
    with open(disc_path, 'r') as f:
        for row in csv.DictReader(f):
            if 'Missing' in row.get('Status', ''):
                absent_users.add(row['Login'])
            elif 'Timestamp' in row.get('Status', ''):
                stale_users.add(row['Login'])

    # Categorize users
    healthy_users = set(user_interactions.keys()) - stale_users - absent_users

    print(f"\nUsers by status:")
    print(f"  Healthy (in JSON, no discrepancy): {len(healthy_users):,}")
    print(f"  Stale (in JSON, timestamp mismatch): {len(stale_users):,}")
    print(f"  Absent (not in JSON): {len(absent_users):,}")

    # Compare interaction counts
    healthy_interactions = [user_interactions[u] for u in healthy_users if u in user_interactions]
    stale_interactions = [user_interactions[u] for u in stale_users if u in user_interactions]

    print(f"\n" + "=" * 60)
    print("INTERACTION COUNT ANALYSIS")
    print("=" * 60)
    
    print(f"\nHealthy users ({len(healthy_interactions):,}):")
    print(f"  Total interactions: {sum(healthy_interactions):,}")
    print(f"  Avg per user: {sum(healthy_interactions)/len(healthy_interactions):.1f}")
    print(f"  Min: {min(healthy_interactions)}, Max: {max(healthy_interactions)}")
    print(f"  Median: {sorted(healthy_interactions)[len(healthy_interactions)//2]}")

    print(f"\nStale users ({len(stale_interactions):,}):")
    print(f"  Total interactions: {sum(stale_interactions):,}")
    print(f"  Avg per user: {sum(stale_interactions)/len(stale_interactions):.1f}")
    print(f"  Min: {min(stale_interactions)}, Max: {max(stale_interactions)}")
    print(f"  Median: {sorted(stale_interactions)[len(stale_interactions)//2]}")

    # Distribution by interaction buckets
    print(f"\n" + "=" * 60)
    print("STALE RATE BY INTERACTION COUNT")
    print("=" * 60)
    print("\nIf random failure, higher interaction users should have LOWER stale rates")
    
    def bucket_interactions(count):
        if count <= 5: return '1-5'
        elif count <= 20: return '6-20'
        elif count <= 50: return '21-50'
        elif count <= 100: return '51-100'
        elif count <= 500: return '101-500'
        else: return '500+'

    healthy_buckets = defaultdict(int)
    stale_buckets = defaultdict(int)

    for c in healthy_interactions:
        healthy_buckets[bucket_interactions(c)] += 1
    for c in stale_interactions:
        stale_buckets[bucket_interactions(c)] += 1

    print(f"\n{'Interactions':>12} {'Healthy':>10} {'Stale':>10} {'Stale Rate':>12}")
    print(f"{'-'*12} {'-'*10} {'-'*10} {'-'*12}")

    rates = []
    for bucket in ['1-5', '6-20', '21-50', '51-100', '101-500', '500+']:
        h = healthy_buckets.get(bucket, 0)
        s = stale_buckets.get(bucket, 0)
        total = h + s
        rate = s / total * 100 if total > 0 else 0
        rates.append((bucket, rate, total))
        print(f"{bucket:>12} {h:>10,} {s:>10,} {rate:>11.1f}%")

    # Interpretation
    print(f"\n" + "=" * 60)
    print("INTERPRETATION")
    print("=" * 60)
    
    # Check if rates decrease with more interactions (supports random failure)
    decreasing = all(rates[i][1] >= rates[i+1][1] for i in range(len(rates)-1) if rates[i][2] > 10 and rates[i+1][2] > 10)
    
    low_interaction_rate = rates[0][1] if rates[0][2] > 0 else 0
    high_interaction_rate = rates[-1][1] if rates[-1][2] > 0 else 0
    
    print(f"\n  Low interaction users (1-5):   {low_interaction_rate:.1f}% stale rate")
    print(f"  High interaction users (500+): {high_interaction_rate:.1f}% stale rate")
    
    if low_interaction_rate > high_interaction_rate * 2:
        print(f"\n  ✓ Pattern SUPPORTS random failure hypothesis:")
        print(f"    Users with more interactions are less likely to be stale.")
        print(f"    This is consistent with random per-event data drops.")
    elif abs(low_interaction_rate - high_interaction_rate) < 2:
        print(f"\n  ⚠️ Pattern suggests SYSTEMATIC issue:")
        print(f"    Stale rate is similar regardless of interaction count.")
        print(f"    Something other than random drops is causing this.")
    else:
        print(f"\n  ⚡ Mixed pattern - inconclusive")

if __name__ == '__main__':
    data_dir = sys.argv[1] if len(sys.argv) > 1 else './bofa-data-v2'
    main(data_dir)
