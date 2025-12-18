#!/usr/bin/env python3
"""Analyze version patterns in discrepancies."""

import csv
from collections import defaultdict
import re
import sys

# Minimum supported versions (same as main script)
MIN_VERSIONS = {
    'vscode': {'ide': (1, 101), 'extension': (0, 28, 0)},
    'visualstudio': {'ide': (17, 14, 13), 'extension': (18, 0, 471)},
    'jetbrains': {'ide': (2024, 2, 6), 'extension': (1, 5, 52)},
    'eclipse': {'ide': (4, 31), 'extension': (0, 9, 3)},
    'xcode': {'ide': (13, 2, 1), 'extension': (0, 40, 0)},
}

def parse_version(version_str):
    if not version_str:
        return None
    match = re.match(r'^(\d+)(?:\.(\d+))?(?:\.(\d+))?', version_str)
    if not match:
        return None
    parts = [int(p) for p in match.groups() if p is not None]
    return tuple(parts) if parts else None

def is_supported(surface_str):
    """Check if version meets minimum requirements."""
    if not surface_str:
        return False
    
    parts = surface_str.split('/')
    if len(parts) < 2:
        return False
    
    ide_name = parts[0].lower()
    ide_version_str = parts[1] if len(parts) > 1 else ''
    ext_version_str = parts[3] if len(parts) > 3 else ''
    
    # Determine which minimum version set to use
    if ide_name in ['vscode', 'vscode-chat']:
        min_ver = MIN_VERSIONS.get('vscode')
    elif ide_name.startswith('jetbrains-') or ide_name in ['eclipse ide', 'eclipse']:
        min_ver = MIN_VERSIONS.get('jetbrains')
    elif ide_name in ['visualstudio', 'vs']:
        min_ver = MIN_VERSIONS.get('visualstudio')
    elif ide_name == 'xcode':
        min_ver = MIN_VERSIONS.get('xcode')
    else:
        return True  # Unknown - assume supported
    
    if not min_ver:
        return True
    
    # Check IDE version
    ide_version = parse_version(ide_version_str)
    if not ide_version:
        return False
    
    min_ide = min_ver.get('ide')
    if min_ide:
        ide_padded = ide_version + (0,) * (len(min_ide) - len(ide_version))
        if ide_padded[:len(min_ide)] < min_ide:
            return False
    
    # Check extension version
    if ext_version_str:
        ext_version = parse_version(ext_version_str)
        min_ext = min_ver.get('extension')
        if ext_version and min_ext:
            ext_padded = ext_version + (0,) * (len(min_ext) - len(ext_version))
            if ext_padded[:len(min_ext)] < min_ext:
                return False
    
    return True

def version_key(v):
    match = re.search(r'(\d+)\.(\d+)\.(\d+)', v)
    if match:
        return (int(match.group(1)), int(match.group(2)), int(match.group(3)))
    return (0, 0, 0)

def analyze_dataset(name, activity_file, discrepancy_file):
    print(f"\n{'='*70}")
    print(f"VERSION ANALYSIS: {name} (supported versions only)")
    print(f"{'='*70}")
    
    # VS Code versions
    all_vscode = defaultdict(int)
    disc_vscode = defaultdict(int)
    
    # Extension versions
    all_ext = defaultdict(int)
    disc_ext = defaultdict(int)
    
    # Read activity report (only supported versions)
    with open(activity_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            surface = row.get('Last Surface Used', '')
            if not is_supported(surface):
                continue
            if surface and surface.lower().startswith('vscode/'):
                parts = surface.split('/')
                if len(parts) >= 2:
                    all_vscode[parts[1]] += 1
                # Extension version
                for i, part in enumerate(parts):
                    if part in ['copilot', 'copilot-chat'] and i+1 < len(parts):
                        all_ext[f'{part}/{parts[i+1]}'] += 1
                        break
    
    # Read discrepancies
    with open(discrepancy_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            surface = row.get('Report Surface', '')
            if surface and surface.lower().startswith('vscode/'):
                parts = surface.split('/')
                if len(parts) >= 2:
                    disc_vscode[parts[1]] += 1
                for i, part in enumerate(parts):
                    if part in ['copilot', 'copilot-chat'] and i+1 < len(parts):
                        disc_ext[f'{part}/{parts[i+1]}'] += 1
                        break
    
    # VS Code version analysis
    print(f"\n--- VS Code IDE Version ---")
    print(f"{'Version':<15} {'Total':>8} {'Discrepancies':>14} {'Rate':>8}")
    print("-" * 50)
    
    for version in sorted(all_vscode.keys(), key=version_key, reverse=True):
        total = all_vscode[version]
        disc = disc_vscode.get(version, 0)
        rate = (disc / total * 100) if total > 0 else 0
        if total >= 20:
            print(f"{version:<15} {total:>8,} {disc:>14,} {rate:>7.1f}%")
    
    # Extension version analysis
    print(f"\n--- Copilot Extension Version ---")
    print(f"{'Extension/Version':<25} {'Total':>8} {'Discrepancies':>14} {'Rate':>8}")
    print("-" * 60)
    
    for ext in sorted(all_ext.keys(), key=version_key, reverse=True):
        total = all_ext[ext]
        disc = disc_ext.get(ext, 0)
        rate = (disc / total * 100) if total > 0 else 0
        if total >= 20:
            print(f"{ext:<25} {total:>8,} {disc:>14,} {rate:>7.1f}%")

if __name__ == '__main__':
    datasets = [
        ('JPMC', 'jpmc-data/jpmcai-seat-activity-1766019925.csv', 'jpmc-data/output/discrepancies.csv'),
        ('BofA', 'bofa-data/bofa-emu-seat-activity-1765996332.csv', 'bofa-data/output/discrepancies.csv'),
        ('Goldman Sachs', 'gs-data/goldman-sachs-seat-activity-1766002508.csv', 'gs-data/output/discrepancies.csv'),
    ]
    
    for name, activity, disc in datasets:
        try:
            analyze_dataset(name, activity, disc)
        except Exception as e:
            print(f"Error analyzing {name}: {e}")
