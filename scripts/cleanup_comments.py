#!/usr/bin/env python3
"""
Comment Cleanup Script for Rust Files

Removes redundant inline comments that match known patterns.
Run with --dry-run to preview changes without modifying files.

Usage:
    python cleanup_comments.py [--dry-run] [paths...]
    
Examples:
    python cleanup_comments.py pg/src/driver/          # Clean all .rs in directory
    python cleanup_comments.py pg/src/driver/mod.rs   # Clean specific file
    python cleanup_comments.py --dry-run .            # Preview all changes
"""

import os
import re
import sys
from pathlib import Path

# Patterns to remove (regex patterns for inline comments ONLY)
# These must only match trailing comments, not code
REMOVE_PATTERNS = [
    # Redundant trailing inline comments (must be preceded by code or whitespace)
    r'(\s+)// ZERO[-_]ALLOC[^"]*$',
    r'(\s+)// ZERO[-_]COPY[^"]*$',
    r'(\s+)// FAST[^"]*$',
    r'(\s+)// ULTRA[-_]FAST[^"]*$',
    r'(\s+)// Collect results$',
    r'(\s+)// Write and flush$',
    r'(\s+)// Cleanup$',
    r'(\s+)// Validate.*$',
    r'(\s+)// Return.*connection.*$',
    r'(\s+)// Decrement.*$',
    r'(\s+)// Increment.*$',
    r'(\s+)// Don\'t return.*$',
    r'(\s+)// Connection.*dropped.*$',
    r'(\s+)// Clear idle.*$',
]

# Full-line comment patterns to remove
REMOVE_LINE_PATTERNS = [
    r'^\s*// ZERO[-_]ALLOC.*$',
    r'^\s*// ZERO[-_]COPY.*$', 
    r'^\s*// FAST path.*$',
    r'^\s*// ULTRA[-_]FAST.*$',
    r'^\s*// Send.*bytes.*$',
    r'^\s*// Collect results$',
    r'^\s*// Hash.*cache.*$',
    r'^\s*// Check.*cache.*$',
    r'^\s*// Generate.*name.*$',
    r'^\s*// Send Parse.*$',
    r'^\s*// Cache.*statement.*$',
    r'^\s*// Write and flush$',
    r'^\s*// Cleanup$',
    r'^\s*// Need more data.*$',
    r'^\s*// Reserve space.*$',
    r'^\s*// Skip.*message.*$',
    r'^\s*// Return.*connection.*$',
]

# Verbose doc comment continuation lines to remove
# These match common "filler" doc lines that can be merged/removed
VERBOSE_DOC_PATTERNS = [
    # Obvious struct field comments (field name says it all)
    r'^\s*/// Host address.*$',
    r'^\s*/// Port number.*$',
    r'^\s*/// Username.*$',
    r'^\s*/// Database name.*$',
    r'^\s*/// Password.*$',
    r'^\s*/// Maximum number of.*$',
    r'^\s*/// Minimum number of.*$',
    r'^\s*/// Maximum time.*$',
    r'^\s*/// Maximum lifetime.*$',
    r'^\s*/// Number of.*$',
    r'^\s*/// Total.*created.*$',
    r'^\s*/// Current.*$',
    # Lines that just repeat the function name or are obvious
    r'^\s*/// Prevents new.*$',
    r'^\s*/// Existing.*will be.*$', 
    r'^\s*/// Waits if all.*$',
    r'^\s*/// Stale connections.*$',
    r'^\s*/// Connection is automatically.*$',
    r'^\s*/// When enabled,.*$',
    r'^\s*/// After this duration,.*$',
    r'^\s*/// This sends all queries.*$',
    r'^\s*/// Reduces N queries.*$',
    r'^\s*/// This achieves.*q/s.*$',
    r'^\s*/// Uses.*reusable buffers.*$',
    r'^\s*/// Queries that exceed.*$',
    r'^\s*/// This is a production.*$',
    r'^\s*/// This is the high-performance.*$',
    r'^\s*/// Unlike.*which only.*$',
    r'^\s*/// collects and returns.*$',
    r'^\s*/// This is the fastest.*$',
    r'^\s*/// Matches native.*$',
    r'^\s*/// Uses pre-computed.*$',
    r'^\s*/// This registers.*$',
    r'^\s*/// Uses reference-counted.*$',
    r'^\s*/// This matches C.*$',
    r'^\s*/// Optimized for the common.*$',
]

def should_skip_line(line: str) -> bool:
    """Check if a line should be entirely removed (full-line comments only)."""
    stripped = line.strip()
    # Empty comment lines in doc blocks
    if stripped == '///':
        return True
    # Lines that match full-line removal patterns
    for pattern in REMOVE_LINE_PATTERNS:
        if re.match(pattern, stripped):
            return True
    # Lines that match verbose doc comment patterns
    for pattern in VERBOSE_DOC_PATTERNS:
        if re.match(pattern, stripped):
            return True
    return False

def clean_line(line: str) -> str:
    """Remove redundant inline comments from a line."""
    for pattern in REMOVE_PATTERNS:
        # Remove trailing inline comments matching patterns
        line = re.sub(pattern, '', line)
    return line

def clean_file(filepath: Path, dry_run: bool = False) -> tuple[int, int]:
    """Clean a single Rust file. Returns (lines_removed, lines_modified)."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            original_lines = f.readlines()
    except Exception as e:
        print(f"  Error reading {filepath}: {e}")
        return 0, 0
    
    new_lines = []
    lines_removed = 0
    lines_modified = 0
    
    for line in original_lines:
        # Check if entire line should be removed
        if should_skip_line(line):
            lines_removed += 1
            continue
        
        # Clean inline comments
        cleaned = clean_line(line)
        if cleaned != line:
            lines_modified += 1
            line = cleaned
        
        new_lines.append(line)
    
    # Write if changes were made
    if lines_removed > 0 or lines_modified > 0:
        if dry_run:
            print(f"  Would modify: {filepath}")
            print(f"    - Remove {lines_removed} lines")
            print(f"    - Modify {lines_modified} lines")
        else:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.writelines(new_lines)
            print(f"  Modified: {filepath} (-{lines_removed} lines, ~{lines_modified} lines)")
    
    return lines_removed, lines_modified

def find_rust_files(path: Path) -> list[Path]:
    """Find all Rust files in a directory or return single file."""
    if path.is_file():
        if path.suffix == '.rs':
            return [path]
        return []
    
    return list(path.rglob('*.rs'))

def main():
    args = sys.argv[1:]
    dry_run = '--dry-run' in args
    args = [a for a in args if a != '--dry-run']
    
    if not args:
        args = ['.']
    
    print("🧹 Rust Comment Cleanup Script")
    print(f"   Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print()
    
    total_removed = 0
    total_modified = 0
    files_processed = 0
    
    for arg in args:
        path = Path(arg)
        if not path.exists():
            print(f"  Warning: {path} does not exist")
            continue
        
        files = find_rust_files(path)
        for filepath in files:
            removed, modified = clean_file(filepath, dry_run)
            if removed > 0 or modified > 0:
                total_removed += removed
                total_modified += modified
                files_processed += 1
    
    print()
    print(f"Summary:")
    print(f"  Files processed: {files_processed}")
    print(f"  Lines removed: {total_removed}")
    print(f"  Lines modified: {total_modified}")
    
    if dry_run and (total_removed > 0 or total_modified > 0):
        print()
        print("Run without --dry-run to apply changes.")

if __name__ == '__main__':
    main()
