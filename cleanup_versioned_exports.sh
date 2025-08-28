#!/bin/bash

# Cleanup script for versioned export directories
# This script removes versioned directories and keeps only the latest export

set -e

EXPORTS_DIR="/app/exports"
DRY_RUN=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --exports-dir)
            EXPORTS_DIR="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--dry-run] [--exports-dir /path/to/exports]"
            exit 1
            ;;
    esac
done

echo "🧹 Export Directory Cleanup Tool"
echo "================================="
echo "Exports directory: $EXPORTS_DIR"
echo "Dry run mode: $DRY_RUN"
echo ""

# Check if exports directory exists
if [ ! -d "$EXPORTS_DIR" ]; then
    echo "❌ Exports directory does not exist: $EXPORTS_DIR"
    exit 1
fi

cd "$EXPORTS_DIR"

# Find all versioned directories (ending with _v followed by numbers)
versioned_dirs=$(find . -maxdepth 1 -type d -name "*_v[0-9]*" | sort)

if [ -z "$versioned_dirs" ]; then
    echo "✅ No versioned directories found. Nothing to clean up."
    exit 0
fi

echo "📋 Found versioned directories:"
for dir in $versioned_dirs; do
    echo "   $dir"
done
echo ""

# Group versioned directories by base name
declare -A base_tables
for dir in $versioned_dirs; do
    # Remove ./ prefix and extract base name (everything before _v)
    clean_dir=${dir#./}
    base_name=$(echo "$clean_dir" | sed 's/_v[0-9]*$//')
    
    if [ -z "${base_tables[$base_name]}" ]; then
        base_tables[$base_name]="$clean_dir"
    else
        base_tables[$base_name]="${base_tables[$base_name]} $clean_dir"
    fi
done

# Process each table's versions
for base_name in "${!base_tables[@]}"; do
    versions=(${base_tables[$base_name]})
    
    # Sort versions by version number
    IFS=$'\n' sorted_versions=($(printf '%s\n' "${versions[@]}" | sort -V))
    
    # Get the latest version (last in sorted list)
    latest_version="${sorted_versions[-1]}"
    
    echo "📊 Table: $base_name"
    echo "   Versions found: ${#sorted_versions[@]}"
    echo "   Latest version: $latest_version"
    
    # Check if base directory (without version) exists
    base_dir_exists=false
    if [ -d "$base_name" ]; then
        base_dir_exists=true
        echo "   Base directory exists: $base_name"
    fi
    
    # Determine what to keep and what to remove
    if $base_dir_exists; then
        # Keep base directory, remove all versioned ones
        to_remove=("${sorted_versions[@]}")
        echo "   Action: Keep base directory, remove all versioned directories"
    else
        # Keep latest version, remove older ones
        to_remove=("${sorted_versions[@]:0:${#sorted_versions[@]}-1}")
        echo "   Action: Keep latest version ($latest_version), remove older versions"
        
        # Optionally rename latest to base name
        if [ ${#sorted_versions[@]} -gt 0 ]; then
            if $DRY_RUN; then
                echo "   Would rename: $latest_version -> $base_name"
            else
                echo "   Renaming: $latest_version -> $base_name"
                mv "$latest_version" "$base_name"
            fi
        fi
    fi
    
    # Remove old versions
    for version_dir in "${to_remove[@]}"; do
        if $DRY_RUN; then
            echo "   Would remove: $version_dir"
            # Show size of directory to be removed
            size=$(du -sh "$version_dir" 2>/dev/null | cut -f1 || echo "unknown")
            echo "     Size: $size"
        else
            echo "   Removing: $version_dir"
            size=$(du -sh "$version_dir" 2>/dev/null | cut -f1 || echo "unknown")
            echo "     Size: $size"
            rm -rf "$version_dir"
        fi
    done
    
    echo ""
done

# Summary
total_versioned=$(echo "$versioned_dirs" | wc -l)
if $DRY_RUN; then
    echo "🔍 DRY RUN SUMMARY:"
    echo "   Found $total_versioned versioned directories"
    echo "   Run without --dry-run to perform cleanup"
else
    echo "✅ CLEANUP COMPLETE:"
    echo "   Processed $total_versioned versioned directories"
    echo "   Cleaned up duplicate exports"
fi

echo ""
echo "💡 TIP: To prevent future versioning, change the conflict resolution"
echo "   strategy in the web interface to 'Overwrite Previous' instead of 'Version'"
