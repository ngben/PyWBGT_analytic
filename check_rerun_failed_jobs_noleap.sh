#!/bin/bash
# Check file sizes and rerun failed jobs

# Thresholds in bytes
MIN_SIZE=$((9 * 1024 * 1024 * 1024))   # 9 GB
MAX_SIZE=$((10 * 1024 * 1024 * 1024))  # 10 GB

# Path to your job launcher (update if needed)
JOB_LAUNCHER="./job_launcher_wbgt_aus10i_rerun_noleap.sh"

# Directory containing model subfolders
SEARCH_DIR="/scratch/e53/bxn599/aus10i/CMCC-ESM2_ssp370"

# Temporary list of failed files
FAILED_LIST="failed_files.txt"
> "$FAILED_LIST"

echo "Checking file sizes in: $SEARCH_DIR"
echo "Files outside [9 GB, 10 GB] will be rerun."
echo

# Find all NetCDF files and check their sizes
find "$SEARCH_DIR" -type f -name "*.nc" | while read -r file; do
    size_bytes=$(stat -c%s "$file")

    if (( size_bytes < MIN_SIZE || size_bytes > MAX_SIZE )); then
        echo "$file" | tee -a "$FAILED_LIST"
    fi
done

echo
echo "=== FAILED FILES ==="
cat "$FAILED_LIST"
echo

# Now rerun each failed file as a PBS job
while read -r filepath; do
    # Skip empty lines
    [[ -z "$filepath" ]] && continue

    filename=$(basename "$filepath")

    model=$(echo "$filename" | cut -d'_' -f3)
    scenario=$(echo "$filename" | cut -d'_' -f4)
    experiment=$(echo "$filename" | grep -oE 'r[0-9]+i[0-9]+p[0-9]+f[0-9]+')
    year=$(echo "$filename" | sed -E 's/.*_([0-9]{4})01010000-.*/\1/')

    if [[ -n "$model" && -n "$scenario" && -n "$experiment" && -n "$year" ]]; then
        echo "➡️  Rerunning ${model} ${scenario} ${experiment} ${year}"
        SCENARIO="$scenario" MODEL="$model" EXPERIMENT="$experiment" YEAR="$year" bash "$JOB_LAUNCHER"
    else
        echo "⚠️  Skipping: Could not parse $filepath"
    fi
done < "$FAILED_LIST"
