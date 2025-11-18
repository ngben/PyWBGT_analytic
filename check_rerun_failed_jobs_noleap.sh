#!/bin/bash
# Check file sizes AND find missing years, then rerun jobs

# Thresholds in bytes
MIN_SIZE=$((9 * 1024 * 1024 * 1024))   # 9 GB
MAX_SIZE=$((10 * 1024 * 1024 * 1024))  # 10 GB

# Path to job launcher
JOB_LAUNCHER="./job_launcher_wbgt_aus10i_rerun_noleap.sh"

# Top-level directory (change depending on scenario)
SEARCH_DIR="/scratch/e53/bxn599/aus10i/CESM2_ssp370"

# Output list
FAILED_LIST="failed_files.txt"
> "$FAILED_LIST"

echo "Scanning directory: $SEARCH_DIR"
echo

###############################################################################
# 1. Determine existing files and detect malformed sizes
###############################################################################
echo "Checking file sizes..."

find "$SEARCH_DIR" -type f -name "*.nc" | while read -r file; do
    size_bytes=$(stat -c%s "$file")

    # Add file to rerun list if too small or too large
    if (( size_bytes < MIN_SIZE || size_bytes > MAX_SIZE )); then
        echo "$file" | tee -a "$FAILED_LIST"
    fi
done

###############################################################################
# 2. Identify missing years for historical, ssp126, ssp370
###############################################################################
echo
echo "Checking for missing years..."

for scenario in historical ssp126 ssp370; do

    # Find scenario directory
    SCEN_DIR=$(find "$SEARCH_DIR" -type d -name "*${scenario}*" | head -n 1)
    [[ -z "$SCEN_DIR" ]] && continue

    echo "→ Scenario: $scenario (directory: $SCEN_DIR)"

    # Year ranges
    if [[ "$scenario" == "historical" ]]; then
        START_Y=1952
        END_Y=2014
    else
        START_Y=2015
        END_Y=2099
    fi

    # Get one example file to extract model + experiment
    example_file=$(find "$SCEN_DIR" -type f -name "*.nc" | head -n 1)
    [[ -z "$example_file" ]] && continue

    MODEL=$(basename "$example_file" | cut -d'_' -f3)
    EXPERIMENT=$(basename "$example_file" | grep -oE 'r[0-9]+i[0-9]+p[0-9]+f[0-9]+')

    # Extract all actual years *present in files*
    present_years=$(find "$SCEN_DIR" -type f -name "*.nc" \
        | sed -E 's/.*_([0-9]{4})01010000-.*/\1/' \
        | sort -u)

    # Loop through required years
    for (( y=START_Y; y<=END_Y; y++ )); do
        if ! echo "$present_years" | grep -q "^$y$"; then
            echo "MISSING YEAR: $MODEL $scenario $EXPERIMENT $y"
            echo "$scenario $MODEL $EXPERIMENT $y" >> "$FAILED_LIST"
        fi
    done

done

###############################################################################
# 3. Rerun jobs for failed or missing years
###############################################################################
echo
echo "=== RERUNNING FAILED/MISSING JOBS ==="
echo

while read -r entry; do
    [[ -z "$entry" ]] && continue

    # Case 1: it's a file path (bad size)
    if [[ "$entry" == /*.nc ]]; then
        filepath="$entry"
        filename=$(basename "$filepath")

        model=$(echo "$filename" | cut -d'_' -f3)
        scenario=$(echo "$filename" | cut -d'_' -f4)
        experiment=$(echo "$filename" | grep -oE 'r[0-9]+i[0-9]+p[0-9]+f[0-9]+')
        year=$(echo "$filename" | sed -E 's/.*_([0-9]{4})01010000-.*/\1/')

    # Case 2: synthetic "missing file" entry: "scenario model experiment year"
    else
        scenario=$(echo "$entry" | awk '{print $1}')
        model=$(echo "$entry" | awk '{print $2}')
        experiment=$(echo "$entry" | awk '{print $3}')
        year=$(echo "$entry" | awk '{print $4}')
    fi

    echo "➡️  Rerunning ${model} ${scenario} ${experiment} ${year}"
    SCENARIO="$scenario" MODEL="$model" EXPERIMENT="$experiment" YEAR="$year" bash "$JOB_LAUNCHER"

done < "$FAILED_LIST"

###############################################################################
echo
echo "Done."

