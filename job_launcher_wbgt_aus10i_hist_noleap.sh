#!/bin/bash

MODEL="CESM2"
EXPERIMENT="r11i1p1f1"
LOGDIR="./${MODEL}_historical_logs"

# Make sure the log directory exists
mkdir -p "${LOGDIR}"

for year in {1951..1952}; do
#    if (( (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0) )); then
        qsub -N wbgt_${MODEL}_${EXPERIMENT}_${year} \
             -v YEAR=${year},MODEL=${MODEL},EXPERIMENT=${EXPERIMENT} \
             -o ${LOGDIR}/wbgt_calc_${MODEL}_historical_${EXPERIMENT}_${year}.log \
             wbgt_job_aus10i_hist_noleap.pbs
#    fi
done
