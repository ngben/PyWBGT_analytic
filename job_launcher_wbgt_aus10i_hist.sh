#!/bin/bash

MODEL="ACCESS-CM2"
EXPERIMENT="r4i1p1f1"
LOGDIR="./${MODEL}_historical_logs"

# Make sure the log directory exists
mkdir -p "${LOGDIR}"

for year in {2011..2014}; do
    qsub -N wbgt_${MODEL}_${EXPERIMENT}_${year} \
         -v YEAR=${year},MODEL=${MODEL},EXPERIMENT=${EXPERIMENT} \
         -o ${LOGDIR}/wbgt_calc_${MODEL}_historical_${EXPERIMENT}_${year}.log \
         wbgt_job_aus10i_hist.pbs
done
