#!/bin/bash

MODEL="ACCESS-ESM1-5"
EXPERIMENT="r6i1p1f1"
LOGDIR="./${MODEL}_ssp126_logs"

# Make sure the log directory exists
mkdir -p "${LOGDIR}"

for year in {2015..2099}; do
    qsub -N wbgt_${MODEL}_${EXPERIMENT}_${year} \
         -v YEAR=${year},MODEL=${MODEL},EXPERIMENT=${EXPERIMENT} \
         -o ${LOGDIR}/wbgt_calc_${MODEL}_ssp126_${EXPERIMENT}_${year}.log \
         wbgt_job_aus10i_ssp126.pbs
done
