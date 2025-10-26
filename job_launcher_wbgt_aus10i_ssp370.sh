#!/bin/bash

MODEL="ACCESS-CM2"
EXPERIMENT="r4i1p1f1"
LOGDIR="./${MODEL}_ssp370_logs"

# Make sure the log directory exists
mkdir -p "${LOGDIR}"

for year in {2019..2020}; do
    qsub -N wbgt_${MODEL}_${EXPERIMENT}_${year} \
         -v YEAR=${year},MODEL=${MODEL},EXPERIMENT=${EXPERIMENT} \
         -o ${LOGDIR}/wbgt_calc_${MODEL}_ssp370_${EXPERIMENT}_${year}.log \
         wbgt_job_aus10i_ssp370.pbs
done
