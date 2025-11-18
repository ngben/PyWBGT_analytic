#!/bin/bash

MODEL="ERA5"
EXPERIMENT="r1i1p1f1"
LOGDIR="./${MODEL}_evaluation_logs"

# Make sure the log directory exists
mkdir -p "${LOGDIR}"

for year in {1979..2020}; do
    qsub -N wbgt_${MODEL}_${EXPERIMENT}_${year} \
         -v YEAR=${year},MODEL=${MODEL},EXPERIMENT=${EXPERIMENT} \
         -o ${LOGDIR}/wbgt_calc_${MODEL}_evaluation_${EXPERIMENT}_${year}.log \
         wbgt_job_aus10i_evaluation.pbs
done
