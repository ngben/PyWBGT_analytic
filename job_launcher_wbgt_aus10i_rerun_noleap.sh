#!/bin/bash

: "${MODEL:?MODEL not set}"
: "${SCENARIO:?SCENARIO not set}"
: "${EXPERIMENT:?EXPERIMENT not set}"
: "${YEAR:?YEAR not set}"

LOGDIR="./${MODEL}_${SCENARIO}_logs"
mkdir -p "${LOGDIR}"

# Submit just the rerun job for the failed year (not full 1951–2014 loop)
qsub -N wbgt_${MODEL}_${EXPERIMENT}_${YEAR} \
     -v YEAR=${YEAR},MODEL=${MODEL},EXPERIMENT=${EXPERIMENT},SCENARIO=${SCENARIO} \
     -o ${LOGDIR}/wbgt_calc_${MODEL}_${SCENARIO}_${EXPERIMENT}_${YEAR}.log \
     wbgt_job_aus10i_rerun_noleap.pbs