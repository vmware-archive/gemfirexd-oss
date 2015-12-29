#!/bin/sh
TPCE_HOME=${TPCE_HOME_ctl:-"/s2qa/tangc/sample/tpce"}
FLAT_IN=${FLAT_IN_ctl:-"${TPCE_HOME}/flat_in"}
FLAT_OUT=${FLAT_OUT_ctl:-"${TPCE_HOME}/flat_out"}
CUSTOMRE=$1
TOTAL=$2
DAYS=$3
echo "${TPCE_HOME}/EGenLoader -c $CUSTOMER -t $TOTAL -w $DAYS -i $FLAT_IN -o $FLAT_OUT"
${TPCE_HOME}/EGenLoader -c $CUSTOMER -t $TOTAL -w $DAYS -i $FLAT_IN -o $FLAT_OUT