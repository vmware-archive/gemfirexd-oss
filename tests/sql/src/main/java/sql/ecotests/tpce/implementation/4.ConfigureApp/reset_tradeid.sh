#!/bin/bash
GFXD_SITE=${GFXD_SITE_ctl:-"/s2qa/tangc/workspace/sqlBuild/product-gfxd"}
LOCATOR_ADDR=${LOCATOR_ADDR_ctl:-"10.150.30.37"}
LOCATOR_PORT=${LOCATOR_PORT_ctl:-"7710"}
HOST_PORT=${LOCATOR_ADDR}:${LOCATOR_PORT}

GFXD_OUTPUT=`${GFXD_SITE}/bin/gfxd <<_EOF1_
connect client '${HOST_PORT}';
set schema tpcegfxd;
select 'TRADE_ID_BEGIN=' || RTRIM(CHAR(max(t_id))) || '=TRADE_ID_END' from trade;
exit;
_EOF1_`

echo $GFXD_OUTPUT;

MAX_TRADE_ID=`expr "$GFXD_OUTPUT" : '.*TRADE_ID_BEGIN=\([0-9]*\)=TRADE_ID_END.*'`
NEXT_TRADE_ID=$((MAX_TRADE_ID + 1))
echo $NEXT_TRADE_ID

GFXD_OUTPUT=`${GFXD_SITE}/bin/gfxd <<_EOF2_
connect client '${HOST_PORT}';
set schema tpcegfxd;
alter table trade alter t_id restart with $NEXT_TRADE_ID;
exit;
_EOF2_`

echo Status: $?
echo $GFXD_OUTPUT;


