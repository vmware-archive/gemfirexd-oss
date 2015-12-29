#!/bin/sh
. ./gfxdenv.sh
GFXD_SITE=${GFXD_SITE_ctl:-"/s2qa/tangc/workspace/sqlBuild/product-gfxd"}
BUILD_DATE=$1
BUILD=$GFXD_SITE$1
TEST_DIR=${TEST_DIR_ctl:-"/s2qa/tangc/samples/tpce/implementation/0.SetupEnv/logs"}

LOCATOR_ADDR=${LOCATOR_ADDR_ctl:-"10.150.30.36"}
SRV1_ADDR=${SRV1_ADDR_ctl:-"10.150.30.36"}
SRV2_ADDR=${SRV2_ADDR_ctl:-"10.150.30.37"}
SRV3_ADDR=${SRV3_ADDR_ctl:-"10.150.30.38"}
SRV4_ADDR=${SRV4_ADDR_ctl:-"10.150.30.39"}

for (( i = 4; i >= 1; i-- ))
do
  SRV_ADDR=$(eval "echo \$SRV${i}_ADDR")
  STOP_SRV="ssh -l tangc -n ${SRV_ADDR} $BUILD/bin/gfxd server stop -dir=$TEST_DIR/gfxdserver$i"
  echo $STOP_SRV
  eval $STOP_SRV
done

if [ -d "${TEST_DIR}/gfxdserverI" ]; then
  $BUILD/bin/gfxd server stop -dir=$TEST_DIR/gfxdserverI
  #/s2qa/tangc/workspace/sqlBuild/product-gfxd/bin/gfxd server stop -dir=/s2qa/tangc/samples/tpce/implementation/0.SetupEnv/logs/gfxdserverI
fi

ssh -l tangc -n ${LOCATOR_ADDR} "$BUILD/bin/gfxd locator stop -dir=$TEST_DIR/gfxdserver0"

BAK_STAMP=`date '+%m-%d-%y-%H_%M_%S'`
for (( i = 4 ; i >= 0; i-- ))
do
  if [ -d "${TEST_DIR}/gfxdserver$i" ];then
     if [ ! -d "${TEST_DIR}/backup_${BAK_STAMP}" ]; then
         mkdir ${TEST_DIR}/backup_${BAK_STAMP}
     fi
     mv ${TEST_DIR}/gfxdserver$i ${TEST_DIR}/backup_${BAK_STAMP}/
  fi
done
echo "Stop Time: $BAK_STAMP"