#!/bin/sh
export CLASSPATH=/s2qa/tangc/ecotest/ojdbc6.jar:$CLASSPATH
export GFXD=/s2qa/tangc/workspace/sqlBuild/product-gfxd
sh ${GFXD}/bin/gfxd write-schema-to-sql -file=tpce_oratogfxd.sql -database-type=oracle -driver-class=oracle.jdbc.OracleDriver -schema-pattern=TPCEGFXD -to-database-type=gemfirexd -url=jdbc:oracle:thin:tpcegfxd/tpcegfxd@vmc-ssrc-rh46.eng.vmware.com:1524:orclrh46 > exportschemasql.log
sh ${GFXD}/bin/gfxd write-schema-to-xml -file=tpce_oratogfxd.xml -database-type=oracle -driver-class=oracle.jdbc.OracleDriver -schema-pattern=TPCEGFXD -url=jdbc:oracle:thin:tpcegfxd/tpcegfxd@vmc-ssrc-rh46.eng.vmware.com:1524:orclrh46 > exportschemaxml.log
sh ${GFXD}/bin/gfxd write-data-to-xml -file=tpce_oratogfxd.dat -database-type=oracle -driver-class=oracle.jdbc.OracleDriver -schema-pattern=TPCEGFXD -url=jdbc:oracle:thin:tpcegfxd/tpcegfxd@vmc-ssrc-rh46.eng.vmware.com:1524:orclrh46 > exportdata.log
#load the data
sh ${GFXD}/bin/gfxd < tpce_oratogfxd.sql
sh ${GFXD}/bin/gfxd write-data-to-db -files=tpce_oratogfxd.dat -schema-files=tpcegfxd.xml -client-bind-address=10.150.30.37 -client-port=7710 > importdata.log