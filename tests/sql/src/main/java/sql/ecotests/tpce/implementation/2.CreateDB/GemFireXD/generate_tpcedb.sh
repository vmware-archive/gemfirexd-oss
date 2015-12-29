#!/bin/sh
export CLASSPATH=/s2qa/tangc/ecotest/ojdbc6.jar:$CLASSPATH
export GFXD=/s2qa/tangc/workspace/sqlBuild/product-gfxd
${GFXD}/bin/gfxd < ./01.tpce_1.12.0_fast_load.sql
${GFXD}/bin/gfxd < ./02.gfxdimport_egen_data.sql
${GFXD}/bin/gfxd < ./03.tpce_1.12.0_add_constraints.sql
