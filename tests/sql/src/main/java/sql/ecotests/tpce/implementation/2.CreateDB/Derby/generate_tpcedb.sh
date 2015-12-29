#!/bin/sh
export CLASSPATH=/s2qa/tangc/ecotest/ojdbc6.jar:$CLASSPATH
export GFXD=/s2qa/tangc/workspace/sqlBuild/product-gfxd
${GFXD}/bin/gfxd < ./tpce_1.12.0_current.sql
${GFXD}/bin/gfxd < ./gfxdimport_egen_data.sql