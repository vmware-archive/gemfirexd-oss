INITTASK  taskClass = gfxdperf.tpch.oracle.OracleClient
          taskMethod = generateDDLTask
          threadGroups = client
          ;
INITTASK  taskClass = gfxdperf.tpch.oracle.OracleClient
          taskMethod = executeDDLTask
          threadGroups = client
          ;
INITTASK  taskClass = gfxdperf.tpch.oracle.OracleClient
          taskMethod = importTableTask
          threadGroups = client
          gfxdperf.tpch.TPCHPrms-tableName = REGION
          ;
INITTASK  taskClass = gfxdperf.tpch.oracle.OracleClient
          taskMethod = importTableTask
          threadGroups = client
          gfxdperf.tpch.TPCHPrms-tableName = NATION
          ;
INITTASK  taskClass = gfxdperf.tpch.oracle.OracleClient
          taskMethod = importTableTask
          threadGroups = client
          gfxdperf.tpch.TPCHPrms-tableName = PART
          ;
INITTASK  taskClass = gfxdperf.tpch.oracle.OracleClient
          taskMethod = importTableTask
          threadGroups = client
          gfxdperf.tpch.TPCHPrms-tableName = SUPPLIER
          ;
INITTASK  taskClass = gfxdperf.tpch.oracle.OracleClient
          taskMethod = importTableTask
          threadGroups = client
          gfxdperf.tpch.TPCHPrms-tableName = PARTSUPP
          ;
INITTASK  taskClass = gfxdperf.tpch.oracle.OracleClient
          taskMethod = importTableTask
          threadGroups = client
          gfxdperf.tpch.TPCHPrms-tableName = CUSTOMER
          ;
INITTASK  taskClass = gfxdperf.tpch.oracle.OracleClient
          taskMethod = importTableTask
          threadGroups = client
          gfxdperf.tpch.TPCHPrms-tableName = ORDERS
          ;
INITTASK  taskClass = gfxdperf.tpch.oracle.OracleClient
          taskMethod = importTableTask
          threadGroups = client
          gfxdperf.tpch.TPCHPrms-tableName = LINEITEM
          ;

