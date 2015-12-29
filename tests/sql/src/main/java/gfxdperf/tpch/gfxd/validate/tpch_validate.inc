include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/gemfirexd/topology_2_locator.inc;
include $JTESTS/hydraconfig/gemfirexd/performance.inc;
include $JTESTS/gfxdperf/tpch/gfxd/validate/persist.inc;

hydra.Prms-testDescription = "TPC-H query validation using the expected answers for the scalability factor of 1G.";

hydra.Prms-totalTaskTimeSec = 3600000;
hydra.Prms-maxResultWaitSec = 7200;

//------------------------------------------------------------------------------
// Threadgroups
//------------------------------------------------------------------------------

THREADGROUP locator
  totalThreads = fcn ${locatorHosts} * ${locatorVMsPerHost}
                                     * ${locatorThreadsPerVM} ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"locator\",
                        ${locatorHosts}, true)" ncf;
THREADGROUP client
  totalThreads = fcn ${clientHosts} * ${clientVMsPerHost}
                                    * ${clientThreadsPerVM} ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"client\",
                        ${clientHosts}, true)" ncf;
THREADGROUP server
  totalThreads = fcn ${serverHosts} * ${serverVMsPerHost}
                                    * ${serverThreadsPerVM} ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"server\",
                        ${serverHosts}, true)" ncf;

//------------------------------------------------------------------------------
// JVM
//------------------------------------------------------------------------------

hydra.VmPrms-extraVMArgs =
  fcn "hydra.TestConfigFcns.duplicate
       (\"-Xms256m -Xmx256m\",
                ${locatorHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
       (\"-Xms${clientHeap} -Xmx${clientHeap}\",
                ${clientHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
       (\"-Xms${serverHeap} -Xmx${serverHeap}\",
                ${serverHosts}, true)" ncf
  ;

//------------------------------------------------------------------------------
// GFXD
//------------------------------------------------------------------------------

hydra.gemfirexd.FabricServerPrms-conserveSockets = false;
hydra.gemfirexd.FabricServerPrms-distributedSystem = ds;
hydra.gemfirexd.FabricServerPrms-enforceUniqueHost = true;
hydra.gemfirexd.FabricServerPrms-saveSysDiskDir = false;
hydra.gemfirexd.FabricServerPrms-useExistingSysDiskDir = false;
hydra.gemfirexd.FabricServerPrms-useGenericSysDiskDir = false;

//------------------------------------------------------------------------------
// TASKS
//------------------------------------------------------------------------------

INITTASK  taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = createLocatorTask
          threadGroups = locator
          ;
INITTASK  taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = startLocatorTask
          threadGroups = locator
          ;
INITTASK  taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = startFabricServerTask
          threadGroups = server
          ;
INITTASK  taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = initDBTask
          threadGroups = client
          ;
INITTASK  taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = generateDDLTask
          threadGroups = server
          ;
INITTASK  taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = executeDDLTask
          threadGroups = client
          ;
INITTASK  taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = createBucketsTask
          threadGroups = client
          ;
INITTASK  taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = checkBucketLoadBalanceTask
          threadGroups = server
          ;
INITTASK  taskClass = gfxdperf.PerfClient
          taskMethod = openStatisticsTask
          threadGroups = client, server
          ;
INITTASK  taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = importTableTask
          threadGroups = client
          gfxdperf.tpch.TPCHPrms-tableName = REGION
          ;
INITTASK  taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = importTableTask
          threadGroups = client
          gfxdperf.tpch.TPCHPrms-tableName = NATION
          ;
INITTASK  taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = importTableTask
          threadGroups = client
          gfxdperf.tpch.TPCHPrms-tableName = PART
          ;
INITTASK  taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = importTableTask
          threadGroups = client
          gfxdperf.tpch.TPCHPrms-tableName = SUPPLIER
          ;
INITTASK  taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = importTableTask
          threadGroups = client
          gfxdperf.tpch.TPCHPrms-tableName = PARTSUPP
          ;
INITTASK  taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = importTableTask
          threadGroups = client
          gfxdperf.tpch.TPCHPrms-tableName = CUSTOMER
          ;
INITTASK  taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = importTableTask
          threadGroups = client
          gfxdperf.tpch.TPCHPrms-tableName = ORDERS
          ;
INITTASK  taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = importTableTask
          threadGroups = client
          gfxdperf.tpch.TPCHPrms-tableName = LINEITEM
          ;
INITTASK  taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = checkDataLoadBalanceTask
          threadGroups = server
          ;
INITTASK  taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = validateQueriesTask
          threadGroups = client
          ;
CLOSETASK taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = cleanupDBTask
          threadGroups = client
          ;
CLOSETASK taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = shutDownAllTask
          threadGroups = locator
          ;

//------------------------------------------------------------------------------
// Application parameters
//------------------------------------------------------------------------------

gfxdperf.tpch.gfxd.GFXDPrms-bucketCount = fcn ${bucketsPerServer} * ${serverHosts} * ${serverVMsPerHost} ncf;
gfxdperf.tpch.gfxd.GFXDPrms-partitionRedundancy = 1;
gfxdperf.tpch.gfxd.GFXDPrms-persistentTables = all;
gfxdperf.tpch.gfxd.GFXDPrms-skipConstraintChecks = true; // to make the load go faster

//perffmwk.PerfReportPrms-statisticsSpecification = $JTESTS/gfxdperf/tpch/core/workloads/CoreA.spec;
