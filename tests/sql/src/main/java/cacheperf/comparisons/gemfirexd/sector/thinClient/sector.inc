include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/gemfirexd/topology_2_locator.inc;
include $JTESTS/hydraconfig/gemfirexd/performance.inc;
include $JTESTS/cacheperf/comparisons/gemfirexd/statspecs/histogram.inc;
include $JTESTS/cacheperf/comparisons/gemfirexd/sector/thinClient/sector${api}.inc;

hydra.Prms-testDescription = "SQL Performance Test using Sectors.";

THREADGROUP locator
  totalThreads = fcn ${locatorHosts} * ${locatorVMsPerHost}
                                     * ${locatorThreadsPerVM} ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames
                      (\"locator\", ${locatorHosts}, true)" ncf;
THREADGROUP client
  totalThreads = fcn ${clientHosts} * ${clientVMsPerHost}
                                    * ${clientThreadsPerVM} ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames
                      (\"client\", ${clientHosts}, true)" ncf;
THREADGROUP server
  totalThreads = fcn ${serverHosts} * ${serverVMsPerHost}
                                    * ${serverThreadsPerVM} ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames
                      (\"server\", ${serverHosts}, true)" ncf;

hydra.Prms-totalTaskTimeSec = 3600000; // time-based workload
hydra.Prms-maxResultWaitSec = 600;

//------------------------------------------------------------------------------
// Tasks
//------------------------------------------------------------------------------

INITTASK  taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = createLocatorTask
          threadGroups = locator
          ;
INITTASK  taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = startLocatorTask
          threadGroups = locator
          ;
INITTASK  taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = startFabricServerTask
          threadGroups = server
          ;
INITTASK  taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = startNetworkServerTask
          threadGroups = server
          ;
INITTASK  taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = connectThinClientTask
          threadGroups = client
          ;
INITTASK  taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = openStatisticsTask
          threadGroups = locator, client, server
          ;
INITTASK  taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = measureEmptyTableMemoryUsageTask
          threadGroups = server
          ;
INITTASK  taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = createQueryDataContainersTask
          threadGroups = client
          ;
INITTASK  taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = createBucketsTask
          threadGroups = server
          ;
//INITTASK  taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
//          taskMethod = rebalanceBucketsTask
//          threadGroups = server
//          ;
INITTASK  taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = checkBucketsTask
          threadGroups = server
          ;
INITTASK  taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = createIndexesTask
          threadGroups = client
          ;
INITTASK  taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = preparedCreateQueryDataTask
          batch
          cacheperf.CachePerfPrms-keyAllocation = ownKeys
          cacheperf.CachePerfPrms-warmupTerminatorMethod = terminateOnTrimIterations
          cacheperf.CachePerfPrms-warmupTerminatorFrequency = 1 iterations
          cacheperf.CachePerfPrms-trimIterations = 0
          cacheperf.CachePerfPrms-taskTerminatorMethod = terminateOnMaxKey
          cacheperf.CachePerfPrms-taskTerminatorFrequency = 1 iterations
          threadGroups = client
          ;
//INITTASK  taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
//          taskMethod = checkDataLoadTask
//          threadGroups = server
//          ;
TASK      taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = preparedQueryQueryDataTask
          cacheperf.CachePerfPrms-keyAllocation = sameKeysRandomWrap
          cacheperf.comparisons.gemfirexd.QueryPerfPrms-queryPlanFrequency = ${queryPlanFrequency}
          threadGroups = client
          ;
CLOSETASK taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = measureLoadedTableMemoryUsageTask
          threadGroups = server
          ;
CLOSETASK taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = reportTrimIntervalsTask
          threadGroups = client, server
          ;
CLOSETASK taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = dumpQueryPlansTask
          threadGroups = server
          ;
CLOSETASK taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = checkDataLoadTask
          threadGroups = server
          ;
CLOSETASK taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = deleteQueryDataContainersTask
          threadGroups = client
          ;
CLOSETASK taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = closeStatisticsTask
          threadGroups = locator, client, server
          ;

//------------------------------------------------------------------------------

cacheperf.CachePerfPrms-batchTerminatorMethod     = terminateOnBatchSeconds;
cacheperf.CachePerfPrms-batchSeconds              = 300;
cacheperf.CachePerfPrms-warmupTerminatorMethod    = terminateOnTrimSeconds;
cacheperf.CachePerfPrms-warmupTerminatorFrequency = 10 seconds;
cacheperf.CachePerfPrms-trimSeconds               = 120;
cacheperf.CachePerfPrms-taskTerminatorMethod      = terminateOnTotalSeconds;
cacheperf.CachePerfPrms-taskTerminatorFrequency   = 10 seconds;
cacheperf.CachePerfPrms-workSeconds               = 300;

//------------------------------------------------------------------------------
// Schema
//------------------------------------------------------------------------------

cacheperf.CachePerfPrms-maxKeys = fcn ${numSectorsPerServer} * ${serverHosts} * ${serverVMsPerHost} ncf;

objects.query.QueryPrms-api = ${api};
objects.query.QueryPrms-objectType = objects.query.sector.Sector;
objects.query.QueryPrms-logQueries = ${logQueries};
objects.query.QueryPrms-logUpdates = ${logQueries};
objects.query.QueryPrms-logQueryResultSize = ${logResultSetSize};
objects.query.QueryPrms-validateResults = ${validateResults};

objects.query.sector.SectorPrms-queryType = instrumentIn;
objects.query.sector.SectorPrms-indexTypes = indexOnSectorId indexOnPositionId indexOnInstrumentId indexOnPositionSymbol indexOnPositionOwner indexOnPositionInstrument indexOnSectorName;

objects.query.sector.SectorPrms-numSectors = fcn ${numSectorsPerServer} * ${serverHosts} * ${serverVMsPerHost} ncf;
objects.query.sector.SectorPrms-numInstrumentsPerSector = ${numInstrumentsPerSector};
objects.query.sector.SectorPrms-numPositionsPerInstrument = ${numPositionsPerInstrument};

objects.query.sector.SectorPrms-numBookValues = ${numBookValues};
objects.query.sector.SectorPrms-numMarketCapValues = ${numMarketCapValues};
objects.query.sector.SectorPrms-numRiskValues = ${numRiskValues};
objects.query.sector.SectorPrms-numSymbolValues = ${numSymbolValues};

objects.query.sector.SectorPrms-numClients = fcn ${clientHosts} * ${clientVMsPerHost} ncf;
objects.query.sector.SectorPrms-numServers = fcn ${serverHosts} * ${serverVMsPerHost} ncf;

objects.query.sector.SectorPrms-sectorDataPolicy     = REPLICATE;
objects.query.sector.SectorPrms-instrumentDataPolicy = PARTITION;
objects.query.sector.SectorPrms-positionDataPolicy   = PARTITION;
objects.query.sector.SectorPrms-riskDataPolicy       = PARTITION;

objects.query.sector.SectorPrms-sectorPartitionRedundancy     = ${redundantCopies};
objects.query.sector.SectorPrms-instrumentPartitionRedundancy = ${redundantCopies};
objects.query.sector.SectorPrms-positionPartitionRedundancy   = ${redundantCopies};
objects.query.sector.SectorPrms-riskPartitionRedundancy       = ${redundantCopies};

objects.query.sector.SectorPrms-sectorPartitionTotalNumBuckets     = fcn ${bucketsPerServer} * ${serverHosts} * ${serverVMsPerHost} ncf;
objects.query.sector.SectorPrms-instrumentPartitionTotalNumBuckets = fcn ${bucketsPerServer} * ${serverHosts} * ${serverVMsPerHost} ncf;
objects.query.sector.SectorPrms-positionPartitionTotalNumBuckets   = fcn ${bucketsPerServer} * ${serverHosts} * ${serverVMsPerHost} ncf;
objects.query.sector.SectorPrms-riskPartitionTotalNumBuckets       = fcn ${bucketsPerServer} * ${serverHosts} * ${serverVMsPerHost} ncf;

objects.query.sector.SectorPrms-sectorPartitionType     = PartitionByPK;
objects.query.sector.SectorPrms-instrumentPartitionType = PartitionByPK;
objects.query.sector.SectorPrms-positionPartitionType   = PartitionByPK;
objects.query.sector.SectorPrms-riskPartitionType       = PartitionByPK;

objects.query.sector.SectorPrms-sectorPartitionColumn     = id;
objects.query.sector.SectorPrms-instrumentPartitionColumn = id;
objects.query.sector.SectorPrms-positionPartitionColumn   = id;
objects.query.sector.SectorPrms-riskPartitionColumn       = id;

objects.query.sector.SectorPrms-sectorCreateTableIndexes     = primaryKeyIndexOnSectorId;
objects.query.sector.SectorPrms-instrumentCreateTableIndexes = primaryKeyIndexOnInstrumentId;
objects.query.sector.SectorPrms-positionCreateTableIndexes   = primaryKeyIndexOnPositionId;
objects.query.sector.SectorPrms-riskCreateTableIndexes       = primaryKeyIndexOnRiskId;

objects.query.sector.SectorPrms-sectorColocationStatement     = none;
objects.query.sector.SectorPrms-instrumentColocationStatement = none;
objects.query.sector.SectorPrms-positionColocationStatement   = none;
objects.query.sector.SectorPrms-riskColocationStatement       = none;

//------------------------------------------------------------------------------
// Performance
//------------------------------------------------------------------------------

cacheperf.comparisons.gemfirexd.QueryPerfPrms-txIsolation = ${txIsolation};

perffmwk.HistogramStatsPrms-statisticsSpecification = $JTESTS/cacheperf/comparisons/gemfirexd/statspecs/queryHistogram.spec;
perffmwk.PerfReportPrms-statisticsSpecification = $JTESTS/cacheperf/comparisons/gemfirexd/statspecs/query.spec;
