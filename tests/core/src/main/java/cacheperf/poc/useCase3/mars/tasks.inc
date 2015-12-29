//------------------------------------------------------------------------------
// Server Locators and Distributed Systems
//

hydra.Prms-manageLocatorAgents = false;

INITTASK  taskClass = cacheperf.poc.useCase3.mars.MarsServer
          taskMethod = createLocatorTask
          threadgroups = trade,indicative,market,position,risk
          ;
INITTASK  taskClass = cacheperf.poc.useCase3.mars.MarsServer
          taskMethod = startLocatorAndDSTask
          threadgroups = trade,indicative,market,position,risk
          ;
INITTASK  taskClass = cacheperf.CachePerfClient
          taskMethod = openStatisticsTask
          threadgroups = trade,indicative,market,position,risk
          ;

//------------------------------------------------------------------------------
// Server Caches
//

INITTASK  taskClass = cacheperf.poc.useCase3.mars.MarsTasks
          taskMethod = openCacheTask
          threadgroups = trade,indicative,market,position,risk
          hydra.ConfigPrms-cacheConfig = server
          ;

//------------------------------------------------------------------------------
// Server Regions
//

INITTASK  taskClass = cacheperf.poc.useCase3.mars.MarsTasks
          taskMethod = createRegionTask
          threadGroups = trade
          hydra.ConfigPrms-regionConfig = server_trade_command
          ;
INITTASK  taskClass = cacheperf.poc.useCase3.mars.MarsTasks
          taskMethod = createRegionTask
          threadGroups = indicative
          hydra.ConfigPrms-regionConfig = server_indicative_command
          ;
INITTASK  taskClass = cacheperf.poc.useCase3.mars.MarsTasks
          taskMethod = createRegionTask
          threadGroups = market
          hydra.ConfigPrms-regionConfig = server_market_command
          ;
INITTASK  taskClass = cacheperf.poc.useCase3.mars.MarsTasks
          taskMethod = createRegionTask
          threadGroups = position
          hydra.ConfigPrms-regionConfig = server_position_command
          ;
INITTASK  taskClass = cacheperf.poc.useCase3.mars.MarsTasks
          taskMethod = createRegionTask
          threadGroups = risk
          hydra.ConfigPrms-regionConfig = server_risk_command
          ;

//------------------------------------------------------------------------------
// Bridge Servers
//

INITTASK  taskClass = cacheperf.poc.useCase3.mars.MarsServer
          taskMethod = startCacheServerTask
          threadgroups = trade,indicative,market,position,risk
          hydra.ConfigPrms-bridgeConfig = server
          ;

//------------------------------------------------------------------------------
// Gateway Hubs and Gateways
//

INITTASK  taskClass = cacheperf.CachePerfClient
          taskMethod = createGatewayHubTask
          threadgroups = trade,indicative,market,position
          hydra.ConfigPrms-gatewayHubConfig = DB
          ;
INITTASK  taskClass = cacheperf.CachePerfClient
          taskMethod = createGatewayHubTask
          threadgroups = risk
          hydra.ConfigPrms-gatewayHubConfig = SERVER_DB
          ;
INITTASK  taskClass = cacheperf.CachePerfClient
          taskMethod = addWBCLGatewayTask
          threadgroups = trade,indicative,market,position
          hydra.ConfigPrms-gatewayConfig = DB
          ;
INITTASK  taskClass = cacheperf.CachePerfClient
          taskMethod = addWBCLGatewayTask
          threadgroups = risk
          hydra.ConfigPrms-gatewayConfig = SERVER_DB
          ;
INITTASK  taskClass = cacheperf.CachePerfClient
          taskMethod = startGatewayHubTask
          threadgroups = trade,indicative,market,position
          ;
INITTASK  taskClass = cacheperf.CachePerfClient
          taskMethod = startGatewayHubTask
          threadgroups = risk
          ;

//------------------------------------------------------------------------------
// Client Distributed Systems
//
// must connect after bridge servers start so that endpoints are available
//

INITTASK  taskClass = cacheperf.CachePerfClient
          taskMethod = openStatisticsTask
          threadgroups = client
          ;

//------------------------------------------------------------------------------
// Client Caches
//

INITTASK  taskClass = cacheperf.poc.useCase3.mars.MarsTasks
          taskMethod = openCacheTask
          threadgroups = client
          hydra.ConfigPrms-cacheConfig = client
          ;

//------------------------------------------------------------------------------
// Client Regions
//

INITTASK  taskClass = cacheperf.poc.useCase3.mars.MarsTasks
          taskMethod = createRegionTask
          threadGroups = client
          hydra.ConfigPrms-regionConfig = client_trade_command
          ;
INITTASK  taskClass = cacheperf.poc.useCase3.mars.MarsTasks
          taskMethod = createRegionTask
          threadGroups = client
          hydra.ConfigPrms-regionConfig = client_indicative_command
          ;
INITTASK  taskClass = cacheperf.poc.useCase3.mars.MarsTasks
          taskMethod = createRegionTask
          threadGroups = client
          hydra.ConfigPrms-regionConfig = client_market_command
          ;
INITTASK  taskClass = cacheperf.poc.useCase3.mars.MarsTasks
          taskMethod = createRegionTask
          threadGroups = client
          hydra.ConfigPrms-regionConfig = client_position_command
          ;
INITTASK  taskClass = cacheperf.poc.useCase3.mars.MarsTasks
          taskMethod = createRegionTask
          threadGroups = client
          hydra.ConfigPrms-regionConfig = client_risk_command
          ;

/*==============================================================================

INITTASK  taskClass = cacheperf.poc.useCase3.UseCase3Client taskMethod = createUseCase3RegionsTask
          threadGroups = bridge
          ;
INITTASK  taskClass = cacheperf.poc.useCase3.UseCase3Client taskMethod = createUseCase3IndexesTask
          threadGroups = bridge
          ;
INITTASK  taskClass = cacheperf.poc.useCase3.UseCase3Client taskMethod = createUseCase3DataTask
          batch
          cacheperf.CachePerfPrms-keyAllocation = ownKeys
          cacheperf.CachePerfPrms-warmupTerminatorMethod = none
          cacheperf.CachePerfPrms-taskTerminatorMethod = terminateOnMaxKey
          cacheperf.CachePerfPrms-taskTerminatorFrequency = 1 iterations
          threadGroups = bridge
          ;
TASK      taskClass = cacheperf.poc.useCase3.UseCase3Client taskMethod = queryUseCase3DataTask
          cacheperf.CachePerfPrms-keyAllocation = sameKeysRandomWrap
          cacheperf.poc.useCase3.UseCase3Prms-numPutsBetweenQueries = ${numPutsBetweenQueries}
          cacheperf.CachePerfPrms-sleepMs = ${sleepMs}
          threadGroups = edge
          ;

=============================================================================*/

//------------------------------------------------------------------------------
// Client Shutdown
//

CLOSETASK taskClass = cacheperf.CachePerfClient
          taskMethod = closeStatisticsTask
          threadgroups = client
          ;

//------------------------------------------------------------------------------
// Server Shutdown
//

CLOSETASK taskClass = cacheperf.CachePerfClient
          taskMethod = stopGatewayHubTask
          threadgroups = trade,indicative,market,position,risk
          ;
CLOSETASK taskClass = cacheperf.poc.useCase3.mars.MarsServer
          taskMethod = stopCacheServerTask
          threadgroups = trade,indicative,market,position,risk
          ;
CLOSETASK taskClass = cacheperf.poc.useCase3.mars.MarsServer
          taskMethod = stopLocatorTask
          threadgroups = trade,indicative,market,position,risk
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient
          taskMethod = closeStatisticsTask
          threadgroups = trade,indicative,market,position,risk
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient
          taskMethod = reportTrimIntervalsTask
          threadgroups = trade,indicative,market,position,risk,client
          ;
