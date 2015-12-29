STARTTASK taskClass   = util.CacheBB taskMethod  = HydraTask_initialize
          clientNames = client1a
          ;

INITTASK taskClass   = cacheLoader.smoke.Client  taskMethod  = initClientTask
          ;

INITTASK taskClass   = cacheLoader.smoke.Client  taskMethod  = defineCacheRegions
	  threadGroups = cacheLoaderAndUpdater
	  cacheLoader.smoke.TestParms-defineCacheLoader = true
          ;

INITTASK taskClass   = cacheLoader.smoke.Client  taskMethod  = defineCacheRegions
	  threadGroups = otherInitializers
	  cacheLoader.smoke.TestParms-defineCacheLoader = false
          ;

INITTASK taskClass   = cacheLoader.smoke.Client  taskMethod  = initializeDataTask
	  threadGroups = cacheLoaderAndUpdater
          ;

CLOSETASK taskClass   = cacheLoader.smoke.Client  taskMethod  = summarizeClientTask
          ;

CLOSETASK taskClass   = cacheLoader.smoke.Client  taskMethod  = verifyResultsTask
	  threadGroups = otherInitializers
          ;

CLOSETASK taskClass   = util.DistributionStats  taskMethod  = reportDistributionStats
          ;

CLOSETASK taskClass   = cacheLoader.smoke.Client  taskMethod  = closeCacheTask
          ;

ENDTASK   taskClass   = cacheLoader.smoke.Client  taskMethod  = defineCacheRegions
          clientNames = client1a, client2a
	  cacheLoader.smoke.TestParms-defineCacheLoader = false
          ;

ENDTASK   taskClass   = util.DistributionStats  taskMethod  = reportDistributionStats
          clientNames = client1a, client2a
          ;

ENDTASK   taskClass   = cacheLoader.smoke.Client  taskMethod  = verifyFinalResultsTask
          clientNames = client1a, client2a
          ;

ENDTASK   taskClass   = cacheLoader.smoke.Client  taskMethod  = closeCacheTask
          clientNames = client1a, client2a
          ;

hydra.Prms-totalTaskTimeSec = 600;
