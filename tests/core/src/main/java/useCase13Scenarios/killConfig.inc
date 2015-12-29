hydra.CachePrms-names                = primary secondary;
hydra.RegionPrms-names               = primary secondary;
hydra.RegionPrms-diskStoreName       = disk    none;
hydra.RegionPrms-cacheListeners      = none,   cacheperf.gemfire.SimpleListener;
hydra.RegionPrms-dataPolicy          = persistentReplicate replicate;
hydra.RegionPrms-scope               = distributedNoAck;
hydra.RegionPrms-diskSynchronous     = false;

hydra.DiskStorePrms-names = disk;
hydra.DiskStorePrms-queueSize = 0;
hydra.DiskStorePrms-timeInterval = 1000;
