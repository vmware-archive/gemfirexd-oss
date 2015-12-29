hydra.RegionPrms-names           = region1 region2;
hydra.RegionPrms-dataPolicy      = replicate persistentReplicate;
hydra.RegionPrms-scope           = ONEOF ack noack global FOENO;
hydra.RegionPrms-evictionAttributes = none, 
                                      none;
hydra.RegionPrms-statisticsEnabled = true;
hydra.RegionPrms-valueConstraint  = objects.PRPortfolio;
hydra.RegionPrms-indexMaintenanceSynchronous  = ONEOF true false FOENO;

hydra.CachePrms-names           = cache1;
hydra.CachePrms-searchTimeout   = 600;

hydra.RegionPrms-diskStoreName  = none synchWrite;
hydra.RegionPrms-diskSynchronous = true;

hydra.DiskStorePrms-names       = none synchWrite;
hydra.DiskStorePrms-maxOplogSize = 10240;
hydra.DiskStorePrms-queueSize = ONEOF 5 10 20 FOENO;
hydra.DiskStorePrms-timeInterval = oneof 50 500 1000 2000 foeno;