hydra.RegionPrms-names           = region1 region2;
hydra.RegionPrms-dataPolicy      = replicate persistentReplicate;
hydra.RegionPrms-scope           = ONEOF ack noack global FOENO;
hydra.RegionPrms-evictionAttributes	= lruHeapPercentage default overflowToDisk, 
                                      lruHeapPercentage default overflowToDisk;
hydra.RegionPrms-statisticsEnabled = true;
hydra.RegionPrms-valueConstraint  = objects.Portfolio;
hydra.RegionPrms-indexMaintenanceSynchronous  = ONEOF true false FOENO;

hydra.CachePrms-names           = cache1;
hydra.CachePrms-searchTimeout   = 600;

hydra.RegionPrms-diskStoreName  = disk1 disk2;
hydra.RegionPrms-diskSynchronous = true;

hydra.DiskStorePrms-names       = disk1 disk2;
hydra.DiskStorePrms-maxOplogSize = 10240;
hydra.DiskStorePrms-queueSize = ONEOF 5 10 20 FOENO;
hydra.DiskStorePrms-timeInterval = oneof 50 500 1000 2000 foeno;