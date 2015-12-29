hydra.RegionPrms-names           = region1 region2 region3 region4;
hydra.RegionPrms-dataPolicy      = partition partition persistentPartition persistentPartition;
hydra.RegionPrms-scope           = default;
hydra.RegionPrms-evictionAttributes = none, 
                                      lruHeapPercentage default overflowToDisk, 
                                      none,
                                      lruHeapPercentage default overflowToDisk;
hydra.RegionPrms-statisticsEnabled = true;
hydra.RegionPrms-valueConstraint  = objects.PRPortfolio;
hydra.RegionPrms-indexMaintenanceSynchronous  = ONEOF true false FOENO;

hydra.CachePrms-names           = cache1;
hydra.CachePrms-searchTimeout   = 600;

hydra.RegionPrms-diskStoreName  = none synchWrite disk1 disk2;
hydra.RegionPrms-diskSynchronous = true;

hydra.DiskStorePrms-names       = none synchWrite disk1 disk2;
hydra.DiskStorePrms-maxOplogSize = 10240;
hydra.DiskStorePrms-queueSize = ONEOF 5 10 20 FOENO;
hydra.DiskStorePrms-timeInterval = oneof 50 500 1000 2000 foeno;

hydra.RegionPrms-partitionName  = pr1 pr2 pr3 pr4;
hydra.PartitionPrms-names = pr1 pr2 pr3 pr4;
hydra.PartitionPrms-redundantCopies = ONEOF 0 1 FOENO;
