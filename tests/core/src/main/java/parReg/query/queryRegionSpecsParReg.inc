hydra.RegionPrms-names           = region1;
hydra.RegionPrms-dataPolicy      = partition;
hydra.RegionPrms-statisticsEnabled = true;
hydra.RegionPrms-indexMaintenanceSynchronous  = true;
hydra.RegionPrms-partitionName  = pr;

hydra.PartitionPrms-names = pr;
hydra.PartitionPrms-redundantCopies = ONEOF 0 1 FOENO;

hydra.CachePrms-names           = cache1;
hydra.CachePrms-searchTimeout   = 600;

