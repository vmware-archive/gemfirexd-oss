hydra.RegionPrms-names           = region1;
hydra.RegionPrms-dataPolicy      = replicate;
hydra.RegionPrms-scope           = ONEOF ack noack global FOENO;
hydra.RegionPrms-statisticsEnabled = true;
hydra.RegionPrms-valueConstraint  = objects.Portfolio;
hydra.RegionPrms-indexMaintenanceSynchronous  = ONEOF true false FOENO;

hydra.CachePrms-names           = cache1;
hydra.CachePrms-searchTimeout   = 600;


