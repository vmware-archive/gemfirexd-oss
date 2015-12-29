hydra.Prms-testDescription += "Uses persistent regions.";

hydra.ConfigPrms-diskStoreConfig = ycsb;

hydra.DiskStorePrms-names = ycsb;
hydra.DiskStorePrms-autoCompact = false;

hydra.RegionPrms-dataPolicy      = persistentPartition partition;
hydra.RegionPrms-diskStoreName   = ycsb                none;
hydra.RegionPrms-diskSynchronous = true                default;
