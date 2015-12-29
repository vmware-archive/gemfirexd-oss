hydra.Prms-totalTaskTimeSec = 60;
hydra.Prms-maxResultWaitSec = 600;

hydra.GemFirePrms-conserveSockets = ONEOF true false FOENO;

hydra.CachePrms-names           = cache1;
hydra.CachePrms-searchTimeout   = 600;

hydra.RegionPrms-cloningEnabled = ONEOF true false FOENO;

util.TestHelperPrms-minTaskGranularitySec = 30;
delta.DeltaPropagationPrms-numOpsPerTask  = 50;

hydra.Prms-useFixedRandomInMaster= true;
hydra.RegionPrms-names             = replicateReg 
                                     normalReg 
                                     emptyReg 
                                     partitionedRegAccessor 
                                     partitionedRegDataStore 
                                     persistentRepReg 
                                     preloadedReg

                                     client_replicateReg
                                     client_normalReg
                                     client_partitionedReg
                                     client_persistentRepReg
                                     client_preloadedReg;
hydra.RegionPrms-regionName        = replicateReg 
                                     normalReg
                                     normalReg
                                     partitionedReg
                                     partitionedReg
                                     persistentRepReg
                                     preloadedReg

                                     replicateReg
                                     normalReg
                                     partitionedReg
                                     persistentRepReg
                                     preloadedReg;
hydra.RegionPrms-scope             = ONEOF ack global FOENO
                                     ONEOF ack global FOENO
                                     ONEOF ack global FOENO
                                     default
                                     default
                                     ONEOF ack global FOENO
                                     ONEOF ack global FOENO 
                                     local local local local local;
hydra.RegionPrms-dataPolicy        = replicate
                                     normal
                                     empty
                                     partition
                                     partition
                                     persistent_replicate
                                     preloaded
                                     default default default default default;
hydra.RegionPrms-cacheListeners    = delta.DeltaTestListener;
hydra.RegionPrms-diskStoreName     = none none none none none diskPrms1 none 
                                     none none none none none;
hydra.RegionPrms-partitionName     = none none none prAccessor prDataStore none none none
                                     none none none none none;
hydra.RegionPrms-diskSynchronous   = false;
hydra.PartitionPrms-names           = prAccessor          prDataStore;
hydra.PartitionPrms-localMaxMemory  = 0                   default;
hydra.PartitionPrms-redundantCopies = ONEOF 0 1 2 3 FOENO;

hydra.DiskStorePrms-names = diskPrms1;

// entryOperations used in peer runs; 
// serverEntryOperations and clientEntryOperations used in bridge runs
delta.DeltaPropagationPrms-entryOperations = ONEOF add add update update update update invalidate destroy FOENO; 
delta.DeltaPropagationPrms-serverEntryOperations = ONEOF add add update update update update invalidate destroy FOENO; 
delta.DeltaPropagationPrms-clientEntryOperations = ONEOF add update update update destroy FOENO; 
delta.DeltaPropagationPrms-upperThreshold = 500;
delta.DeltaPropagationPrms-upperThresholdOperations = destroy;
delta.DeltaPropagationPrms-lowerThreshold = 0;
delta.DeltaPropagationPrms-lowerThresholdOperations = add;
delta.DeltaPropagationPrms-useUniqueKeys = false;

