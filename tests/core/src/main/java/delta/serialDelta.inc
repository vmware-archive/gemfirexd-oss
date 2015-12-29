hydra.GemFirePrms-stopSystemsAfterTest = true;

hydra.Prms-totalTaskTimeSec = 900; // test is workload based, or it will run 15 minutes max
hydra.Prms-maxResultWaitSec = 600;
hydra.Prms-serialExecution = true;
hydra.Prms-roundRobin = true;

hydra.GemFirePrms-conserveSockets = ${conserveSockets};

hydra.CachePrms-names           = cache1;
hydra.CachePrms-searchTimeout   = 600;

hydra.RegionPrms-cloningEnabled = ${cloningEnabled};

delta.DeltaPropagationPrms-numOpsPerTask = 100;

// entryOperations used in peer runs; 
// serverEntryOperations and clientEntryOperations used in bridge runs
delta.DeltaPropagationPrms-entryOperations = ONEOF add update update update update invalidate destroy FOENO; 
delta.DeltaPropagationPrms-serverEntryOperations = ONEOF add update update update update invalidate destroy FOENO; 
delta.DeltaPropagationPrms-clientEntryOperations = ONEOF add update update update destroy FOENO; 
delta.DeltaPropagationPrms-upperThreshold = 300;
delta.DeltaPropagationPrms-upperThresholdOperations = destroy;
delta.DeltaPropagationPrms-lowerThreshold = 0;
delta.DeltaPropagationPrms-lowerThresholdOperations = add;
