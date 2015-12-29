hydra.GemFirePrms-stopSystemsAfterTest = true;

// delta.DeltaPropagationPrms-secondsToRun determines the running length rather than
// totalTaskTimeSec because of the test's pausing scheme; without this the
// test could hang
hydra.Prms-totalTaskTimeSec = 31536000;
delta.DeltaPropagationPrms-secondsToRun = 600;
hydra.Prms-maxResultWaitSec = 600;

hydra.GemFirePrms-conserveSockets = ONEOF true false FOENO;

hydra.CachePrms-names           = cache1;
hydra.CachePrms-searchTimeout   = 600;

hydra.RegionPrms-cloningEnabled = ONEOF true false FOENO;

util.TestHelperPrms-minTaskGranularitySec = 30;

// entryOperations used in peer runs; 
// serverEntryOperations and clientEntryOperations used in bridge runs
delta.DeltaPropagationPrms-entryOperations = ONEOF add add update update update update invalidate destroy FOENO; 
delta.DeltaPropagationPrms-serverEntryOperations = ONEOF add add update update update update invalidate destroy FOENO; 
delta.DeltaPropagationPrms-clientEntryOperations = ONEOF add update update update destroy FOENO; 
delta.DeltaPropagationPrms-upperThreshold = 500;
delta.DeltaPropagationPrms-upperThresholdOperations = destroy;
delta.DeltaPropagationPrms-lowerThreshold = 0;
delta.DeltaPropagationPrms-lowerThresholdOperations = add;
delta.DeltaPropagationPrms-useUniqueKeys = true; // whether peer regions or PR
                                    // we need to work around ordering problems so
                                    // verification can be done

