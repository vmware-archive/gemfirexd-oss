INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/systemparams3.inc;

hydra.ClientPrms-vmQuantities = 2 2 2;
hydra.ClientPrms-vmThreads    = 5 5 5;

hydra.Prms-totalTaskTimeSec = 1200;
hydra.Prms-maxResultWaitSec = 1200;
hydra.Prms-haltIfBadResult = true;
hydra.Prms-maxClientShutdownWaitSec = 240;
hydra.Prms-finalClientSleepSec = 60;
hydra.Prms-maxEndTaskResultWaitSec = 400;

hydra.VmPrms-extraVMArgs        = -Xmx500m;

hydra.ConfigPrms-cacheConfig = gemfireCache;
hydra.CachePrms-names = gemfireCache;

// Logging params
hydra.log.LogPrms-file_logging = true;

util.RandomValuesPrms-borderCasePercentage = 0;
