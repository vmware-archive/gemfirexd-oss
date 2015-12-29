//// hydraparams1.inc: general, vm, and client parameters, but no gemfire parameters

hydra.Prms-useIPv6          = false;
hydra.Prms-useNFS           = true;

hydra.Prms-totalTaskTimeSec = 60;
hydra.Prms-serialExecution  = false;
hydra.Prms-roundRobin       = false;
hydra.Prms-haltIfBadResult  = true;

hydra.Prms-checkTaskMethodsExist     = true;
hydra.Prms-doInitTasksSequentially   = false;
hydra.Prms-doStartAndEndTasksLockStep = false;
hydra.Prms-alwaysDoEndTasks          = false;

hydra.Prms-manageDerbyServer                = false;
hydra.Prms-manageLocatorAgents              = true;
hydra.Prms-startLocatorAgentsBeforeTest     = true;
hydra.Prms-stopLocatorAgentsAfterTest       = true;
hydra.Prms-dumpStacksOnError                = true;
hydra.Prms-stopSystemsOnError               = true;

hydra.Prms-sleepBeforeShutdownSec      =   0;
hydra.Prms-logLevelForHostAgent        = info;
hydra.Prms-maxHostAgentStartupWaitSec  = 300;
hydra.Prms-maxHostAgentShutdownWaitSec = 300;
hydra.Prms-maxClientStartupWaitSec     = 300;
hydra.Prms-maxClientShutdownWaitSec    = 300;
hydra.Prms-maxResultWaitSec            = 300;

hydra.Prms-initialClientSleepSec = 0;
hydra.Prms-finalClientSleepSec = 0;

hydra.HostPrms-names     = host;

hydra.VmPrms-names     = vm;
hydra.VmPrms-hostNames = host;

hydra.ClientPrms-names        = client;
hydra.ClientPrms-vmNames      = vm;
hydra.ClientPrms-vmQuantities = 2;
hydra.ClientPrms-vmThreads    = 4;

hydra.log.LogPrms-file_logging           = true;
hydra.log.LogPrms-file_logLevel          = info;
hydra.log.LogPrms-file_maxKBPerVM        = 0;

perffmwk.PerfReportPrms-generatePerformanceReport = true;
perffmwk.PerfReportPrms-maxReportWaitSec = 300;
perffmwk.PerfReportPrms-heapSize = 300M;
