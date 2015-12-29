hydra.ConfigPrms-cacheConfig = cache;
hydra.CachePrms-names = cache;

hydra.RegionPrms-names          = pub     sub;
hydra.RegionPrms-dataPolicy     = default replicate;
hydra.RegionPrms-cacheListeners = none,   cacheperf.gemfire.LatencyListener;
hydra.RegionPrms-scope          = ${scope};

hydra.GemFirePrms-conserveSockets = ${conserveSockets};

// TO TURN ON MULTICAST
//hydra.RegionPrms-multicastEnabled = true;
//hydra.GemFirePrms-enableMcast = true;
//hydra.GemFirePrms-disableTcp = true;
//hydra.GemFirePrms-useLocator = false;

// TO CONFIGURE MULTICAST
//hydra.GemFirePrms-mcastTtl = 4;
//hydra.GemFirePrms-socketBufferSize=32768; // default 32768
//hydra.GemFirePrms-socketLeaseTime=15000;  // default 15000
//hydra.GemFirePrms-udpRecvBufferSize=20000000; // default 640000
//hydra.GemFirePrms-udpSendBufferSize=200000;   // default 640000
//hydra.GemFirePrms-udpFragmentSize=60000;      // default 30000
//hydra.GemFirePrms-mcastRecvBufferSize=20000000; // default 640000
//hydra.GemFirePrms-mcastSendBufferSize=200000;   // default 640000
//hydra.GemFirePrms-mcastFlowControlByteAllowance=15000000; // default 100000
//hydra.GemFirePrms-mcastFlowControlRechargeThreshold=0.25; // default 0.25
//hydra.GemFirePrms-mcastFlowControlRechargeBlockMs=5000;   // default 5000

THREADGROUP pub
    totalThreads = fcn
                   ${pubHosts} * ${pubVMsPerHost} * ${pubThreadsPerVM}
                   ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"pub\", ${pubHosts}, true)"
                   ncf;
THREADGROUP sub
    totalThreads = fcn
                   ${subHosts} * ${subVMsPerHost} * ${subThreadsPerVM}
                   ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"sub\", ${subHosts}, true)"
                   ncf;

cacheperf.CachePerfPrms-batchTerminatorMethod = terminateOnBatchSeconds;
cacheperf.CachePerfPrms-batchSeconds   = 30;

cacheperf.CachePerfPrms-warmupTerminatorMethod = terminateOnTrimSeconds;
cacheperf.CachePerfPrms-warmupTerminatorFrequency = 30 seconds;
cacheperf.CachePerfPrms-trimSeconds    = 180;

cacheperf.CachePerfPrms-taskTerminatorMethod = terminateOnTotalSeconds;
cacheperf.CachePerfPrms-taskTerminatorFrequency = 30 seconds;
cacheperf.CachePerfPrms-workSeconds    = 420;

hydra.Prms-maxResultWaitSec = 300;
hydra.Prms-totalTaskTimeSec = 999999; // see terminators
