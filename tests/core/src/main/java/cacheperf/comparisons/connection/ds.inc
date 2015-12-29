include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/performance.inc;
include $JTESTS/hydraconfig/topology_p2p.inc;

hydra.Prms-testDescription = "Scales the number of peers connected to a distributed system.";
hydra.Prms-testRequirement = "The number of members possible is very large.";

hydra.Prms-totalTaskTimeSec = 300;
hydra.Prms-maxResultWaitSec = 600;

//------------------------------------------------------------------------------
// Tasks
//------------------------------------------------------------------------------

hydratest.locators.LocatorPrms-expectedMembers =
          fcn ${peerHosts} * ${peerVMsPerHost} ncf;

INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = connectTask
          ;
INITTASK  taskClass = hydratest.locators.LocatorClient
          taskMethod = validateExpectedMembersTask
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = disconnectTask
          ;

//------------------------------------------------------------------------------
// Data
//------------------------------------------------------------------------------

cacheperf.CachePerfPrms-maxKeys = 1; // not used

//------------------------------------------------------------------------------
// helps mitigate Bug 39849 for sequential case and fixes it for concurrent

hydra.VmPrms-extraVMArgs += "-Dp2p.HANDSHAKE_POOL_SIZE=${handshakePoolSize}";
