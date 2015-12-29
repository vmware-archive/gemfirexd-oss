//------------------------------------------------------------------------------
// INCLUDE FILES
//------------------------------------------------------------------------------

include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/topology_wan_p2p_locator.inc;
include $JTESTS/newWan/wan.inc;
include $JTESTS/newWan/p2p.inc;

wan.CacheClientPrms-cacheConfig = peer;
wan.CacheClientPrms-regionConfig = peer;

// use ValueHolder, encode opNum in 'modVal' field
INCLUDE $JTESTS/util/randomValues.inc;
util.RandomValuesPrms-objectType = byte[];
util.RandomValuesPrms-elementSize = ${byteArraySize};
util.ValueHolderPrms-useExtraObject = true;

//------------------------------------------------------------------------------
// TEST DESCRIPTION
//------------------------------------------------------------------------------

hydra.Prms-testDescription="WAN test where each site is peer-to-peer in its own distributed system and each VM is a WAN gateway sender to other sites.";

util.TestHelperPrms-minTaskGranularitySec = 600;

hydra.Prms-totalTaskTimeSec = 30;
hydra.Prms-maxResultWaitSec = 800;

//------------------------------------------------------------------------------
// INITIAL WORK (configure locators, caches, hubs, gateways)
//------------------------------------------------------------------------------

INITTASK taskClass = newWan.orderPolicy.NewWanOrderPolicyTest taskMethod = createLocatorTask
         threadGroups = locator;
INITTASK taskClass = newWan.orderPolicy.NewWanOrderPolicyTest taskMethod = startAndConnectLocatorTask
         threadGroups = locator;
INITTASK taskClass = newWan.orderPolicy.NewWanOrderPolicyTest taskMethod = initPeerForOrderPolicyTest
         runMode = always
         threadGroups = peer;
INITTASK taskClass = newWan.orderPolicy.NewWanOrderPolicyTest taskMethod = HydraTask_initWANComponentsTask
         runMode = always
         threadGroups = peer;
INITTASK taskClass = newWan.orderPolicy.NewWanOrderPolicyTest taskMethod = initializeInstanceTask
         runMode = always
         threadGroups = peer;

//------------------------------------------------------------------------------
// FINAL WORK (validate and close)
//------------------------------------------------------------------------------

CLOSETASK taskClass = newWan.orderPolicy.NewWanOrderPolicyTest taskMethod = waitForQueuesToDrain
          threadGroups = peer;          
CLOSETASK taskClass = newWan.orderPolicy.NewWanOrderPolicyTest taskMethod = printSequentialKeysTask
          threadGroups = peer;
CLOSETASK taskClass = newWan.orderPolicy.NewWanOrderPolicyTest taskMethod = checkForEventErrors
          threadGroups = peer;
CLOSETASK taskClass = newWan.orderPolicy.NewWanOrderPolicyTest taskMethod = closePeerCacheTask
          threadGroups = peer;
CLOSETASK taskClass = wan.WANBlackboard taskMethod = printBlackboard
          threadGroups = peer;

