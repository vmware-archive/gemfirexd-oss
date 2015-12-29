include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/systemparams2.inc;

INITTASK  taskClass = dlock.BasicDLockClient taskMethod = createCacheTask
          threadGroups = creator 
          ;
INITTASK  taskClass = dlock.BasicDLockClient taskMethod = createDataTask
          threadGroups = creator
          ;
INITTASK  taskClass = dlock.BasicDLockClient taskMethod = initTask
          ;
CLOSETASK taskClass = dlock.BasicDLockClient taskMethod = closeTask
          ;

CLOSETASK taskClass = dlock.BasicDLockClient taskMethod = validateTask
          threadGroups = creator
          ;

CLOSETASK taskClass = dlock.BasicDLockClient taskMethod = closeCacheTask
          threadGroups = creator
          ;

//ENDTASK   taskClass = util.DistributionStats taskMethod = reportDistributionStats
//          clientNames = client1
//          ;
ENDTASK   taskClass = dlock.DLockBlackboard  taskMethod = printBlackboard
          clientNames = client1
          ;

//THREADGROUP creator totalThreads = 4 totalVMs = 4;

hydra.GemFirePrms-conserveSockets = true;

hydra.Prms-totalTaskTimeSec = 120;
hydra.Prms-maxResultWaitSec = 600;

dlock.DLockPrms-blackboardName = "Global";
dlock.DLockPrms-blackboardType = "rmi";

dlock.DLockPrms-scope = global;
dlock.DLockPrms-useEntryLock = false;
dlock.DLockPrms-getLockFirst = false;
dlock.DLockPrms-numTimesToEnterLock = RANGE 1 10 EGNAR;

// set lease time and lock timeout high so Hydra will timeout hung client
dlock.DLockPrms-cacheLeaseTime = 300000;
dlock.DLockPrms-cacheLockTimeout = 300000;
