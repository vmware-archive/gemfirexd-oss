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

// conf files which use this "inc" file must define creator threadgroup.

hydra.GemFirePrms-conserveSockets = true;

hydra.Prms-totalTaskTimeSec = 120;
hydra.Prms-maxResultWaitSec = 1200;

dlock.DLockPrms-blackboardName = "DLock";
dlock.DLockPrms-blackboardType = "rmi";

dlock.DLockPrms-scope = distributedAck;
dlock.DLockPrms-useEntryLock = false;
dlock.DLockPrms-getLockFirst = true;
dlock.DLockPrms-numTimesToEnterLock = RANGE 1 10 EGNAR;
