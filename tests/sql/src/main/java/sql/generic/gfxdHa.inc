// configuration related to member fail-over

sql.generic.SQLGenericPrms-isHA = ${isHA};

util.StopStartPrms-stopModes = ONEOF NICE_EXIT MEAN_KILL MEAN_EXIT NICE_KILL FOENO;
util.StopStartPrms-numVMsToStop = ${numVMsToStop};

INITTASK     taskClass   = util.PRObserver  taskMethod = initialize
             runMode = once
             threadGroups = dataStoreThreads;

INITTASK     taskClass   = util.StopStartVMs  taskMethod = StopStart_initTask;

TASK         taskClass   = sql.generic.SQLTestExecutor taskMethod  = HydraTask_cycleStoreVms
		     MAXTHREADS = 1
             threadGroups = accessorThreads;
