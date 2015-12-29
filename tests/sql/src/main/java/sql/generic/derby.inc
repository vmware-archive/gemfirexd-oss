// contains derby related configuration

hydra.VmPrms-extraClassPaths        +=   "/export/gcm/where/java/derby/derby-10.8.2.2/jars/insane/derby.jar";
hydra.VmPrms-extraClassPaths        +=   "/export/gcm/where/java/derby/derby-10.8.2.2/jars/insane/derbyclient.jar";
hydra.Prms-derbyServerClassPath     +=   "/export/gcm/where/java/derby/derby-10.8.2.2/jars/insane/derbynet.jar";
hydra.VmPrms-extraClassPaths        +=   "/gcm/where/java/derby/derby-10.8.2.2/jars/insane/derbytools.jar";
hydra.Prms-extraDerbyServerVMArgs   += " -Xmx1024m -Dderby.storage.pageCacheSize=32000 -Dderby.locks.waitTimeout=30 -Dderby.locks.deadlockTimeout=20 ";

INITTASK     taskClass   = sql.generic.SQLTestExecutor taskMethod  = HydraTask_createDiscDB
	threadGroups = accessorThreads;

INITTASK     taskClass   = sql.generic.SQLTestExecutor taskMethod  = HydraTask_createDiscSchemas
	threadGroups = ddlThread;

INITTASK     taskClass   = sql.generic.SQLTestExecutor taskMethod  = HydraTask_createDiscTables
	threadGroups = ddlThread;