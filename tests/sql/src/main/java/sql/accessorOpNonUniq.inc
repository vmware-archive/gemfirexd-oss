INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/systemparams2.inc;
INCLUDE $JTESTS/hydraconfig/topology_p2p_2.inc;

/*
hydra.VmPrms-extraClassPaths        +=   "/gcm/where/java/derby/derby-10.4.2.0/jars/insane/derby.jar";
hydra.VmPrms-extraClassPaths        +=   "/gcm/where/java/derby/derby-10.4.2.0/jars/insane/derbynet.jar";
hydra.VmPrms-extraClassPaths        +=   "/gcm/where/java/derby/derby-10.4.2.0/jars/insane/derbyclient.jar";
*/
hydra.VmPrms-extraClassPaths        += $GEMFIRE/../product-gfxd/lib/gemfirexd.jar;

//hydra.GemFirePrms-roles += "gemfirexd.datastore"	   "gemfirexd.accessor"; // setup accessor & dataStore
            
THREADGROUP dataStoreThreads
            totalThreads = fcn "(${${A}Hosts} * ${${A}VMsPerHost} *  ${${A}ThreadsPerVM}) " ncf
            totalVMs     = fcn "(${${A}Hosts} * ${${A}VMsPerHost})" ncf
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${A}\",
                                ${${A}Hosts}, true)" ncf;

THREADGROUP accessorThreads
            totalThreads = fcn "(${${B}Hosts} * ${${B}VMsPerHost} *  ${${B}ThreadsPerVM}) -1  " ncf
            totalVMs     = fcn "(${${B}Hosts} * ${${B}VMsPerHost})" ncf
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${B}\",
                                ${${B}Hosts}, true)" ncf;  

THREADGROUP ddlThread
            totalThreads = 1
            totalVMs     = 1
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${B}\",
                                ${${B}Hosts}, true)" ncf;     
                                                                
INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_initialize
	threadGroups = dataStoreThreads, ddlThread, accessorThreads;		

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createGFEDB
	threadGroups = dataStoreThreads;
	
INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createGFEDBForAccessors
	threadGroups = accessorThreads;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createGFESchemas
	threadGroups = ddlThread;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createGFETables
	threadGroups = ddlThread;
	
INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createFuncForProcedures
	threadGroups = ddlThread;
	
INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createFuncForSubquery
	threadGroups = ddlThread;	
	
INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_populateTables
	threadGroups = accessorThreads, ddlThread;		
		
INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_populateTables
	threadGroups = accessorThreads, ddlThread;		
		
TASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_doDMLOp
	 threadGroups = accessorThreads, ddlThread;	
	 
CLOSETASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_checkConstraints
	 threadGroups = ddlThread;	 
	 
CLOSETASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_shutDownDB
	 threadGroups = ddlThread; 

RANDOMINCLUDE $JTESTS/sql/offHeap.inc; // uses off-heap if include is randomly chosen
