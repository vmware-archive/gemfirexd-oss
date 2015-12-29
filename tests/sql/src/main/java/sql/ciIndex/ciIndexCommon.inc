hydra.Prms-testDescription = "test case-insensitive index in = queries";
 
include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/gemfirexd/topology_client_locator.inc;

hydra.VmPrms-extraClassPaths        += $GEMFIRE/../product-gfxd/lib/gemfirexd.jar;
hydra.VmPrms-extraClassPaths        += $GEMFIRE/../product-gfxd/lib/gemfirexd-client.jar;
hydra.VmPrms-extraClassPaths        += $GEMFIRE/../product-gfxd/lib/gemfirexd-tools.jar;
hydra.VmPrms-extraClassPaths        += $GEMFIRE/../product-gfxd/lib/jline-1.0.jar;

hydra.VmPrms-extraClassPaths        +=   "/gcm/where/java/derby/derby-10.8.2.2/jars/insane/derby.jar";
hydra.VmPrms-extraClassPaths        +=   "/gcm/where/java/derby/derby-10.8.2.2/jars/insane/derbynet.jar";
hydra.VmPrms-extraClassPaths        +=   "/gcm/where/java/derby/derby-10.8.2.2/jars/insane/derbyclient.jar";
hydra.VmPrms-extraClassPaths        +=   "/gcm/where/java/derby/derby-10.8.2.2/jars/insane/derbytools.jar";
hydra.VmPrms-extraClassPaths        += $JTESTS;

hydra.VmPrms-extraVMArgs   = fcn "hydra.TestConfigFcns.duplicate
                                 (\"-Xmx128m\", ${locatorHosts}, true)"
                             ncf
                             ,
                             fcn "hydra.TestConfigFcns.duplicate
                                  (\"-Xms512m -Xmx2048m\", ${serverHosts}, true)"
                             ncf
                              ,
                             fcn "hydra.TestConfigFcns.duplicate
                                  (\"-Xmx256m \", ${clientHosts}, true)"
                             ncf;

hydra.Prms-totalTaskTimeSec           = 600;
hydra.Prms-maxResultWaitSec           = 480;
hydra.Prms-maxCloseTaskResultWaitSec  = 300;
hydra.Prms-serialExecution            = false;	

hydra.gemfirexd.FabricServerPrms-persistDD = true;
                     

THREADGROUP locator
  totalThreads = fcn  ${locatorHosts} * ${locatorVMsPerHost} * ${locatorThreadsPerVM} ncf
  totalVMs     = fcn "(${locatorHosts} * ${locatorVMsPerHost})" ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"locator\",
                                ${locatorHosts}, true)" ncf;
  
THREADGROUP serverThreads
            totalThreads = fcn ${serverHosts} * ${serverVMsPerHost} * ${serverThreadsPerVM} ncf
            totalVMs     = fcn ${serverHosts} * ${serverVMsPerHost} ncf
            clientNames  = fcn "hydra.TestConfigFcns.generateNames
                                (\"server\", ${serverHosts}, true)" ncf;                                
THREADGROUP clientThreads
            totalThreads = fcn ${clientHosts} * ${clientVMsPerHost} * ${clientThreadsPerVM} -1  ncf
            totalVMs     = fcn ${clientHosts} * ${clientVMsPerHost} ncf
            clientNames  = fcn "hydra.TestConfigFcns.generateNames
                                (\"client\", ${clientHosts}, true)" ncf;
                                
THREADGROUP ddlThread
            totalThreads = 1
            totalVMs     = 1
            clientNames  = fcn "hydra.TestConfigFcns.generateNames
                                (\"client\", ${clientHosts}, true)" ncf;      

INITTASK taskClass = sql.ciIndex.CaseInsensitiveIndexTest
		 taskMethod = HydraTask_createGfxdLocatorTask
         threadGroups = locator;
         
INITTASK taskClass = sql.ciIndex.CaseInsensitiveIndexTest
		 taskMethod = HydraTask_startGfxdLocatorTask
		 runMode = always
         threadGroups = locator;
                                
INITTASK taskClass   = sql.ciIndex.CaseInsensitiveIndexTest
		 taskMethod  = HydraTask_initializeServer
         runMode = always
         threadGroups = serverThreads;
         
INITTASK taskClass   = sql.ciIndex.CaseInsensitiveIndexTest 
		 taskMethod  = HydraTask_startFabricServer
		 runMode = always
         threadGroups = serverThreads;           
		
INITTASK taskClass   = sql.ciIndex.CaseInsensitiveIndexTest
		 taskMethod  = HydraTask_startNetworkServer
		 runMode = always
		 threadGroups =  serverThreads;    

INITTASK taskClass   = sql.ciIndex.CaseInsensitiveIndexTest
		 taskMethod  = HydraTask_initClient
		 threadGroups = clientThreads, ddlThread;				
		 
INITTASK taskClass   = sql.ciIndex.CaseInsensitiveIndexTest
		 taskMethod  = HydraTask_createGfxdObjectsByClients
		 threadGroups = ddlThread; 		
	
INITTASK taskClass   = sql.ciIndex.CaseInsensitiveIndexTest 
		 taskMethod  = HydraTask_populateDataByClients
		 threadGroups = ddlThread;      
  	            
TASK     taskClass   = sql.ciIndex.CaseInsensitiveIndexTest 
                 taskMethod  = HydraTask_queryCaseInsentiveIndexByClients
	         threadGroups = clientThreads, ddlThread;	
 
sql.ciIndex.CaseInsensitiveIndexPrms-ddlFilePath = "sql/ciIndex/ciIndexDDL.sql";
sql.ciIndex.CaseInsensitiveIndexPrms-ciIndexDataFiles = 
	"/s2qa/tangc/workspace/ciitest/TABLE_DATA.dat"
	;
	
hydra.gemfirexd.NetworkServerPrms-names = server;
hydra.gemfirexd.GfxdConfigPrms-networkServerConfig = server;
sql.SQLPrms-useGfxdConfig = true;

RANDOMINCLUDE $JTESTS/sql/ciIndex/offHeap.inc; // uses off-heap if include is randomly chosen
