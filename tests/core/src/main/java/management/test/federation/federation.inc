hydra.VmPrms-extraVMArgs = "-Dgemfire.enableManagement=true";
management.jmx.JMXPrms-sleepTimeFactor = 3;

THREADGROUP locatorThreads 
			totalThreads = fcn 
								(${locator_managing_Hosts}*${locator_managing_VMsPerHost})    
						   ncf 
			totalVms =     fcn 
								(${locator_managing_Hosts}*${locator_managing_VMsPerHost})   
						   ncf  
			clientNames =  fcn "hydra.TestConfigFcns.generateNames
                               (\"locator_managing\", ${locator_managing_Hosts}, true)"
                           ncf
                           ;
                           
THREADGROUP managingThreads 
			totalThreads = fcn 
								(${managing_1_Hosts}*${managing_1_VMsPerHost}) + (${locator_managing_Hosts}*${locator_managing_VMsPerHost})   
						   ncf 
			totalVms =     fcn 
								(${managing_1_Hosts}*${managing_1_VMsPerHost}) + (${locator_managing_Hosts}*${locator_managing_VMsPerHost})  
						   ncf  
			clientNames =  fcn "hydra.TestConfigFcns.generateNames
                               (\"managing_1\", ${managing_1_Hosts}, true)"
                           ncf
                           ,
                           fcn "hydra.TestConfigFcns.generateNames
                               (\"locator_managing\", ${locator_managing_Hosts}, true)"
                           ncf
                           ;
                                                     
INITTASK 
	taskClass   = util.StopStartVMs  
	taskMethod = StopStart_initTask;
	
INITTASK
  taskClass = management.jmx.JMXBlackboard
  taskMethod = JMXBlackboard_Init;
  
INITTASK
  taskClass = management.test.jmx.JMXTest
  taskMethod = HydraInitTask_startLocator
  runMode = always
  threadGroups = locatorThreads;  
  
INITTASK
  taskClass = management.test.federation.FederationTest
  taskMethod = HydraInitTask_initialize
  runMode = always;

INITTASK
  taskClass = management.test.federation.FederationTest
  taskMethod = HydraInitTask_InitFedBB
  runMode = always;  
  
INITTASK
  taskClass = management.test.federation.FederationTest
  taskMethod = HydraInitTask_becomeManager
  threadGroups = managingThreads
  runMode = always;
  
INITTASK
  taskClass = management.test.federation.FederationTest
  taskMethod = HydraInitTask_RegisterMBeans
  threadGroups = default
  runMode = always;   
  
