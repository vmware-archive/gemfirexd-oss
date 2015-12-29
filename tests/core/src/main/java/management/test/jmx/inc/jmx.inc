management.jmx.JMXPrms-sleepTimeFactor = 3;
hydra.VmPrms-extraClassPaths = $GEMFIRE/lib/antlr.jar $GEMFIRE/lib/gemfire.jar $GEMFIRE/lib/gfsh-dependencies.jar;

hydra.VmPrms-extraVMArgs = "-Dgemfire.enableManagement=true";

STARTTASK
  taskClass = management.operations.ops.JMXOperations 
  taskMethod = HydraStartTask_ReadMbeanDescriptors
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
                          
THREADGROUP jmxThreads 
			totalThreads = fcn 
								${managed_1_Hosts} + ${managed_2_Hosts}
						   ncf 
			totalVms =     fcn 
								${managed_1_Hosts} + ${managed_2_Hosts}
						   ncf  
			clientNames =  fcn "hydra.TestConfigFcns.generateNames
                               (\"managed_1\", ${managed_1_Hosts}, true)"
                           ncf
                           ,
                           fcn "hydra.TestConfigFcns.generateNames
                               (\"managed_2\", ${managed_2_Hosts}, true)"
                           ncf                                                    
                          ;                          

THREADGROUP locatorThreads 
			totalThreads = fcn 
								(${locator_managing_Hosts})    
						   ncf 
			totalVms =     fcn 
								(${locator_managing_Hosts})   
						   ncf  
			clientNames =  fcn "hydra.TestConfigFcns.generateNames
                               (\"locator_managing\", ${locator_managing_Hosts}, true)"
                           ncf
                           ;
                           
INITTASK
  taskClass = management.test.jmx.JMXTest
  taskMethod = HydraInitTask_startLocator
  runMode = always
  threadGroups = locatorThreads;
  
INITTASK
  taskClass = management.test.jmx.JMXTest
  taskMethod = HydraInitTask_initialize
  runMode = always
  threadGroups = default, jmxThreads;
  
INITTASK
  taskClass = management.test.jmx.JMXTest
  taskMethod = HydraInitTask_becomeManager
  threadGroups = managingThreads
  runMode = always;
  
INITTASK
  taskClass = management.test.jmx.JMXTest
  taskMethod = HydraInitTask_setUpJMXListeners
  runMode = always
  threadGroups = managingThreads, jmxThreads;    
  
TASK
  taskClass = management.test.jmx.JMXTest
  taskMethod = HydraTask_jmxOperations
  maxTimesToRun = ${jmxIterations}
  threadGroups = jmxThreads
  ;  
  
CLOSETASK
  taskClass = management.test.jmx.JMXTest
  taskMethod = HydraCloseTask_printEvents 
  threadGroups = jmxThreads 
  ;   

CLOSETASK
  taskClass = management.test.jmx.JMXTest
  taskMethod = HydraCloseTask_validateEvents 
  threadGroups = jmxThreads 
  ;
  
CLOSETASK
  taskClass = management.test.jmx.JMXTest
  taskMethod = HydraCloseTask_validateNotifications 
  threadGroups = jmxThreads 
  ;  