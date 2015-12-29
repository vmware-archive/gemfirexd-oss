hydra.VmPrms-extraClassPaths += $GEMFIRE/lib/gfsh-dependencies.jar;
hydra.VmPrms-extraClassPaths += $GEMFIRE/lib/antlr.jar;
hydra.VmPrms-extraClassPaths += $JAVA_HOME/../lib/tools.jar;

//hydra.VmPrms-extraVMArgs = "-Dgemfire.enableManagement=true";

hydra.VmPrms-extraVMArgs =   fcn "hydra.TestConfigFcns.duplicateString
                                  (\"-Dgemfire.groups=gfsh\", ${gfsh_Hosts}, true)"
                             ncf
                             ,
                             fcn "hydra.TestConfigFcns.duplicateString
                                  (\"-Dgemfire.enableManagement=true -Dgemfire.groups=managed,managed1\", ${managed_1_Hosts}, true)"
                             ncf
                             ,
                             fcn "hydra.TestConfigFcns.duplicateString
                                  (\"-Dgemfire.enableManagement=true -Dgemfire.groups=managed,managed2\", ${managed_2_Hosts}, true)"
                             ncf
                             ,
                             fcn "hydra.TestConfigFcns.duplicateString
                                  (\"-Dgemfire.enableManagement=true -Dgemfire.groups=managing,locator_managing\", ${locator_managing_Hosts}, true)"
                             ncf
                             ,
                             fcn "hydra.TestConfigFcns.duplicateString
                                  (\"-Dgemfire.enableManagement=true -Dgemfire.groups=managing,managing1\", ${managing_1_Hosts}, true)"
                             ncf;

 

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
  
THREADGROUP managingThreads 
			totalThreads = fcn 
								(${managing_1_Hosts})  + (${locator_managing_Hosts})
						   ncf 
			totalVms =     fcn 
								${managing_1_Hosts}   + (${locator_managing_Hosts})
						   ncf  
			clientNames =  fcn "hydra.TestConfigFcns.generateNames
                               (\"managing_1\", ${managing_1_Hosts}, true)"
                           ncf
                           ,
                           fcn "hydra.TestConfigFcns.generateNames
                               (\"locator_managing\", ${locator_managing_Hosts}, true)"
                           ncf
                           ;
                          
THREADGROUP cliThreads 
			totalThreads = fcn 
								${gfsh_Hosts}*${gfsh_VMsPerHost}*${gfsh_ThreadsPerVM} 
						   ncf 
			totalVms =     fcn 
								${gfsh_Hosts}*${gfsh_VMsPerHost}
						   ncf  
			clientNames =  fcn "hydra.TestConfigFcns.generateNames
                               (\"gfsh\", ${gfsh_Hosts}, true)"
                           ncf
                           ;

STARTTASK
  taskClass = management.operations.ops.CLIOperations 
  taskMethod = HydraStartTask_ReadCommandSpecs
  ;

INITTASK
  taskClass = management.test.cli.CLITest
  taskMethod = HydraInitTask_startLocator
  runMode = always
  threadGroups = locatorThreads; 
  
                             
INITTASK
  taskClass = management.test.cli.CLITest
  taskMethod = HydraInitTask_initialize
  runMode = always
  threadGroups = default, cliThreads;
  
                           
INITTASK
  taskClass = management.test.cli.CLITest
  taskMethod = HydraInitTask_becomeManager
  threadGroups = managingThreads
  runMode = always;
  
CLOSETASK
  taskClass = management.test.cli.CLITest
  taskMethod = HydraCloseTask_printFailedCommands 
  threadGroups = cliThreads 
  ;  
  
management.jmx.JMXPrms-sleepTimeFactor = 5;                                  