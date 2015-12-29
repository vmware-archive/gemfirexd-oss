hydra.Prms-testDescription = "
This test creates a partitioned region with custom parititioning and colocation, then does various entry operations(using function execution)
 on a known key range. Values are checked for correctness in the close task.
";

INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_p2p.inc;

THREADGROUP verifyThreads totalThreads = fcn "${peerVMsPerHost} * ${peerHosts}" ncf
                          totalVMs = fcn "${peerVMsPerHost} * ${peerHosts}" ncf;

STARTTASK    taskClass     = parReg.execute.FunctionServiceTest  taskMethod = StartTask_initialize;

INITTASK     taskClass     = parReg.execute.FunctionServiceTest  taskMethod = HydraTask_initialize
                         threadGroups = verifyThreads
			 runMode = always;

INITTASK     taskClass     = parReg.execute.FunctionServiceTest  taskMethod = HydraTask_loadRegions
             threadGroups = verifyThreads, default
             batch;
             
INITTASK     taskClass     = parReg.execute.FunctionServiceTest  taskMethod  = HydraTask_verifyPRMetaData
             threadGroups = verifyThreads;

INITTASK     taskClass     = parReg.execute.FunctionServiceTest  taskMethod = HydraTask_verifyRegionSize
             threadGroups = verifyThreads
             ;     
             
INITTASK     taskClass     = parReg.execute.FunctionServiceTest  taskMethod = HydraTask_registerFunctions
             threadGroups = verifyThreads, default
             ;               
             
TASK         taskClass     = parReg.execute.FunctionServiceTest  taskMethod = HydraTask_doFunctionExecution
             threadGroups = verifyThreads, default;  
             
CLOSETASK    taskClass     = parReg.execute.FunctionServiceTest  taskMethod  = HydraTask_verifyPrimaries
             threadGroups = verifyThreads;

CLOSETASK    taskClass     = parReg.execute.FunctionServiceTest  taskMethod  = HydraTask_verifyPRMetaData
             threadGroups = verifyThreads; 
             
CLOSETASK    taskClass     = parReg.execute.FunctionServiceTest  taskMethod  = HydraTask_verifyColocatedRegions
             threadGroups = verifyThreads;

CLOSETASK    taskClass     = parReg.execute.FunctionServiceTest  taskMethod  = HydraTask_verifyRegionContents
             threadGroups = verifyThreads
             ;

CLOSETASK    taskClass     = parReg.execute.FunctionServiceTest  taskMethod  = HydraTask_verifyBucketCopies
             threadGroups = verifyThreads
             ;
             
CLOSETASK    taskClass     = parReg.execute.FunctionServiceTest  taskMethod  = HydraTask_executeFunctionAllKeys
             threadGroups = verifyThreads
			 ;                                                        


hydra.RegionPrms-names          = 
      region1 region2 region3 region4 region5 region6 region7;

hydra.RegionPrms-regionName     = 
      clientRegion1 clientRegion2 clientRegion3 clientRegion4 clientRegion5 clientRegion6 clientRegion7;

hydra.RegionPrms-dataPolicy     = partition;
hydra.RegionPrms-partitionName  = pr1 pr2 pr3 pr4 pr5 pr6 pr7;
hydra.PartitionPrms-names       = pr1 pr2 pr3 pr4 pr5 pr6 pr7;


hydra.PartitionPrms-partitionResolver = parReg.colocation.MonthPartitionResolver;

hydra.PartitionPrms-colocatedWith = none clientRegion1 clientRegion2 clientRegion3 clientRegion4 clientRegion5 clientRegion6;

hydra.PartitionPrms-redundantCopies = 	 ${redundantCopies} ;
hydra.PartitionPrms-startupRecoveryDelay = -1;  // rely on rebalance
hydra.PartitionPrms-totalNumBuckets = 12;

hydratest.grid.GridPrms-functions =
          parReg.execute.RegionOperationsFunction
          parReg.execute.KeysOperationsFunction
          parReg.execute.LocalDataSetFunctionPrimaryExecute
          parReg.execute.LocalDataSetFunctionPrimaryExecuteFalse
          ;

hydra.Prms-maxResultWaitSec = 600;

hydra.VmPrms-extraVMArgs = "-Xmx1000m";

hydra.Prms-totalTaskTimeSec = 14400; // test is workload based
hydra.Prms-maxResultWaitSec = 800;

util.TestHelperPrms-minTaskGranularitySec = 60;

getInitialImage.InitImagePrms-numKeys = ${numKeys};
// numNewKeys is 10% of the total number of keys
getInitialImage.InitImagePrms-numNewKeys = fcn "${numKeys} * 0.1" ncf;
getInitialImage.InitImagePrms-useCacheLoader=false;

INCLUDE $JTESTS/util/randomValues.inc;
util.RandomValuesPrms-objectType = byte[];
util.RandomValuesPrms-elementSize = ONEOF 50 100 500 1000 2500 5000 FOENO;
util.ValueHolderPrms-useExtraObject = true;

hydra.GemFirePrms-conserveSockets = ONEOF true false FOENO;

hydra.CachePrms-names           = cache1;
hydra.CachePrms-searchTimeout   = 600;


parReg.ParRegPrms-partitionResolverData = BB;
parReg.ParRegPrms-isWithRoutingResolver = true;
parReg.ParRegPrms-numberOfDataStore = fcn "${peerVMsPerHost} * ${peerHosts}" ncf;

util.CachePrms-useDeclarativeXmlFile = true;

hydra.VmPrms-extraClassPaths = $GEMFIRE/lib/antlr.jar;
