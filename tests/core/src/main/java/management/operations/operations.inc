

//--- Region Operations Prms  ----------------------------------------
//util.RegionDefPrms-regionSpecs = "
//   specName = OperationsRegion:
//      regionName = OperationsRegion:
//       scope = ack, noack, global: dataPolicy = replicate: 
//       entryTTLSec = 0 20: 
//       entryTTLAction = destroy invalidate:
//      entryIdleTimeoutSec = 0 20: 
//       entryIdleTimeoutAction = destroy invalidate:
//       statisticsEnabled = true:
//  ";
   
//management.operations.OperationPrms-regionList = OperationsRegion;

   

management.operations.OperationPrms-regionCacheListner= management.operations.OperationsCacheListener;

management.operations.OperationPrms-recordRegionOperations = true;

management.operations.OperationPrms-regionOperations  = ONEOF add destroy invalidate clear FOENO;

//--- Entry Operations Prms  ----------------------------------------

management.operations.OperationPrms-recordEntryOperations = true;

management.operations.OperationPrms-entryOperations  = ONEOF add update invalidate destroy read localInvalidate localDestroy putIfAbsent remove replace FOENO;

management.operations.OperationPrms-lowerThreshold = 0;

management.operations.OperationPrms-lowerThresholdOperations = add;

management.operations.OperationPrms-upperThreshold = 5000;

management.operations.OperationPrms-upperThresholdOperations = ONEOF destroy localDestroy FOENO;

//--- QueryAndEntry Operations Prms  ----------------------------------------

// More weightage to query
management.operations.OperationPrms-entryANDQueryOperations = ONEOF add update invalidate destroy read localInvalidate localDestroy putIfAbsent remove replace query query query query FOENO;

management.operations.OperationPrms-recordQueryOperations = true;

management.operations.OperationPrms-regionLoadSize = 1000;

management.operations.OperationPrms-objectType = objects.Portfolio;

//--- Tx Operations Prms  ----------------------------------------

management.operations.OperationPrms-useTransactions = true;

management.operations.OperationPrms-txCommitPercentage = 75;

management.operations.OperationPrms-recordTxOperations = true;

//--- Function Operations Prms  ----------------------------------------

management.operations.OperationPrms-recordFunctionOps = true;

management.operations.OperationPrms-functionOps = ONEOF register unregister exec exec exec exec exec FOENO;

management.operations.OperationPrms-functionRegisterList = sleepFunction sleepFunction sleepFunction sleepFunction sleepFunction sleepFunction sleepFunction;

management.operations.OperationPrms-functionList = ONEOF sleepFunction sleepFunction sleepFunction sleepFunction FOENO;

management.operations.OperationPrms-functionSleepDelay = 30000;

//--- CQ and Index Operations Prms  ----------------------------------------

management.operations.OperationPrms-recordCqIndexOps = true;

management.operations.OperationPrms-cqIndexOps = ONEOF createIndex removeIndex createCq stopCq FOENO;

//--- DLock Operations Prms  ----------------------------------------

management.operations.OperationPrms-recordDlockOperations = true;
management.operations.OperationPrms-dlockDelay = 10000;
management.operations.OperationPrms-dlockLeaseTimeMillis = 10000;
management.operations.OperationPrms-dlockWaitTimeMillis = 100000;
