// General include file for all "validate" tests

hydra.log.LogPrms-mergeLogFiles = onFailure;

regions.validate.ValidatePrms-entryOperationsDuration = 300;

INITTASK
  taskClass = regions.validate.ValidateTasks
  taskMethod = initializeVM
  threadGroups = default
  ;

INITTASK
  taskClass = regions.validate.ValidateTasks
  taskMethod = populateRegion
  threadGroups = default
  ;

TASK
  taskClass = regions.validate.ValidateTasks
  taskMethod = doEntryOperations
  threadGroups = default
  ;

CLOSETASK
  taskClass = regions.validate.ValidateTasks
  taskMethod = validateRegion
  threadGroups = default
  ;

ENDTASK
  taskClass = regions.validate.ValidateTasks
  taskMethod = ensureNotInUse
  clientNames = client
  ;
