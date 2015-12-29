hydra.StatMonitorPrms-conditions =
  SystemStats allocatedSwap isUnchanged 0 warn
  ,
  SystemStats freeMemory isGreaterThan 10 warn
  ;
hydra.StatMonitorPrms-sampleRateMs = 5000;
