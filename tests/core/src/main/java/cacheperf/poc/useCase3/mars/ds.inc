//------------------------------------------------------------------------------
// Distributed system
//------------------------------------------------------------------------------

hydra.GemFirePrms-archiveDiskSpaceLimit = 400;
hydra.GemFirePrms-archiveFileSizeLimit = 10;
hydra.GemFirePrms-conserveSockets =
  fcn "hydra.TestConfigFcns.duplicate(\"false\", ${tradeHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"false\", ${indicativeHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"false\", ${marketHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"false\", ${positionHosts})" ncf 
  fcn "hydra.TestConfigFcns.duplicate(\"false\", ${riskHosts})" ncf 
  fcn "hydra.TestConfigFcns.duplicate(\"true\", ${clientHosts})" ncf
  ;
hydra.GemFirePrms-distributedSystem =
  fcn "hydra.TestConfigFcns.duplicate(\"trade\", ${tradeHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"indicative\", ${indicativeHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"market\", ${marketHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"position\", ${positionHosts})" ncf 
  fcn "hydra.TestConfigFcns.duplicate(\"results\", ${riskHosts})" ncf 
  fcn "hydra.TestConfigFcns.duplicate(\"loner\", ${clientHosts})" ncf
  ;
hydra.GemFirePrms-enableTimeStatistics =
  fcn "hydra.TestConfigFcns.duplicate(\"true\", ${tradeHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"true\", ${indicativeHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"true\", ${marketHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"true\", ${positionHosts})" ncf 
  fcn "hydra.TestConfigFcns.duplicate(\"true\", ${riskHosts})" ncf 
  fcn "hydra.TestConfigFcns.duplicate(\"false\", ${clientHosts})" ncf
  ;
hydra.GemFirePrms-logDiskSpaceLimit = 400;

