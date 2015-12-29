hydra.Prms-testDescription += "Uses thin clients.";

hydra.VmPrms-extraVMArgs += -XX:+DisableExplicitGC;

hydra.gemfirexd.LonerPrms-names = client server;
hydra.gemfirexd.LonerPrms-clientNames =
  fcn "hydra.TestConfigFcns.generateNames(\"client\", ${clientHosts})" ncf
  ,
  fcn "hydra.TestConfigFcns.generateNames(\"server\", ${serverHosts})" ncf
  ;
hydra.gemfirexd.LonerPrms-enableTimeStatistics = false;

gfxdperf.tpch.oracle.OraclePrms-txIsolation = read_committed;
