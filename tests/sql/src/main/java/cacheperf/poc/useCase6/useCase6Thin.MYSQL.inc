include $JTESTS/hydraconfig/gemfirexd/topology_2_locator.inc;
include $JTESTS/hydraconfig/gemfirexd/performance.inc;

// GIVE LONER SAME LOGICAL NAME AS CLIENT AS SEEN IN STATSPECS
hydra.gemfirexd.LonerPrms-names = client;
hydra.gemfirexd.LonerPrms-clientNames =
  fcn "hydra.TestConfigFcns.generateNames(\"locator\", ${locatorHosts})" ncf
  fcn "hydra.TestConfigFcns.generateNames(\"client\", ${clientHosts})" ncf
  fcn "hydra.TestConfigFcns.generateNames(\"server\", ${serverHosts})" ncf
  ;

hydra.VmPrms-extraClassPaths =
  fcn "hydra.TestConfigFcns.duplicate
   (\"none\", ${locatorHosts}, true)"
  ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
   (\"/export/gcm/where/java/mysql/mysql-connector-java-5.1.8-bin.jar\", ${clientHosts}, true)"
  ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
   (\"none\", ${serverHosts}, true)"
  ncf
  ;

INITTASK  taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = createLocatorTask
          threadGroups = locator
          ;
