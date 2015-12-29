hydra.Prms-testRequirement = "Test that pdx is compatibile for minor Gemfire version numbers by performing a rolling upgrade";
hydra.Prms-testDescription = "
   Start a DS with old version members.
   Populate the regions.
   
   // rolling upgrade: replace old version jvms with new version jvms 
   Repeat
      Stop N old jvms 
      Start new version jvms to replace old version jvms
      Run ops for a while to make sure the DS can handle the mix of old and new jvms
      If this is a persistent test
         Stop all the jvms and do a disk recovery while we have a mix of old and new jvms
      Run ops for a while
   Until all old version jvms are replaced with new version jvms 
";

INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_3.inc;

// define thread groups
// just need one thread from the controller vm
THREADGROUP controllerThread
    totalThreads = 1
    totalVMs = 1;

// oldVersion threads; 1 vm is reserved as a "proxy" vm where regions are empty or accessors
THREADGROUP oldVersionThreads
    totalThreads = fcn (${${B}Hosts} * ${${B}VMsPerHost} * ${${B}ThreadsPerVM}) - ${${B}ThreadsPerVM} ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames (\"${B}\", ${${B}Hosts}, true)" ncf;
THREADGROUP oldVersionProxyVMThreads
    totalThreads = fcn ${${B}ThreadsPerVM} ncf
    totalVMs = 1
    clientNames  = oldVersion1;

// newVersion threads; 1 vm is reserved as a "proxy" vm where regions are empty or accessors
THREADGROUP newVersionThreads
    totalThreads = fcn (${${C}Hosts} * ${${C}VMsPerHost} * ${${C}ThreadsPerVM}) ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames (\"${C}\", ${${C}Hosts}, true)" ncf;

// controller jvm uses new the new version
hydra.ClientPrms-versionNames = fcn "hydra.TestConfigFcns.duplicate
                                     (\"newVersion\",    ${controllerHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"oldVersion\",    ${oldVersionHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"newVersion\",    ${newVersionHosts})"
                                ncf;
hydra.VersionPrms-names   = oldVersion     newVersion;
hydra.VersionPrms-version = ${oldVersion}  ${newVersion};
hydra.VersionPrms-gemfireHome =
  fcn "hydra.VersionDescription.getGemFireHome(\"${oldVersion}\")" ncf
  fcn "hydra.VersionDescription.getGemFireHome(\"${newVersionHome}\")" ncf
  ;

INITTASK    taskClass     = util.StopStartVMs  taskMethod = StopStart_initTask;

INITTASK    taskClass     = pdx.compat.PdxCompatTest taskMethod = HydraTask_initController
            threadGroups = controllerThread;

INITTASK    taskClass     = pdx.compat.PdxCompatTest taskMethod = HydraTask_initialize
            pdx.PdxPrms-createProxyRegions=false
            threadGroups = oldVersionThreads;

INITTASK    taskClass     = pdx.compat.PdxCompatTest taskMethod = HydraTask_initialize
            pdx.PdxPrms-createProxyRegions=true
            threadGroups = oldVersionProxyVMThreads;

INITTASK    taskClass     = pdx.compat.PdxCompatTest taskMethod = HydraTask_createXmlFile
            threadGroups = oldVersionThreads, oldVersionProxyVMThreads;

INITTASK    taskClass     = pdx.compat.PdxCompatTest taskMethod = HydraTask_loadRegions
            threadGroups = oldVersionThreads,oldVersionProxyVMThreads;

INITTASK    taskClass = util.AdminHelper  taskMethod = HydraTask_initializeAdminDS
            threadGroups = controllerThread;

// dynamic init tasks for any restarted jvms; new version jvms initialize from an old version's xml file
INITTASK    taskClass = pdx.compat.PdxCompatTest taskMethod = HydraTask_initializeWithXml
            threadGroups = newVersionThreads, oldVersionThreads, oldVersionProxyVMThreads
            runMode = dynamic;

TASK        taskClass     = pdx.compat.Operations taskMethod = HydraTask_doOperations
            threadGroups = oldVersionThreads,oldVersionProxyVMThreads,newVersionThreads;

TASK        taskClass     = pdx.compat.PdxCompatTest taskMethod = HydraTask_compatController
            threadGroups = controllerThread;

hydra.Prms-totalTaskTimeSec = 7200; // test is workload based

INCLUDE $JTESTS/pdx/randomValuesPdx.inc;
util.ValueHolderPrms-useExtraObject = true;

hydra.GemFirePrms-conserveSockets = ONEOF true false FOENO;

hydra.CachePrms-names           = cache1;
hydra.CachePrms-searchTimeout   = 600;
hydra.Prms-maxResultWaitSec = 900;

pdx.PdxPrms-numToLoadPerRegion = 500;

util.TestHelperPrms-minTaskGranularitySec = 10; // length of ops per region

util.OperationsClientPrms-entryOperations = ONEOF add add getNew getNew update update update invalidate get destroy replace remove FOENO;
util.OperationsClientPrms-upperThreshold = 550;
util.OperationsClientPrms-upperThresholdOperations = ONEOF destroy remove FOENO;
util.OperationsClientPrms-lowerThreshold = 400;
util.OperationsClientPrms-lowerThresholdOperations = add ;
util.OperationsClientPrms-objectTypes = util.PdxVersionedValueHolder util.VersionedValueHolder util.ValueHolder util.VHDataSerializable;

hydra.CachePrms-pdxSerializerInstantiator = pdx.PdxTestVersionHelper instantiatePdxSerializer;
pdx.PdxPrms-pdxSerializerClassName = util.PdxTestSerializer;

hydra.Prms-useFixedRandomInMaster= true; // lock down value of pdxReadSerialized
hydra.CachePrms-pdxReadSerialized = ${pdxReadSerialized};

pdx.PdxPrms-initClassLoader = true;
pdx.PdxPrms-doEnumValidation = true; 
 
