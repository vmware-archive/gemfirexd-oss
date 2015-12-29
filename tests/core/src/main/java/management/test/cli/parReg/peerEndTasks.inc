// End tasks for persistent peer tests; recover from disk and validate recovery 
hydra.Prms-doStartAndEndTasksLockStep = true;

// recover from disk 
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_initialize
            clientNames = fcn "hydra.TestConfigFcns.generateNames(\"peer\", (${peerHosts} * ${peerVMsPerHost}), true)" ncf
            parReg.ParRegPrms-recoverFromDisk = true;

// validate region contents while redundancy recovery is running,
// wait for redundancy recovery and verify PR internals 
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_validateRegionContents
            clientNames = fcn "hydra.TestConfigFcns.generateNames(\"peer\", (${peerHosts} * ${peerVMsPerHost}), true)" ncf;
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_waitForMyStartupRecovery
            clientNames = fcn "hydra.TestConfigFcns.generateNames(\"peer\", (${peerHosts} * ${peerVMsPerHost}), true)" ncf;
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_validateInternalPRState
            clientNames = fcn "hydra.TestConfigFcns.generateNames(\"peer\", (${peerHosts} * ${peerVMsPerHost}), true)" ncf;

// offline validation and compaction
ENDTASK     taskClass   = parReg.ParRegTest     taskMethod = HydraTask_disconnect
            clientNames = fcn "hydra.TestConfigFcns.generateNames(\"peer\", (${peerHosts} * ${peerVMsPerHost}), true)" ncf;

// reinit the cli in all peers; the offlineValAndCompactionOnce method will randomly choose one peer to do offline
// validation and compaction and it will execute from that randomly chosen peer because executeRemote is false
ENDTASK taskClass    = management.test.cli.CommandTest taskMethod = HydraTask_logBBs
        clientNames = fcn "hydra.TestConfigFcns.generateNames(\"peer\", (${peerHosts} * ${peerVMsPerHost}), true)" ncf;
ENDTASK taskClass    = management.test.cli.CommandTest taskMethod = HydraTask_initAndConnectCLI
        clientNames = fcn "hydra.TestConfigFcns.generateNames(\"peer\", (${peerHosts} * ${peerVMsPerHost}), true)" ncf;
ENDTASK     taskClass   = util.PersistenceUtil  taskMethod = HydraTask_doOfflineValAndCompactionOnce
            util.CliHelperPrms-executeRemote=false
            clientNames = fcn "hydra.TestConfigFcns.generateNames(\"peer\", (${peerHosts} * ${peerVMsPerHost}), true)" ncf;

// recover from disk
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_prepareForRecovery
            clientNames = fcn "hydra.TestConfigFcns.generateNames(\"peer\", (${peerHosts} * ${peerVMsPerHost}), true)" ncf;
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_initialize
            clientNames = fcn "hydra.TestConfigFcns.generateNames(\"peer\", (${peerHosts} * ${peerVMsPerHost}), true)" ncf
            parReg.ParRegPrms-recoverFromDisk = true;

// validate after compaction; validate region contents while redundancy recovery is running,
// wait for redundancy recovery and verify PR internals 
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_validateRegionContents
            clientNames = fcn "hydra.TestConfigFcns.generateNames(\"peer\", (${peerHosts} * ${peerVMsPerHost}), true)" ncf;
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_waitForMyStartupRecovery
            clientNames = fcn "hydra.TestConfigFcns.generateNames(\"peer\", (${peerHosts} * ${peerVMsPerHost}), true)" ncf;
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_validateInternalPRState
            clientNames = fcn "hydra.TestConfigFcns.generateNames(\"peer\", (${peerHosts} * ${peerVMsPerHost}), true)" ncf;


