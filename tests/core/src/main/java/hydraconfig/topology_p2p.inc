//------------------------------------------------------------------------------
// PEER-TO-PEER TOPOLOGY WITH A GROUP NAMED PEER
//------------------------------------------------------------------------------
// PROPERTIES:
//      peerHosts         number of logical peer hosts
//      peerVMsPerHost    number of peer VMs per logical peer host
//      peerThreadsPerVM  number of peer threads per peer VM
//------------------------------------------------------------------------------
hydra.Prms-testTopology = "Peer-to-peer topology with ${peerHosts} hosts, ${peerVMsPerHost} VMs per host, and ${peerThreadsPerVM} threads per VM.";

//------------------------------------------------------------------------------
// Configures a host description per logical peer host.
//     host1  host2  ...  host${peerHosts}
//
hydra.HostPrms-names = fcn "hydra.TestConfigFcns.generateNames
                            (\"host\", ${peerHosts})"
                       ncf;

//------------------------------------------------------------------------------
// Configures a VM description per logical peer host
// and maps each one to the appropriate host description
// and shows how to give them all the same arguments.
//     vm1    vm2    ...  vm${peerHosts}
//     host1  host2  ...  host${peerHosts}
//     ARGS
//
hydra.VmPrms-names         = fcn "hydra.TestConfigFcns.generateNames
                                  (\"vm\", ${peerHosts})"
                             ncf;
hydra.VmPrms-hostNames     = fcn "hydra.TestConfigFcns.generateNames
                                  (\"host\", ${peerHosts})"
                             ncf;
//hydra.VmPrms-extraVMArgs = ARGS;

//------------------------------------------------------------------------------
// Configures a gemfire description per logical peer host
// and maps each one to the appropriate host description
// and puts them all in the same distributed system.
//     gemfire1  gemfire2  ...  gemfire${peerHosts}
//     host1     host2     ...  host${peerHosts}
//     ds
//
hydra.GemFirePrms-names     = fcn "hydra.TestConfigFcns.generateNames
                                   (\"gemfire\", ${peerHosts})"
                              ncf;
hydra.GemFirePrms-hostNames = fcn "hydra.TestConfigFcns.generateNames
                                   (\"host\", ${peerHosts})"
                              ncf;
hydra.GemFirePrms-distributedSystem = ds;

//------------------------------------------------------------------------------
// Configures a client description per logical peer host
// and maps each one to the appropriate VM description
// and maps each one to the appropriate GemFire description
// and scales the number of VMs and threads per VM for each.
//     client1   client2   ...  client${peerHosts}
//     vm1       vm2       ...  vm${peerHosts}
//     gemfire1  gemfire2  ...  gemfire${peerHosts}
//     ${peerVMsPerHost}
//     ${peerThreadsPerVM}
//
hydra.ClientPrms-names        = fcn "hydra.TestConfigFcns.generateNames
                                     (\"client\", ${peerHosts})"
                                ncf;
hydra.ClientPrms-vmNames      = fcn "hydra.TestConfigFcns.generateNames
                                     (\"vm\", ${peerHosts})"
                                ncf;
hydra.ClientPrms-gemfireNames = fcn "hydra.TestConfigFcns.generateNames
                                     (\"gemfire\", ${peerHosts})"
                                ncf;
hydra.ClientPrms-vmQuantities = ${peerVMsPerHost};
hydra.ClientPrms-vmThreads    = ${peerThreadsPerVM};
//------------------------------------------------------------------------------
