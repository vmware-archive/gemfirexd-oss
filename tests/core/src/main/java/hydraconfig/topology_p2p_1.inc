//------------------------------------------------------------------------------
// PEER-TO-PEER TOPOLOGY WITH A NAMED GROUP
//------------------------------------------------------------------------------
// PROPERTIES:
//      A                 name of peer group
//      ${A}Hosts         number of logical peer hosts in group A
//      ${A}VMsPerHost    number of peer VMs per peer host in group A
//      ${A}ThreadsPerVM  number of peer threads per peer VM in group A
//------------------------------------------------------------------------------
hydra.Prms-testTopology = "Peer-to-peer topology with peers in group ${A}.  Group ${A} has ${${A}Hosts} hosts, ${${A}VMsPerHost} VMs per host, and ${${A}ThreadsPerVM} threads per VM.";

//------------------------------------------------------------------------------
// Configures a host description per logical peer host in the group.
//     Ahost1  Ahost2  ...  Ahost${${A}Hosts}
//
hydra.HostPrms-names = fcn "hydra.TestConfigFcns.generateNames
                            (\"${A}host\", ${${A}Hosts})"
                       ncf;

//------------------------------------------------------------------------------
// Configures a VM description per logical peer host in the group
// and maps each one to the appropriate host description
// and shows how to give different VM arguments to each peer subgroup.
//     Avm1    Avm2    ...  Avm${${A}Hosts}
//     Ahost1  Ahost2  ...  Ahost${${A}Hosts}
//     AARGS   AARGS   ...  AARGS // ${${A}Hosts} times
//
hydra.VmPrms-names         = fcn "hydra.TestConfigFcns.generateNames
                                  (\"${A}vm\", ${${A}Hosts})"
                             ncf;
hydra.VmPrms-hostNames     = fcn "hydra.TestConfigFcns.generateNames
                                  (\"${A}host\", ${${A}Hosts})"
                             ncf;
//hydra.VmPrms-extraVMArgs   = AARGS;

//------------------------------------------------------------------------------
// Configures a gemfire description per logical peer host in the group
// and maps each one to the appropriate host description
// and puts them all in the same distributed system.
//     Agemfire1  Agemfire2  ...  Agemfire${${A}Hosts}
//     Ahost1     Ahost2     ...  Ahost${${A}Hosts}
//     ds
//
hydra.GemFirePrms-names     = fcn "hydra.TestConfigFcns.generateNames
                                   (\"${A}gemfire\", ${${A}Hosts})"
                              ncf;
hydra.GemFirePrms-hostNames = fcn "hydra.TestConfigFcns.generateNames
                                   (\"${A}host\", ${${A}Hosts})"
                              ncf;
hydra.GemFirePrms-distributedSystem = ds;

//------------------------------------------------------------------------------
// Configures a client description per logical peer host in the group
// and maps each one to the appropriate VM description
// and maps each one to the appropriate GemFire description
// and scales the number of VMs and threads per VM for each.
//     A1         A2         ...  A${${A}Hosts}
//     Avm1       Avm2       ...  Avm${${A}Hosts}
//     Agemfire1  Agemfire2  ...  Agemfire${${A}Hosts}
//     ${${A}VMsPerHost}     ... ${${A}VMsPerHost} // ${${A}Hosts} times
//     ${${A}ThreadsPerVM}   ... ${${A}ThreadsPerVM} // ${${A}Hosts} times
//
hydra.ClientPrms-names        = fcn "hydra.TestConfigFcns.generateNames
                                     (\"${A}\", ${${A}Hosts})"
                                ncf;
hydra.ClientPrms-vmNames      = fcn "hydra.TestConfigFcns.generateNames
                                     (\"${A}vm\", ${${A}Hosts})"
                                ncf;
hydra.ClientPrms-gemfireNames = fcn "hydra.TestConfigFcns.generateNames
                                     (\"${A}gemfire\", ${${A}Hosts})"
                                ncf;
hydra.ClientPrms-vmQuantities = fcn "hydra.TestConfigFcns.duplicate
                                     (\"${${A}VMsPerHost}\", ${${A}Hosts})"
                                ncf;
hydra.ClientPrms-vmThreads    = fcn "hydra.TestConfigFcns.duplicate
                                     (\"${${A}ThreadsPerVM}\", ${${A}Hosts})"
                                ncf;

//------------------------------------------------------------------------------
// Example threadgroup where all threads in group A are in threadgroup A.
//
//THREADGROUP ${A}
//    totalThreads = fcn
//                   ${${A}Hosts} * ${${A}VMsPerHost} * ${${A}ThreadsPerVM}
//                   ncf
//    clientNames  = fcn "hydra.TestConfigFcns.generateNames
//                        (\"${A}\", ${${A}Hosts}, true)"
//                   ncf;
