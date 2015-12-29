//------------------------------------------------------------------------------
// GENERIC TOPOLOGY WITH ONE NAMED SUBGROUP
//
//------------------------------------------------------------------------------
// PROPERTIES:
//      A                 name of the group
//      ${A}Hosts         number of logical hosts in group A
//      ${A}VMsPerHost    number of VMs per host in group A
//      ${A}ThreadsPerVM  number of threads per VM in group A
//------------------------------------------------------------------------------
hydra.Prms-testTopology = "
Generic topology with one named group.
Group ${A} has ${${A}Hosts} hosts, ${${A}VMsPerHost} VMs per host, and ${${A}ThreadsPerVM} threads per VM.";
//------------------------------------------------------------------------------
// Configures a host description.
//     Ahost1  Ahost2  ...  Ahost${${A}Hosts}
//
hydra.HostPrms-names = fcn "hydra.TestConfigFcns.generateNames
                            (\"${A}host\", ${${A}Hosts})"
                       ncf;
//------------------------------------------------------------------------------
// Configures a VM description
// and maps it to the host description
// and shows how to give different VM arguments to the group.
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
//hydra.VmPrms-extraVMArgs   = fcn "hydra.TestConfigFcns.duplicate
//                                  (\"AARGS\", ${${A}Hosts}, true)"
//                             ncf;
//------------------------------------------------------------------------------
// Configures a gemfire description
// and maps it to the host description.
//     Agemfire1  Agemfire2  ...  Agemfire${${A}Hosts}
//     Ahost1     Ahost2     ...  Ahost${${A}Hosts}
//
hydra.GemFirePrms-names     = fcn "hydra.TestConfigFcns.generateNames
                                   (\"${A}gemfire\", ${${A}Hosts})"
                              ncf;
hydra.GemFirePrms-hostNames = fcn "hydra.TestConfigFcns.generateNames
                                   (\"${A}host\", ${${A}Hosts})"
                              ncf;
hydra.GemFirePrms-distributedSystem = ds;

//------------------------------------------------------------------------------
// Configures a client description
// and maps it to the VM description
// and maps it to the GemFire description
// and scales the number of VMs and threads per VM.
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
// Example where all threads in the group are in the same threadgroup.
//
//THREADGROUP ${A}
//    totalThreads = fcn
//                   ${${A}Hosts} * ${${A}VMsPerHost} * ${${A}ThreadsPerVM}
//                   ncf
//    clientNames  = fcn "hydra.TestConfigFcns.generateNames
//                        (\"${A}\", ${${A}Hosts}, true)"
//                   ncf;
