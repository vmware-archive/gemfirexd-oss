//------------------------------------------------------------------------------
// PEER-TO-PEER TOPOLOGY DIVIDED INTO TWO NAMED SUBGROUPS
//------------------------------------------------------------------------------
// PROPERTIES:
//      A                 name of one peer subgroup
//      ${A}Hosts         number of logical peer hosts in group A
//      ${A}VMsPerHost    number of peer VMs per peer host in group A
//      ${A}ThreadsPerVM  number of peer threads per peer VM in group A
//      B                 name of the other peer subgroup
//      ${B}Hosts         number of logical peer hosts in group B
//      ${B}VMsPerHost    number of peer VMs per peer host in group B
//      ${B}ThreadsPerVM  number of peer threads per peer VM in group B
//------------------------------------------------------------------------------
hydra.Prms-testTopology = "Peer-to-peer topology logically divided into two named subgroups, ${A} and ${B}.  Subgroup ${A} has ${${A}Hosts} hosts, ${${A}VMsPerHost} VMs per host, and ${${A}ThreadsPerVM} threads per VM.  Subgroup ${B} has ${${B}Hosts} hosts, ${${B}VMsPerHost} VMs per host, and ${${B}ThreadsPerVM} threads per VM.";

//------------------------------------------------------------------------------
// Configures a host description per logical peer host in each subgroup.
//     Ahost1  Ahost2  ...  Ahost${${A}Hosts}
//     Bhost1  Bhost2  ...  Bhost${${B}Hosts}
//
hydra.HostPrms-names = fcn "hydra.TestConfigFcns.generateNames
                            (\"${A}host\", ${${A}Hosts})"
                       ncf
                       fcn "hydra.TestConfigFcns.generateNames
                            (\"${B}host\", ${${B}Hosts})"
                       ncf;

//------------------------------------------------------------------------------
// Configures a VM description per logical peer host per subgroup
// and maps each one to the appropriate host description
// and shows how to give different VM arguments to each peer subgroup.
//     Avm1    Avm2    ...  Avm${${A}Hosts}
//     Bvm1    Bvm2    ...  Bvm${${B}Hosts}
//     Ahost1  Ahost2  ...  Ahost${${A}Hosts}
//     Bhost1  Bhost2  ...  Bhost${${B}Hosts}
//     AARGS   AARGS   ...  AARGS // ${${A}Hosts} times
//     BARGS   BARGS   ...  BARGS // ${${B}Hosts} times
//
hydra.VmPrms-names         = fcn "hydra.TestConfigFcns.generateNames
                                  (\"${A}vm\", ${${A}Hosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"${B}vm\", ${${B}Hosts})"
                             ncf;
hydra.VmPrms-hostNames     = fcn "hydra.TestConfigFcns.generateNames
                                  (\"${A}host\", ${${A}Hosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"${B}host\", ${${B}Hosts})"
                             ncf;
//hydra.VmPrms-extraVMArgs   = fcn "hydra.TestConfigFcns.duplicate
//                                  (\"AARGS\", ${${A}Hosts}, true)"
//                             ncf
//                             ,
//                             fcn "hydra.TestConfigFcns.duplicate
//                                  (\"BARGS\", ${${B}Hosts}, true)"
//                             ncf;

//------------------------------------------------------------------------------
// Configures a gemfire description per logical peer host per subgroup
// and maps each one to the appropriate host description
// and puts them all in the same distributed system.
//     Agemfire1  Agemfire2  ...  Agemfire${${A}Hosts}
//     Bgemfire1  Bgemfire2  ...  Bgemfire${${B}Hosts}
//     Ahost1     Ahost2     ...  Ahost${${A}Hosts}
//     Bhost1     Bhost2     ...  Bhost${${B}Hosts}
//     ds
//
hydra.GemFirePrms-names     = fcn "hydra.TestConfigFcns.generateNames
                                   (\"${A}gemfire\", ${${A}Hosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.generateNames
                                   (\"${B}gemfire\", ${${B}Hosts})"
                              ncf;
hydra.GemFirePrms-hostNames = fcn "hydra.TestConfigFcns.generateNames
                                   (\"${A}host\", ${${A}Hosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.generateNames
                                   (\"${B}host\", ${${B}Hosts})"
                              ncf;
hydra.GemFirePrms-distributedSystem = ds;

//------------------------------------------------------------------------------
// Configures a client description per logical peer host per subgroup
// and maps each one to the appropriate VM description
// and maps each one to the appropriate GemFire description
// and scales the number of VMs and threads per VM for each.
//     A1         A2         ...  A${${A}Hosts}
//     B1         B2         ...  B${${B}Hosts}
//     Avm1       Avm2       ...  Avm${${A}Hosts}
//     Bvm1       Bvm2       ...  Bvm${${B}Hosts}
//     Agemfire1  Agemfire2  ...  Agemfire${${A}Hosts}
//     Bgemfire1  Bgemfire2  ...  Bgemfire${${B}Hosts}
//     ${${A}VMsPerHost}     ... ${${A}VMsPerHost} // ${${A}Hosts} times
//     ${${B}VMsPerHost}     ... ${${B}VMsPerHost} // ${${B}Hosts} times
//     ${${A}ThreadsPerVM}   ... ${${A}ThreadsPerVM} // ${${A}Hosts} times
//     ${${B}ThreadsPerVM}   ... ${${B}ThreadsPerVM} // ${${B}Hosts} times
//
hydra.ClientPrms-names        = fcn "hydra.TestConfigFcns.generateNames
                                     (\"${A}\", ${${A}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${B}\", ${${B}Hosts})"
                                ncf;
hydra.ClientPrms-vmNames      = fcn "hydra.TestConfigFcns.generateNames
                                     (\"${A}vm\", ${${A}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${B}vm\", ${${B}Hosts})"
                                ncf;
hydra.ClientPrms-gemfireNames = fcn "hydra.TestConfigFcns.generateNames
                                     (\"${A}gemfire\", ${${A}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${B}gemfire\", ${${B}Hosts})"
                                ncf;
hydra.ClientPrms-vmQuantities = fcn "hydra.TestConfigFcns.duplicate
                                     (\"${${A}VMsPerHost}\", ${${A}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"${${B}VMsPerHost}\", ${${B}Hosts})"
                                ncf;
hydra.ClientPrms-vmThreads    = fcn "hydra.TestConfigFcns.duplicate
                                     (\"${${A}ThreadsPerVM}\", ${${A}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"${${B}ThreadsPerVM}\", ${${B}Hosts})"
                                ncf;

//------------------------------------------------------------------------------
// Example threadgroups where all threads in subgroup A are in threadgroup A
// and all threads in subgroup B are in threadgroup B.
//
//THREADGROUP ${A}
//    totalThreads = fcn
//                   ${${A}Hosts} * ${${A}VMsPerHost} * ${${A}ThreadsPerVM}
//                   ncf
//    clientNames  = fcn "hydra.TestConfigFcns.generateNames
//                        (\"${A}\", ${${A}Hosts}, true)"
//                   ncf;
//THREADGROUP ${B}
//    totalThreads = fcn
//                   ${${B}Hosts} * ${${B}VMsPerHost} * ${${B}ThreadsPerVM}
//                   ncf
//    clientNames  = fcn "hydra.TestConfigFcns.generateNames
//                        (\"${B}\", ${${B}Hosts}, true)"
//                   ncf;
