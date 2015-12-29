//------------------------------------------------------------------------------
// GENERIC TOPOLOGY DIVIDED INTO THREE NAMED SUBGROUPS
//
// It is up to users of this file to map the groups to distributed systems.
//
//------------------------------------------------------------------------------
// PROPERTIES:
//      A                 name of the first subgroup
//      ${A}Hosts         number of logical hosts in group A
//      ${A}VMsPerHost    number of VMs per host in group A
//      ${A}ThreadsPerVM  number of threads per VM in group A
//      B                 name of the second subgroup
//      ${B}Hosts         number of logical hosts in group B
//      ${B}VMsPerHost    number of VMs per host in group B
//      ${B}ThreadsPerVM  number of threads per VM in group B
//      C                 name of the third subgroup
//      ${C}Hosts         number of logical hosts in group C
//      ${C}VMsPerHost    number of VMs per host in group C
//      ${C}ThreadsPerVM  number of threads per VM in group C
//------------------------------------------------------------------------------
hydra.Prms-testTopology = "
Generic topology logically divided into three named subgroups.
Subgroup ${A} has ${${A}Hosts} hosts, ${${A}VMsPerHost} VMs per host, and ${${A}ThreadsPerVM} threads per VM.
Subgroup ${B} has ${${B}Hosts} hosts, ${${B}VMsPerHost} VMs per host, and ${${B}ThreadsPerVM} threads per VM.
Subgroup ${C} has ${${C}Hosts} hosts, ${${C}VMsPerHost} VMs per host, and ${${C}ThreadsPerVM} threads per VM.";
//------------------------------------------------------------------------------
// Configures a host description per logical host in each subgroup.
//     Ahost1  Ahost2  ...  Ahost${${A}Hosts}
//     Bhost1  Bhost2  ...  Bhost${${B}Hosts}
//     Chost1  Chost2  ...  Chost${${C}Hosts}
//
hydra.HostPrms-names = fcn "hydra.TestConfigFcns.generateNames
                            (\"${A}host\", ${${A}Hosts})"
                       ncf
                       fcn "hydra.TestConfigFcns.generateNames
                            (\"${B}host\", ${${B}Hosts})"
                       ncf
                       fcn "hydra.TestConfigFcns.generateNames
                            (\"${C}host\", ${${C}Hosts})"
                       ncf;
//------------------------------------------------------------------------------
// Configures a VM description per logical host per subgroup
// and maps each one to the appropriate host description
// and shows how to give different VM arguments to each subgroup.
//     Avm1    Avm2    ...  Avm${${A}Hosts}
//     Bvm1    Bvm2    ...  Bvm${${B}Hosts}
//     Cvm1    Cvm2    ...  Cvm${${C}Hosts}
//     Ahost1  Ahost2  ...  Ahost${${A}Hosts}
//     Bhost1  Bhost2  ...  Bhost${${B}Hosts}
//     Chost1  Chost2  ...  Chost${${C}Hosts}
//     AARGS   AARGS   ...  AARGS // ${${A}Hosts} times
//     BARGS   BARGS   ...  BARGS // ${${B}Hosts} times
//     CARGS   CARGS   ...  CARGS // ${${C}Hosts} times
//
hydra.VmPrms-names         = fcn "hydra.TestConfigFcns.generateNames
                                  (\"${A}vm\", ${${A}Hosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"${B}vm\", ${${B}Hosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"${C}vm\", ${${C}Hosts})"
                             ncf;
hydra.VmPrms-hostNames     = fcn "hydra.TestConfigFcns.generateNames
                                  (\"${A}host\", ${${A}Hosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"${B}host\", ${${B}Hosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"${C}host\", ${${C}Hosts})"
                             ncf;
//hydra.VmPrms-extraVMArgs   = fcn "hydra.TestConfigFcns.duplicate
//                                  (\"AARGS\", ${${A}Hosts}, true)"
//                             ncf
//                             ,
//                             fcn "hydra.TestConfigFcns.duplicate
//                                  (\"BARGS\", ${${B}Hosts}, true)"
//                             ncf
//                             ,
//                             fcn "hydra.TestConfigFcns.duplicate
//                                  (\"CARGS\", ${${C}Hosts}, true)"
//                             ncf;
//------------------------------------------------------------------------------
// Configures a gemfire description per logical host per subgroup
// and maps each one to the appropriate host description.
// No distributed system mapping is given.
//     Agemfire1  Agemfire2  ...  Agemfire${${A}Hosts}
//     Bgemfire1  Bgemfire2  ...  Bgemfire${${B}Hosts}
//     Cgemfire1  Cgemfire2  ...  Cgemfire${${C}Hosts}
//     Ahost1     Ahost2     ...  Ahost${${A}Hosts}
//     Bhost1     Bhost2     ...  Bhost${${B}Hosts}
//     Chost1     Chost2     ...  Chost${${C}Hosts}
//
hydra.GemFirePrms-names     = fcn "hydra.TestConfigFcns.generateNames
                                   (\"${A}gemfire\", ${${A}Hosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.generateNames
                                   (\"${B}gemfire\", ${${B}Hosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.generateNames
                                   (\"${C}gemfire\", ${${C}Hosts})"
                              ncf;
hydra.GemFirePrms-hostNames = fcn "hydra.TestConfigFcns.generateNames
                                   (\"${A}host\", ${${A}Hosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.generateNames
                                   (\"${B}host\", ${${B}Hosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.generateNames
                                   (\"${C}host\", ${${C}Hosts})"
                              ncf;
//------------------------------------------------------------------------------
// Example where all subgroups are peers.
//
//hydra.GemFirePrms-distributedSystem = ds;
//
//------------------------------------------------------------------------------
// Example where subgroups A and B are peers and C has only loners.
//
//hydra.GemFirePrms-distributedSystem =
//                              fcn "hydra.TestConfigFcns.duplicate
//                                   (\"ds\",    ${${A}Hosts})"
//                              ncf
//                              fcn "hydra.TestConfigFcns.duplicate
//                                   (\"ds\",    ${${B}Hosts})"
//                              ncf
//                              fcn "hydra.TestConfigFcns.duplicate
//                                   (\"loner\", ${${C}Hosts})"
//                              ncf;
//------------------------------------------------------------------------------
// Configures a client description per logical host per subgroup
// and maps each one to the appropriate VM description
// and maps each one to the appropriate GemFire description
// and scales the number of VMs and threads per VM for each.
//     A1         A2         ...  A${${A}Hosts}
//     B1         B2         ...  B${${B}Hosts}
//     C1         C2         ...  C${${C}Hosts}
//     Avm1       Avm2       ...  Avm${${A}Hosts}
//     Bvm1       Bvm2       ...  Bvm${${B}Hosts}
//     Cvm1       Cvm2       ...  Cvm${${C}Hosts}
//     Agemfire1  Agemfire2  ...  Agemfire${${A}Hosts}
//     Bgemfire1  Bgemfire2  ...  Bgemfire${${B}Hosts}
//     Cgemfire1  Cgemfire2  ...  Cgemfire${${C}Hosts}
//     ${${A}VMsPerHost}     ... ${${A}VMsPerHost} // ${${A}Hosts} times
//     ${${B}VMsPerHost}     ... ${${B}VMsPerHost} // ${${B}Hosts} times
//     ${${C}VMsPerHost}     ... ${${C}VMsPerHost} // ${${C}Hosts} times
//     ${${A}ThreadsPerVM}   ... ${${A}ThreadsPerVM} // ${${A}Hosts} times
//     ${${B}ThreadsPerVM}   ... ${${B}ThreadsPerVM} // ${${B}Hosts} times
//     ${${C}ThreadsPerVM}   ... ${${C}ThreadsPerVM} // ${${C}Hosts} times
//
hydra.ClientPrms-names        = fcn "hydra.TestConfigFcns.generateNames
                                     (\"${A}\", ${${A}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${B}\", ${${B}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${C}\", ${${C}Hosts})"
                                ncf;
hydra.ClientPrms-vmNames      = fcn "hydra.TestConfigFcns.generateNames
                                     (\"${A}vm\", ${${A}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${B}vm\", ${${B}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${C}vm\", ${${C}Hosts})"
                                ncf;
hydra.ClientPrms-gemfireNames = fcn "hydra.TestConfigFcns.generateNames
                                     (\"${A}gemfire\", ${${A}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${B}gemfire\", ${${B}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${C}gemfire\", ${${C}Hosts})"
                                ncf;
hydra.ClientPrms-vmQuantities = fcn "hydra.TestConfigFcns.duplicate
                                     (\"${${A}VMsPerHost}\", ${${A}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"${${B}VMsPerHost}\", ${${B}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"${${C}VMsPerHost}\", ${${C}Hosts})"
                                ncf;
hydra.ClientPrms-vmThreads    = fcn "hydra.TestConfigFcns.duplicate
                                     (\"${${A}ThreadsPerVM}\", ${${A}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"${${B}ThreadsPerVM}\", ${${B}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"${${C}ThreadsPerVM}\", ${${C}Hosts})"
                                ncf;
//------------------------------------------------------------------------------
// Example where all threads in the same subgroup are in the same threadgroup.
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
//THREADGROUP ${C}
//    totalThreads = fcn
//                   ${${C}Hosts} * ${${C}VMsPerHost} * ${${C}ThreadsPerVM}
//                   ncf
//    clientNames  = fcn "hydra.TestConfigFcns.generateNames
//                        (\"${C}\", ${${C}Hosts}, true)"
//                   ncf;
