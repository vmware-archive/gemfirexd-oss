//------------------------------------------------------------------------------
// GENERIC TOPOLOGY WITH A LOCATOR SUBGROUP AND FOUR NAMED SUBGROUPS
//
// It is up to users of this file to map the groups to distributed systems.
//
// Note that it is also up to the appropriate client VMs to become locators
// through a hydra task that invokes:
//
//              hydra.DistributedSystemHelper.createLocator
// and
//              hydra.DistributedSystemHelper.startLocator...
//
// It is important that all locators for a distributed system are created
// before it or any other members connect.
//
//------------------------------------------------------------------------------
// PROPERTIES:
//      locatorHosts         number of logical locator hosts
//      locatorVMsPerHost    number of locator VMs per logical locator host
//      locatorThreadsPerVM  number of locator threads per locator VM
//      A                    name of the first subgroup
//      ${A}Hosts            number of logical hosts in group A
//      ${A}VMsPerHost       number of VMs per host in group A
//      ${A}ThreadsPerVM     number of threads per VM in group A
//      B                    name of the second subgroup
//      ${B}Hosts            number of logical hosts in group B
//      ${B}VMsPerHost       number of VMs per host in group B
//      ${B}ThreadsPerVM     number of threads per VM in group B
//      C                    name of the third subgroup
//      ${C}Hosts            number of logical hosts in group C
//      ${C}VMsPerHost       number of VMs per host in group C
//      ${C}ThreadsPerVM     number of threads per VM in group C
//      D                    name of the fourth subgroup
//      ${D}Hosts            number of logical hosts in group D
//      ${D}VMsPerHost       number of VMs per host in group D
//      ${D}ThreadsPerVM     number of threads per VM in group D
//------------------------------------------------------------------------------
hydra.Prms-testTopology = "Generic topology logically divided into a locator subgroup and four named subgroups.  Subgroup locator has ${locatorHosts} hosts, ${locatorVMsPerHost} VMs per host, and ${locatorThreadsPerVM} threads per VM.  Subgroup ${A} has ${${A}Hosts} hosts, ${${A}VMsPerHost} VMs per host, and ${${A}ThreadsPerVM} threads per VM.  Subgroup ${B} has ${${B}Hosts} hosts, ${${B}VMsPerHost} VMs per host, and ${${B}ThreadsPerVM} threads per VM.  Subgroup ${C} has ${${C}Hosts} hosts, ${${C}VMsPerHost} VMs per host, and ${${C}ThreadsPerVM} threads per VM.  Subgroup ${D} has ${${D}Hosts} hosts, ${${D}VMsPerHost} VMs per host, and ${${D}ThreadsPerVM} threads per VM.";

hydra.Prms-manageLocatorAgents = false; // turn off master-managed locators

//------------------------------------------------------------------------------
// Configures a host description per logical host in each subgroup.
//     locatorhost1  locatorhost2  ...  locatorhost${locatorHosts}
//     Ahost1        Ahost2        ...  Ahost${${A}Hosts}
//     Bhost1        Bhost2        ...  Bhost${${B}Hosts}
//     Chost1        Chost2        ...  Chost${${C}Hosts}
//     Dhost1        Dhost2        ...  Dhost${${D}Hosts}
//
hydra.HostPrms-names = fcn "hydra.TestConfigFcns.generateNames
                            (\"locatorhost\", ${locatorHosts})"
                       ncf
                       fcn "hydra.TestConfigFcns.generateNames
                            (\"${A}host\", ${${A}Hosts})"
                       ncf
                       fcn "hydra.TestConfigFcns.generateNames
                            (\"${B}host\", ${${B}Hosts})"
                       ncf
                       fcn "hydra.TestConfigFcns.generateNames
                            (\"${C}host\", ${${C}Hosts})"
                       ncf
                       fcn "hydra.TestConfigFcns.generateNames
                            (\"${D}host\", ${${D}Hosts})"
                       ncf;
//------------------------------------------------------------------------------
// Configures a VM description per logical host per subgroup
// and maps each one to the appropriate host description
// and shows how to give different VM arguments to each subgroup.
//     locatorvm1    locatorvm2    ...  locatorvm${locatorHosts}
//     Avm1          Avm2          ...  Avm${${A}Hosts}
//     Bvm1          Bvm2          ...  Bvm${${B}Hosts}
//     Cvm1          Cvm2          ...  Cvm${${C}Hosts}
//     Dvm1          Dvm2          ...  Dvm${${D}Hosts}
//     locatorhost1  locatorhost2  ...  locatorhost${locatorHosts}
//     Ahost1        Ahost2        ...  Ahost${${A}Hosts}
//     Bhost1        Bhost2        ...  Bhost${${B}Hosts}
//     Chost1        Chost2        ...  Chost${${C}Hosts}
//     Dhost1        Dhost2        ...  Dhost${${D}Hosts}
//     LOCATORARGS   LOCATORARGS   ...  LOCATORARGS // ${locatorHosts} times
//     AARGS         AARGS         ...  AARGS // ${${A}Hosts} times
//     BARGS         BARGS         ...  BARGS // ${${B}Hosts} times
//     CARGS         CARGS         ...  CARGS // ${${C}Hosts} times
//     DARGS         DARGS         ...  DARGS // ${${D}Hosts} times
//
hydra.VmPrms-names         = fcn "hydra.TestConfigFcns.generateNames
                                  (\"locatorvm\", ${locatorHosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"${A}vm\", ${${A}Hosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"${B}vm\", ${${B}Hosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"${C}vm\", ${${C}Hosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"${D}vm\", ${${D}Hosts})"
                             ncf;
hydra.VmPrms-hostNames     = fcn "hydra.TestConfigFcns.generateNames
                                  (\"locatorhost\", ${locatorHosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"${A}host\", ${${A}Hosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"${B}host\", ${${B}Hosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"${C}host\", ${${C}Hosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"${D}host\", ${${D}Hosts})"
                             ncf;
//hydra.VmPrms-extraVMArgs   = fcn "hydra.TestConfigFcns.duplicate
//                                  (\"LOCATORARGS\", ${locatorHosts}, true)"
//                             ncf
//                             ,
//                             fcn "hydra.TestConfigFcns.duplicate
//                                  (\"AARGS\", ${${A}Hosts}, true)"
//                             ncf
//                             ,
//                             fcn "hydra.TestConfigFcns.duplicate
//                                  (\"BARGS\", ${${B}Hosts}, true)"
//                             ncf
//                             ,
//                             fcn "hydra.TestConfigFcns.duplicate
//                                  (\"CARGS\", ${${C}Hosts}, true)"
//                             ncf
//                             ,
//                             fcn "hydra.TestConfigFcns.duplicate
//                                  (\"DARGS\", ${${D}Hosts}, true)"
//                             ncf;
//------------------------------------------------------------------------------
// Configures a gemfire description per logical host per subgroup
// and maps each one to the appropriate host description.
// No distributed system mapping is given.
//     locatorgemfire1  locatorgemfire2  ...  locatorgemfire${locatorHosts}
//     Agemfire1        Agemfire2        ...  Agemfire${${A}Hosts}
//     Bgemfire1        Bgemfire2        ...  Bgemfire${${B}Hosts}
//     Cgemfire1        Cgemfire2        ...  Cgemfire${${C}Hosts}
//     Dgemfire1        Dgemfire2        ...  Dgemfire${${D}Hosts}
//     locatorhost1     locatorhost2     ...  locatorhost${locatorHosts}
//     Ahost1           Ahost2           ...  Ahost${${A}Hosts}
//     Bhost1           Bhost2           ...  Bhost${${B}Hosts}
//     Chost1           Chost2           ...  Chost${${C}Hosts}
//     Dhost1           Dhost2           ...  Dhost${${D}Hosts}
//
hydra.GemFirePrms-names     = fcn "hydra.TestConfigFcns.generateNames
                                   (\"locatorgemfire\", ${locatorHosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.generateNames
                                   (\"${A}gemfire\", ${${A}Hosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.generateNames
                                   (\"${B}gemfire\", ${${B}Hosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.generateNames
                                   (\"${C}gemfire\", ${${C}Hosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.generateNames
                                   (\"${D}gemfire\", ${${D}Hosts})"
                              ncf;
hydra.GemFirePrms-hostNames = fcn "hydra.TestConfigFcns.generateNames
                                   (\"locatorhost\", ${locatorHosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.generateNames
                                   (\"${A}host\", ${${A}Hosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.generateNames
                                   (\"${B}host\", ${${B}Hosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.generateNames
                                   (\"${C}host\", ${${C}Hosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.generateNames
                                   (\"${D}host\", ${${D}Hosts})"
                              ncf;
//------------------------------------------------------------------------------
// Example where all subgroups are peers.
//
//hydra.GemFirePrms-distributedSystem = ds;
//
//------------------------------------------------------------------------------
// Example where subgroups locator, A, B are peers, C and D are all loners.
//
//hydra.GemFirePrms-distributedSystem =
//                              fcn "hydra.TestConfigFcns.duplicate
//                                   (\"ds\",    ${locatorHosts})"
//                              ncf
//                              fcn "hydra.TestConfigFcns.duplicate
//                                   (\"ds\",    ${${A}Hosts})"
//                              ncf
//                              fcn "hydra.TestConfigFcns.duplicate
//                                   (\"ds\",    ${${B}Hosts})"
//                              ncf
//                              fcn "hydra.TestConfigFcns.duplicate
//                                   (\"loner\", ${${C}Hosts})"
//                              ncf
//                              fcn "hydra.TestConfigFcns.duplicate
//                                   (\"loner\", ${${D}Hosts})"
//                              ncf;
//------------------------------------------------------------------------------
// Configures a client description per logical host per subgroup
// and maps each one to the appropriate VM description
// and maps each one to the appropriate GemFire description
// and scales the number of VMs and threads per VM for each.
//   locator1        locator2        ...  locator${locatorHosts}
//   A1              A2              ...  A${${A}Hosts}
//   B1              B2              ...  B${${B}Hosts}
//   C1              C2              ...  C${${C}Hosts}
//   D1              D2              ...  D${${D}Hosts}
//   locatorvm1      locatorvm2      ...  locatorvm${locatorHosts}
//   Avm1            Avm2            ...  Avm${${A}Hosts}
//   Bvm1            Bvm2            ...  Bvm${${B}Hosts}
//   Cvm1            Cvm2            ...  Cvm${${C}Hosts}
//   Dvm1            Dvm2            ...  Dvm${${D}Hosts}
//   locatorgemfire1 locatorgemfire2 ...  locatorgemfire${locatorHosts}
//   Agemfire1       Agemfire2       ...  Agemfire${${A}Hosts}
//   Bgemfire1       Bgemfire2       ...  Bgemfire${${B}Hosts}
//   Cgemfire1       Cgemfire2       ...  Cgemfire${${C}Hosts}
//   Dgemfire1       Dgemfire2       ...  Dgemfire${${D}Hosts}
//   ${locatorVMsPerHost}   ... ${locatorVMsPerHost} // ${locatorHosts} times
//   ${${A}VMsPerHost}      ... ${${A}VMsPerHost} // ${${A}Hosts} times
//   ${${B}VMsPerHost}      ... ${${B}VMsPerHost} // ${${B}Hosts} times
//   ${${C}VMsPerHost}      ... ${${C}VMsPerHost} // ${${C}Hosts} times
//   ${${D}VMsPerHost}      ... ${${D}VMsPerHost} // ${${D}Hosts} times
//   ${locatorThreadsPerVM} ... ${locatorThreadsPerVM} // ${locatorHosts} times
//   ${${A}ThreadsPerVM}    ... ${${A}ThreadsPerVM} // ${${A}Hosts} times
//   ${${B}ThreadsPerVM}    ... ${${B}ThreadsPerVM} // ${${B}Hosts} times
//   ${${C}ThreadsPerVM}    ... ${${C}ThreadsPerVM} // ${${C}Hosts} times
//   ${${D}ThreadsPerVM}    ... ${${D}ThreadsPerVM} // ${${D}Hosts} times
//
hydra.ClientPrms-names        = fcn "hydra.TestConfigFcns.generateNames
                                     (\"locator\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${A}\", ${${A}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${B}\", ${${B}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${C}\", ${${C}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${D}\", ${${D}Hosts})"
                                ncf;
hydra.ClientPrms-vmNames      = fcn "hydra.TestConfigFcns.generateNames
                                     (\"locatorvm\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${A}vm\", ${${A}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${B}vm\", ${${B}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${C}vm\", ${${C}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${D}vm\", ${${D}Hosts})"
                                ncf;
hydra.ClientPrms-gemfireNames = fcn "hydra.TestConfigFcns.generateNames
                                     (\"locatorgemfire\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${A}gemfire\", ${${A}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${B}gemfire\", ${${B}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${C}gemfire\", ${${C}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${D}gemfire\", ${${D}Hosts})"
                                ncf;
hydra.ClientPrms-vmQuantities = fcn "hydra.TestConfigFcns.duplicate
                                  (\"${locatorVMsPerHost}\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                  (\"${${A}VMsPerHost}\", ${${A}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                  (\"${${B}VMsPerHost}\", ${${B}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                  (\"${${C}VMsPerHost}\", ${${C}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                  (\"${${D}VMsPerHost}\", ${${D}Hosts})"
                                ncf;
hydra.ClientPrms-vmThreads    = fcn "hydra.TestConfigFcns.duplicate
                                  (\"${locatorThreadsPerVM}\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                  (\"${${A}ThreadsPerVM}\", ${${A}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                  (\"${${B}ThreadsPerVM}\", ${${B}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                  (\"${${C}ThreadsPerVM}\", ${${C}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                  (\"${${D}ThreadsPerVM}\", ${${D}Hosts})"
                                ncf;
//------------------------------------------------------------------------------
// Example where all threads in the same subgroup are in the same threadgroup.
//
//THREADGROUP locator
//    totalThreads = fcn ${locatorHosts} * ${locatorVMsPerHost}
//                                       * ${locatorThreadsPerVM}
//                   ncf
//    clientNames  = fcn "hydra.TestConfigFcns.generateNames
//                        (\"locator\", ${locatorHosts}, true)"
//                   ncf;
//THREADGROUP ${A}
//    totalThreads = fcn ${${A}Hosts} * ${${A}VMsPerHost}
//                                    * ${${A}ThreadsPerVM}
//                   ncf
//    clientNames  = fcn "hydra.TestConfigFcns.generateNames
//                        (\"${A}\", ${${A}Hosts}, true)"
//                   ncf;
//THREADGROUP ${B}
//    totalThreads = fcn ${${B}Hosts} * ${${B}VMsPerHost}
//                                    * ${${B}ThreadsPerVM}
//                   ncf
//    clientNames  = fcn "hydra.TestConfigFcns.generateNames
//                        (\"${B}\", ${${B}Hosts}, true)"
//                   ncf;
//THREADGROUP ${C}
//    totalThreads = fcn ${${C}Hosts} * ${${C}VMsPerHost}
//                                    * ${${C}ThreadsPerVM}
//                   ncf
//    clientNames  = fcn "hydra.TestConfigFcns.generateNames
//                        (\"${C}\", ${${C}Hosts}, true)"
//                   ncf;
//THREADGROUP ${D}
//    totalThreads = fcn ${${D}Hosts} * ${${D}VMsPerHost}
//                                    * ${${D}ThreadsPerVM}
//                   ncf
//    clientNames  = fcn "hydra.TestConfigFcns.generateNames
//                        (\"${D}\", ${${D}Hosts}, true)"
//                   ncf;
//------------------------------------------------------------------------------
