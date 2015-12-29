//------------------------------------------------------------------------------
// GENERIC TOPOLOGY WITH A LOCATOR SUBGROUP AND ONE NAMED SUBGROUP
//
// It is up to users of this file to map the groups to fabric server
// distributed systems.
//
// Note that it is also up to the locator VMs to become gemfirexd locators
// through hydra task(s) that invoke:
//
//              hydra.gemfirexd.FabricServerHelper.createLocator
// and
//              hydra.gemfirexd.FabricServerHelper.startLocator
//
// It is important that all locators for a fabric are created before it
// or any other fabric servers connect.
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
//------------------------------------------------------------------------------
hydra.Prms-testTopology = "Generic topology logically divided into a locator subgroup and one named subgroup.  Subgroup locator has ${locatorHosts} hosts, ${locatorVMsPerHost} VMs per host, and ${locatorThreadsPerVM} threads per VM.  Subgroup ${A} has ${${A}Hosts} hosts, ${${A}VMsPerHost} VMs per host, and ${${A}ThreadsPerVM} threads per VM.";

hydra.Prms-manageLocatorAgents = false; // turn off master-managed locators

//------------------------------------------------------------------------------
// Configures a host description per logical host in each subgroup.
//     locatorhost1  locatorhost2  ...  locatorhost${locatorHosts}
//     Ahost1        Ahost2        ...  Ahost${${A}Hosts}
//
hydra.HostPrms-names = fcn "hydra.TestConfigFcns.generateNames
                            (\"locatorhost\", ${locatorHosts})"
                       ncf
                       fcn "hydra.TestConfigFcns.generateNames
                            (\"${A}host\", ${${A}Hosts})"
                       ncf;
//------------------------------------------------------------------------------
// Configures a VM description per logical host per subgroup
// and maps each one to the appropriate host description
// and shows how to give different VM arguments to each subgroup.
//     locatorvm1    locatorvm2    ...  locatorvm${locatorHosts}
//     Avm1          Avm2          ...  Avm${${A}Hosts}
//     locatorhost1  locatorhost2  ...  locatorhost${locatorHosts}
//     Ahost1        Ahost2        ...  Ahost${${A}Hosts}
//     LOCATORARGS   LOCATORARGS   ...  LOCATORARGS // ${locatorHosts} times
//     AARGS         AARGS         ...  AARGS // ${${A}Hosts} times
//
hydra.VmPrms-names         = fcn "hydra.TestConfigFcns.generateNames
                                  (\"locatorvm\", ${locatorHosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"${A}vm\", ${${A}Hosts})"
                             ncf;
hydra.VmPrms-hostNames     = fcn "hydra.TestConfigFcns.generateNames
                                  (\"locatorhost\", ${locatorHosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"${A}host\", ${${A}Hosts})"
                             ncf;
//hydra.VmPrms-extraVMArgs   = fcn "hydra.TestConfigFcns.duplicate
//                                  (\"LOCATORARGS\", ${locatorHosts}, true)"
//                             ncf
//                             ,
//                             fcn "hydra.TestConfigFcns.duplicate
//                                  (\"AARGS\", ${${A}Hosts}, true)"
//                             ncf;
//------------------------------------------------------------------------------
// Configures a client description per logical host per subgroup
// and maps each one to the appropriate VM description
// and scales the number of VMs and threads per VM for each.
//   locator1        locator2        ...  locator${locatorHosts}
//   A1              A2              ...  A${${A}Hosts}
//   locatorvm1      locatorvm2      ...  locatorvm${locatorHosts}
//   Avm1            Avm2            ...  Avm${${A}Hosts}
//   ${locatorVMsPerHost}   ... ${locatorVMsPerHost} // ${locatorHosts} times
//   ${${A}VMsPerHost}      ... ${${A}VMsPerHost} // ${${A}Hosts} times
//   ${locatorThreadsPerVM} ... ${locatorThreadsPerVM} // ${locatorHosts} times
//   ${${A}ThreadsPerVM}    ... ${${A}ThreadsPerVM} // ${${A}Hosts} times
//
hydra.ClientPrms-names        = fcn "hydra.TestConfigFcns.generateNames
                                     (\"locator\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${A}\", ${${A}Hosts})"
                                ncf;
hydra.ClientPrms-vmNames      = fcn "hydra.TestConfigFcns.generateNames
                                     (\"locatorvm\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${A}vm\", ${${A}Hosts})"
                                ncf;
hydra.ClientPrms-vmQuantities = fcn "hydra.TestConfigFcns.duplicate
                                  (\"${locatorVMsPerHost}\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                  (\"${${A}VMsPerHost}\", ${${A}Hosts})"
                                ncf;
hydra.ClientPrms-vmThreads    = fcn "hydra.TestConfigFcns.duplicate
                                  (\"${locatorThreadsPerVM}\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                  (\"${${A}ThreadsPerVM}\", ${${A}Hosts})"
                                ncf;
//------------------------------------------------------------------------------
// Configures a fabric server description per subgroup
// and maps each one to the appropriate client descriptions.
// No distributed system is given.
//
hydra.gemfirexd.FabricServerPrms-names = locator ${A};
hydra.gemfirexd.FabricServerPrms-clientNames =
      fcn "hydra.TestConfigFcns.generateNames
           (\"locator\", ${locatorHosts})" ncf
      ,
      fcn "hydra.TestConfigFcns.generateNames
           (\"${A}\", ${${A}Hosts})" ncf
      ;
//------------------------------------------------------------------------------
// Example where all subgroups are peers.
//
//hydra.gemfirexd.FabricServerPrms-distributedSystem = ds;
//
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
//------------------------------------------------------------------------------
