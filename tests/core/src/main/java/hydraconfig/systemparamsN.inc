/**
 *  systemparamsN.inc
 *
 *  Creates N gemfire systems on N hosts for N clients with 4 threads each.
 *
 *  To use, either set -Dhydra.numHosts=N on the hydra command line, or set
 *  hydra.numHosts=N for the test in the batterytest .bt file, for the desired
 *  value of N.
 */
INCLUDE $JTESTS/hydraconfig/gemfireprms.inc;

hydra.HostPrms-names          = fcn "hydra.TestConfigFcns.generateNames(\"host\",      ${hydra.numHosts} )" ncf;

hydra.VmPrms-names            = fcn "hydra.TestConfigFcns.generateNames(\"vm\",        ${hydra.numHosts} )" ncf;
hydra.VmPrms-hostNames        = fcn "hydra.TestConfigFcns.generateNames(\"host\",      ${hydra.numHosts} )" ncf;

hydra.GemFirePrms-names       = fcn "hydra.TestConfigFcns.generateNames(\"gemfire\",   ${hydra.numHosts} )" ncf;
hydra.GemFirePrms-hostNames   = fcn "hydra.TestConfigFcns.generateNames(\"host\",      ${hydra.numHosts} )" ncf;

hydra.ClientPrms-names        = fcn "hydra.TestConfigFcns.generateNames(\"client\",    ${hydra.numHosts} )" ncf;
hydra.ClientPrms-vmNames      = fcn "hydra.TestConfigFcns.generateNames(\"vm\",        ${hydra.numHosts} )" ncf;
hydra.ClientPrms-gemfireNames = fcn "hydra.TestConfigFcns.generateNames(\"gemfire\",   ${hydra.numHosts} )" ncf;

hydra.ClientPrms-vmQuantities = 1;
hydra.ClientPrms-vmThreads    = 4;
