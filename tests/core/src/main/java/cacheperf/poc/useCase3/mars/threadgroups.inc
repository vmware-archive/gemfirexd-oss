//------------------------------------------------------------------------------
// Threadgroups
//------------------------------------------------------------------------------

THREADGROUP trade
  totalThreads = fcn ${tradeHosts} * ${tradeVMsPerHost}
                                   * ${tradeThreadsPerVM}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames
                      (\"trade\", ${tradeHosts}, true)"
                 ncf;
THREADGROUP indicative
  totalThreads = fcn
                 ${indicativeHosts} * ${indicativeVMsPerHost}
                                    * ${indicativeThreadsPerVM}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames
                      (\"indicative\", ${indicativeHosts}, true)"
                 ncf;
THREADGROUP market
  totalThreads = fcn
                 ${marketHosts} * ${marketVMsPerHost}
                                * ${marketThreadsPerVM}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames
                      (\"market\", ${marketHosts}, true)"
                 ncf;
THREADGROUP position
  totalThreads = fcn
                 ${positionHosts} * ${positionVMsPerHost}
                                  * ${positionThreadsPerVM}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames
                      (\"position\", ${positionHosts}, true)"
                 ncf;
THREADGROUP risk
  totalThreads = fcn
                 ${riskHosts} * ${riskVMsPerHost}
                              * ${riskThreadsPerVM}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames
                      (\"risk\", ${riskHosts}, true)"
                 ncf;
THREADGROUP client
  totalThreads = fcn
                 ${clientHosts} * ${clientVMsPerHost}
                                * ${clientThreadsPerVM}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames
                      (\"client\", ${clientHosts}, true)"
                 ncf;
