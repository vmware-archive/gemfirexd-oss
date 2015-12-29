//
// adds client/server connection-related knobs
//

hydra.Prms-testDescription += fcn "smoketest.scale.Fcns.describeConnections(${maxThreads}, ${maxConnections}, ${threadLocalConnections})" ncf;

hydra.BridgePrms-maxThreads = ${maxThreads};

hydra.PoolPrms-contactAlgorithm = hydra.EdgeHelper getRoundRobinEndpoints;
hydra.PoolPrms-maxConnections = ${maxConnections};
hydra.PoolPrms-threadLocalConnections = ${threadLocalConnections};
