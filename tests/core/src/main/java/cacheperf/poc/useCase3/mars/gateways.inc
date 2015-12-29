//------------------------------------------------------------------------------
// Gateway Hubs
//

hydra.GatewayHubPrms-names                    = DB        SERVER_DB;
hydra.GatewayHubPrms-acceptGatewayConnections = false;
hydra.GatewayHubPrms-distributionPolicy       = parallel;
hydra.GatewayHubPrms-haEnabled                = true      false;

//------------------------------------------------------------------------------
// Gateways
//

hydra.GatewayPrms-names     = DB SERVER_DB;

hydra.GatewayPrms-batchSize = 500;
hydra.GatewayPrms-batchConflation = false;
hydra.GatewayPrms-batchTimeInterval = 1000;
hydra.GatewayPrms-diskStoreName = diskGLOBAL;
hydra.GatewayPrms-listeners = cacheperf.gemfire.GatewayLatencyListener;
                              //com.useCase3.rds.masterserver.DBWriteBehindListener;
hydra.GatewayPrms-listenersSingleton = true;
hydra.GatewayPrms-maximumQueueMemory = 5;

//hydra.GatewayPrms-numCopies = 1  15;
hydra.GatewayPrms-numCopies = 1  3;
