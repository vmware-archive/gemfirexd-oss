
//----  INCLUDE FILE TO USE IN ALL WAN TESTS

wan.CacheServerPrms-gatewayHubConfig = hub;
hydra.GatewayHubPrms-names = hub;

wan.CacheServerPrms-gatewayConfig = gateway;
hydra.GatewayPrms-names = gateway;
hydra.GatewayPrms-maximumQueueMemory = 300;
hydra.GatewayPrms-batchTimeInterval = 10;
hydra.GatewayPrms-diskStoreName = disk;
hydra.DiskStorePrms-names = disk;
