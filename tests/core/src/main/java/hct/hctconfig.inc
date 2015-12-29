//------------------------------------------------------------------------------
// Default bridge and edge config for subset of tests in this directory
//------------------------------------------------------------------------------

hydra.BridgePrms-names = bridge;
hydra.BridgePrms-maxThreads = ONEOF default 16 FOENO; // selector control
 

hydra.PoolPrms-names = brclient;
hydra.PoolPrms-minConnections = 10;
hydra.PoolPrms-threadLocalConnections = true;

//------------------------------------------------------------------------------
// Default cache and region config for subset of tests in this directory
//------------------------------------------------------------------------------

hydra.CachePrms-names        = bridge             edge;

hydra.RegionPrms-names       = bridge             edge;
hydra.RegionPrms-cacheLoader = hct.DatabaseLoader none;
hydra.RegionPrms-poolName    = none               brclient;
hydra.RegionPrms-scope       = dack               local;
// the bridge scope was previously local (each server was in its own ds)

hydra.RegionPrms-evictionAttributes = lruMemorySize 100, // bridge
                                      lruMemorySize 100; // edge

hydra.RegionPrms-concurrencyLevel = 16;
hydra.RegionPrms-initialCapacity  = 16;
hydra.RegionPrms-keyConstraint    = java.lang.String;
