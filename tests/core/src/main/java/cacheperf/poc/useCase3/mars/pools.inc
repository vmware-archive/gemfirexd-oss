hydra.PoolPrms-names =
      Trade-POOL
      Indicative-POOL
      Market-POOL
      Position-POOL
      Results-POOL
      ;
hydra.PoolPrms-contactAlgorithm = hydra.PoolHelper getSameContacts
      ;
hydra.PoolPrms-distributedSystem =
      trade
      indicative
      market
      position
      results
      ;
hydra.PoolPrms-idleTimeout = -1
      ;
hydra.PoolPrms-loadConditioningInterval = -1
      ;
hydra.PoolPrms-minConnections = 1
      ;
hydra.PoolPrms-readTimeout = 300000
      ;
hydra.PoolPrms-retryAttempts =
      0   // Trade-POOL
      0   // Indicative-POOL
      0   // Market-POOL
      0   // Position-POOL
      1   // Results-POOL
      ;
