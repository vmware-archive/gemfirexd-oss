hydra.CachePrms-names = server client;

hydra.RegionPrms-names =
  server_trade_command
  server_indicative_command
  server_market_command
  server_position_command
  server_risk_command
  client_trade_command
  client_indicative_command
  client_market_command
  client_position_command
  client_risk_command
  ;
hydra.RegionPrms-regionName =
  TRADE      // server_trade_command
  INDICATIVE // server_indicative_command
  MARKET     // server_market_command
  POSITION   // server_position_command
  RISK       // server_risk_command
  TRADE      // client_trade_command
  INDICATIVE // client_indicative_command
  MARKET     // client_market_command
  POSITION   // client_position_command
  RISK       // client_risk_command
  ;
hydra.RegionPrms-dataPolicy =
  empty // server_trade_command
  empty // server_indicative_command
  empty // server_market_command
  empty // server_position_command
  empty // server_risk_command
  empty // client_trade_command
  empty // client_indicative_command
  empty // client_market_command
  empty // client_position_command
  empty // client_risk_command
  ;
hydra.RegionPrms-poolName =
  none // server_trade_command
  none // server_indicative_command
  none // server_market_command
  none // server_position_command
  none // server_risk_command
  Trade-POOL      // client_trade_command
  Indicative-POOL // client_indicative_command
  Market-POOL     // client_market_command
  Position-POOL   // client_position_command
  Results-POOL    // client_risk_command
  ;
hydra.RegionPrms-scope =
  local // server_trade_command
  local // server_indicative_command
  local // server_market_command
  local // server_position_command
  local // server_risk_command
  local // client_trade_command
  local // client_indicative_command
  local // client_market_command
  local // client_position_command
  local // client_risk_command
  ;
