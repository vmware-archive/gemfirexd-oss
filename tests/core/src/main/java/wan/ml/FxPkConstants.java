/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package wan.ml;

/**
 * @author glow
 *
 * Hold all the constants used for the FxPk GemFire implementation.
 */
public interface FxPkConstants {
  public static final String DB_CONX_URL_PROPERTY = "jdbc_conx_url";
  public static final String RTE_ENDPOINTS = "endpoints"; // Server1=host:port,Server2=host:port,etc . . .
  public static final String RTE_TIMEOUT = "readTimeout"; // In Milli's
  public static final String RTE_RETRY_ATTEMPTS = "retryAttempts"; // Default 5
  public static final String RTE_RETRY_INTERVAL = "retryInterval"; // In Milli's
  public static final String RTE_LOAD_BALANCING_POLICY = "LBPolicy"; // Sticky, RoundRobin, or Random
  public static final String TRADES_FILE_PROPERTY = "trade_file_location";
  public static final String TRADES_FILE_START_LINE = "trades_file_start_line";
  public static final String TRADES_FILE_END_LINE = "trades_file_end_line";
  public static final String TRADES_PER_SECOND = "trades_per_second";
  public static final String TRADE_START_ID = "trade_start_id"; // initializes the TradeID counter in TradeLoader
  public static final String QUOTES_FILE_PROPERTY = "quotes_file_location";
  public static final String QUOTES_FILE_START_LINE = "quotes_file_start_line";
  public static final String QUOTES_FILE_END_LINE = "quotes_file_end_line";
  public static final String QUOTES_TO_PROCESS = "quotes_to_process";
  public static final String QUOTES_PER_SECOND = "quotes_per_second";
  public static final String POSITION_UPDATE_DB_THREAD_COUNT = "num_position_db_update_threads";
  public static final String JDBC_DRIVER_CLASSNAME = "jdbc_driver_class";
  public static final String DB_USER = "db_user";
  public static final String DB_PASSWORD = "db_password";
  public static final String TRADES_REGION = "TRADES";
  public static final String POSITIONS_REGION = "POSITIONS";
  public static final String POISTION_DELTAS_REGION = "POSITION_DELTAS";
  public static final String QUOTES_REGION = "QUOTES";
  public static final String WAN_DELIVERY_BATCH_SIZE = "wan_delivery_batch_size";
  public static final String PROPERTY_PASSIVE_MODE = "passive_mode";
  public static final String GEMFIRE_PERSISTENCE_DIR = "gf_persistence_dir";
  /**
   * Primary runs feed handlers, puts objects into the cache, and publishes to RTE.
   * Note: Both now do position aggregation independently (driven by new Trades and MarketUpdates).
   */
  public static final String ROLE_PRIMARY = "PRIMARY";
  /**
   * Secondary does Position Aggregation, DB Persistence.
   */
  public static final String ROLE_SECONDARY = "SECONDARY";
  /**
   * If both, we assume the responsibilities of both PRIMARY and SECONDARY
   */
  public static final String ROLE_BOTH = "BOTH";

  public static final String BURST_SLEEP_INTERVAL = "BURST_SLEEP_INTERVAL";
  public static final String BURST_TIME = "BURST_TIME";

}
