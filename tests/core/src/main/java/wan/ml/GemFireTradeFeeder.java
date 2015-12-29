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

import java.util.Properties;

import com.gemstone.gemfire.cache.*;

/**
 * @author glow
 *
 * Encapsulates the logic to feed trades read from a file into an FxPk Cache Server via a BridgeWriter
 * This is in a sub-class for Trade loading to properly handle the fact that the in-process class uses
 * the WanCacheRegion interface for data updates, while the Bridge Feeder uses the Region interface.
 */
public class GemFireTradeFeeder extends GemFireTradeLoader {

  public static void main ( String[] args ) {
    // Get the cache
    Cache c = CacheFactory.getAnyInstance();

    Properties p = new Properties ( );
    // Get out the startup parameters
    try {
      p.put( FxPkConstants.TRADE_START_ID, args[0] );
      p.put( FxPkConstants.TRADES_FILE_START_LINE, args[1] );
      p.put( FxPkConstants.TRADES_FILE_END_LINE, args[2] );
      p.put( FxPkConstants.TRADES_PER_SECOND, args[3] );
      p.put( FxPkConstants.TRADES_FILE_PROPERTY, args[4] );
    } catch ( Exception e ) {
      c.getLogger().severe("Error Extracting Startup Arguments" , e);
      return;
    }

    // Create an instance of the class . . .
    GemFireTradeFeeder app = new GemFireTradeFeeder ( );

    // Retrieve TRADES region
    try {
      app.tradeCache = c.getRegion ( FxPkConstants.TRADES_REGION );
    } catch ( Exception e ) {
      c.getLogger().severe("Could not create TRADES region" , e);
      return;
    }

    app.init ( p );

    // Wait for notification that the trades are complete
    try {
      app.waitForNotification();
    } catch (InterruptedException e) {
      c.getLogger().severe("Trade feeder interrupted waiting for notification" , e);
      return;
    }
  }
}
