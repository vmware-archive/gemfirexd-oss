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
import java.util.logging.FileHandler;
import java.util.logging.Level;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.*;

/**
 * @author glow
 *
 *
 */
public class GemFireQuoteFeeder extends MarketLoader {
  //
  Region marketDataCache = null;
  QuoteFeederThread feederThread = null;

  public GemFireQuoteFeeder ( ) {
    super ();
  }

  public static void main ( String[] args ) {
    // Get the cache
    Cache c = CacheFactory.getAnyInstance();

    Properties p = new Properties ( );
    // Get out the startup parameters
    try {
      p.put( FxPkConstants.QUOTES_TO_PROCESS, args[0] );
      p.put( FxPkConstants.QUOTES_PER_SECOND, args[1] );
      p.put( FxPkConstants.QUOTES_FILE_PROPERTY, args[2] );
    } catch ( Exception e ) {
      c.getLogger().severe("Error Extracting Startup Arguments" , e);
      return;
    }

    // Create an instance of the class . . .
    GemFireQuoteFeeder app = new GemFireQuoteFeeder ( );

    // Retrieve QUOTES region
    try {
      app.marketDataCache = c.getRegion ( FxPkConstants.QUOTES_REGION );
      c.getLogger().info ( "Retrieved Bridge Region QUOTES" );
      c.getLogger().info ( "QUOTES Region Attributes: " + app.marketDataCache.getAttributes().toString() );
    } catch ( Exception e ) {
      c.getLogger().severe("Could not create QUOTES region" , e);
    }
    app.init ( p );

    // Wait for notification that the trades are complete
    try {
      app.waitForNotification();
    } catch (InterruptedException e) {
      c.getLogger().severe("Quote feeder interrupted waiting for notification" , e);
      return;
    }
  }

  public void init ( Properties p ) {
    //
    initTestParams ( p );
    initLog ( "MarketLoader_LOG.txt" );

    feederThread = new QuoteFeederThread ( this );
    feederThread.start();
  }

  public void initTestParams ( Properties p ) {
    try {
      file_name = p.getProperty( FxPkConstants.QUOTES_FILE_PROPERTY );
      RATE_PER_SECOND = Integer.parseInt( p.getProperty( FxPkConstants.QUOTES_PER_SECOND ) );
      TOTAL_QUOTES = Integer.parseInt( p.getProperty( FxPkConstants.QUOTES_TO_PROCESS ) );
    } catch ( Exception e ) {
      System.out.println ( "Error getting trade pumping parameters, using defaults: " + e );
      e.printStackTrace();
      RATE_PER_SECOND = 1;
    }
  }

  public void initLog ( String logFileName ) {
    try {
      fh = new FileHandler( logFileName );
      logger.addHandler( fh );
      logger.setLevel(Level.ALL);
    } catch ( Exception e ) {
      System.out.println ( "Could not open log file handle! Exiting . . . " );
      System.exit(0);
    }
  }

  /**
   * NOT USED
   */
  protected void vendorInit(String[] args) throws Throwable {
    // NOT USED

  }

  /**
   * Handle insert into cache, RTE feeding, and mapping of ccy's to positions
   */
  protected void vendorPublishingAPI(String xml) throws Throwable {
    //
    MarketUpdate update = new MarketUpdate ( xml, false );
    marketDataCache.put ( update.ccy, update );

  }

  public class QuoteFeederThread extends Thread {
    GemFireQuoteFeeder loader;

    /**
     * This should be an initialized GemFireTradeLoader ready-to-go.
     * @param loader
     */
    public QuoteFeederThread ( GemFireQuoteFeeder loader ) {
      this.loader = loader;
    }

    public void run ( ) {
      try {
        loader.easyPumpMarketChanges();
        // Notify the main thread that trades are complete
        synchronized (completeNotification) {
          completeNotification.notify();
        }
      } 
      catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      }
      catch ( Throwable e ) {
        System.out.println ( "Thowable Error caught from easyPumpMarketChanges: " + e );
        e.printStackTrace();
      }
    }
  }
}
