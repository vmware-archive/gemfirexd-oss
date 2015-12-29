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

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.*;

import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Level;

/**
 * @author glow
 */
public class GemFireTradeLoader extends TradeLoader {
  Region tradeCache = null;
  Object completeNotification = new Object();

  TradeFeederThread feederThread = null;

  public GemFireTradeLoader ( ) {
    super();
  }

  public void init ( Properties p ) {
    initTestParams ( p );
    initLog ( "TradeLoader_LOG.txt" );

    feederThread = new TradeFeederThread ( this );
    feederThread.start();
  }

  public void initTestParams ( Properties p ) {
    try {
      file_name = p.getProperty( FxPkConstants.TRADES_FILE_PROPERTY );
      RATE_PER_SECOND = Integer.parseInt( p.getProperty( FxPkConstants.TRADES_PER_SECOND ) );
      START_ROW = Integer.parseInt( p.getProperty( FxPkConstants.TRADES_FILE_START_LINE ) );
      END_ROW = Integer.parseInt( p.getProperty( FxPkConstants.TRADES_FILE_END_LINE ) );
      START_TRADE_ID = Integer.parseInt ( p.getProperty ( FxPkConstants.TRADE_START_ID ) );
    } catch ( Exception e ) {
      System.out.println ( "Error getting trade pumping parameters, using defaults: " + e );
      e.printStackTrace();
      RATE_PER_SECOND = 1;
      START_ROW = 1;
      END_ROW = 11;
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

  public void start ( ) {
    easyPumpTrades ( );
  }

  protected void vendorPublishingAPI(String xml) throws Throwable {
    Trade t = new Trade ( xml);
    t.setGFLatencyStart();
    tradeCache.put ( new Long ( t.TradeId ), t );
  }

  protected void waitForNotification() throws InterruptedException {
    synchronized (completeNotification) {
      completeNotification.wait();
    }
  }

  /**
   * NOT USED.
   */
  protected void vendorInit(String[] args) throws Throwable {
    //

    System.out.println ( "DEBUG: End GemFireTradeLoader.vendorInit()");
  }

  /**
   * @author glow
   *
   * A simple Thread to read the trades from file
   */
  private class TradeFeederThread extends Thread {
    GemFireTradeLoader loader;

    /**
     * This should be an initialized GemFireTradeLoader ready-to-go.
     * @param loader
     */
    public TradeFeederThread ( GemFireTradeLoader loader ) {
      this.loader = loader;
    }

    public void run ( ) {
      try {
        loader.easyPumpTrades();
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
        System.out.println ( "Thowable Error caught from easyPumpTrades: " + e );
        e.printStackTrace();
      }
    }
  }

}
