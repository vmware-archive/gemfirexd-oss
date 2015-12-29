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

import java.io.*;
import java.util.zip.GZIPInputStream;

import java.util.Properties;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.*;

/**
 * @author glow
 *
 * Encapsulates the logic to feed trades read from a file into an FxPk Cache Server via a BridgeWriter
 * This is in a sub-class for Trade loading to properly handle the fact that the in-process class uses
 * the WanCacheRegion interface for data updates, while the Bridge Feeder uses the Region interface.
 */
public class GemFireTradeBurstFeeder extends GemFireTradeLoader {

  Region bridgeTradeCache = null;

  protected int burst_sleep_interval;
  protected int burst_time;

  public static void main ( String[] args ) {

    Properties p = new Properties ( );
    // Get out the startup parameters
    try {
      p.put( FxPkConstants.TRADE_START_ID, args[0] );
      p.put( FxPkConstants.TRADES_FILE_START_LINE, args[1] );
      p.put( FxPkConstants.TRADES_FILE_END_LINE, args[2] );
      p.put( FxPkConstants.TRADES_PER_SECOND, args[3] );
      p.put( FxPkConstants.TRADES_FILE_PROPERTY, args[4] );

      p.put( FxPkConstants.BURST_SLEEP_INTERVAL, args[5] );
      p.put( FxPkConstants.BURST_TIME, args[6] );

    } catch ( Exception e ) {
      System.out.println ( "Error Extracting Startup Arguments: " + e );
      e.printStackTrace();
      System.exit ( 0 );
    }

    // Create an instance of the class . . .
    GemFireTradeBurstFeeder app = new GemFireTradeBurstFeeder ( );

    // Get the cache
    Cache c = CacheFactory.getAnyInstance();

    // Retrieve TRADES region
    try {
      app.bridgeTradeCache = c.getRegion ( FxPkConstants.TRADES_REGION );
      System.out.println ( "Retrieved Bridge Region TRADES" );
      System.out.println ( "TRADES Region Attributes: " + app.bridgeTradeCache.getAttributes().toString() );
    } catch ( Exception e ) {
      System.out.println ( "Could not create TRADES bridge Region: " + e );
      e.printStackTrace();
      System.exit(0);
    }

    app.init ( p );

  }

  public void initTestParams ( Properties p ) {
    super.initTestParams ( p );
    try {
      burst_sleep_interval = Integer.parseInt( p.getProperty( FxPkConstants.BURST_SLEEP_INTERVAL ) ) * 1000;
      burst_time = Integer.parseInt ( p.getProperty ( FxPkConstants.BURST_TIME ) );

      System.out.println("burst_sleep_interval: " + burst_sleep_interval + " ms");
      System.out.println("burst_time: " + burst_time + " sec");
    } catch ( Exception e ) {
      e.printStackTrace();
    }
  }

  /**
   * this method uses the various arguments to the main method to pump trades
   * through the vendorPublishingAPI <br />trades are read from a file and then row
   * by row fed through the vendorPublishingAPI
   *
   */
  protected void easyPumpTrades() {
    String xml = null;
    boolean isPureDelayAdjustedForLatency = false;
    int trade_count = 0;
    start_time = System.currentTimeMillis();
    long endBatch = 0, startBatch = 0;;

    int burst=0;
    int sentInBurst=0;
    long now, end;
    double sleepMs=0.0, accumulatedSleepMs=0.0;
    int interval = 1000; // 1 second interval

    try {
      //FileReader reader = new FileReader(file_name);
      //LineNumberReader line_reader = new LineNumberReader(reader);
      LineNumberReader line_reader = new LineNumberReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file_name))));
      line_reader.setLineNumber(START_ROW);
      sleepMs = 0;
      outside_loop: while (true) {

        // Sleep for the burst_sleep_interval between each burst
        System.out.println("Sleeping for " + burst_sleep_interval + " ms");
        Thread.sleep(burst_sleep_interval);

        // Publish burst_time seconds worth of messages
        burst = 0;
        while (burst < burst_time) {
          System.out.println("Sending burst " + burst+1);
          now = System.currentTimeMillis();
          end = now + interval;
          sentInBurst = 0;
          long startBurst = System.currentTimeMillis();
          while (now<end && sentInBurst<RATE_PER_SECOND) {
            now = System.currentTimeMillis();
            // Read and send trade
            xml = line_reader.readLine();
            //System.out.println("Read xml: " + xml);
            if (null == xml) {
              logger.warning("The XML is null at line number " + line_reader.getLineNumber());
              long end_time = System.currentTimeMillis();
              System.out.println("Processed " + trade_count + " trades in "
                  + (end_time - start_time) + " millies.");
              System.exit(0);
            }
            if (("<XMLROOT>".equals(xml))
                  || (line_reader.getLineNumber() < START_ROW)) {
              continue;
            } else if (("</XMLROOT>".equals(xml))
                  || (line_reader.getLineNumber() > END_ROW)) {
              break outside_loop;
            } else {
              xml = assignTradeId(xml);
              long writeStartNanoTime = System.currentTimeMillis();
              long writeWallStartTime = System.currentTimeMillis();
              vendorPublishingAPI(xml);
              bracketLog(writeWallStartTime, System.currentTimeMillis() - writeStartNanoTime, xml);
              //System.out.println("Sending trade " + trade_count + " in burst " + burst);
              trade_count++;

              // For each trade sleep the remaining time to fill up the interval.
              // This helps fill the entire interval with trades, rather than
              // the burst, sleep cycle. This actually accumulates a certain number
              // of milliseconds before sleeping.
              accumulatedSleepMs += sleepMs;
              if (accumulatedSleepMs > 50)
              {
                accumulatedSleepMs -= 50;
                try {
                  //System.out.println("Sleeping for: 50 ms");
                  Thread.sleep(50);
                } catch (InterruptedException e) {break;}
              }
              sentInBurst++;
            }
          }
            long endBurst = System.currentTimeMillis();
            System.out.println("Time to send a burst: " + (endBurst - startBurst));

          System.out.println("Sent " + sentInBurst + " trades in burst " + burst);

          // Sleep for the remainder of the interval if necessary
          if (now < end)
          {
            // How much time is left in the interval per message.
            // Use this value in the next iteration to sleep between each
            // message published.

            // Calculate sleepMs using the previous value plus the time
            // left over in the second
            sleepMs = sleepMs + ((end-now)/(RATE_PER_SECOND*1.0));
            accumulatedSleepMs = 0.0;
            try {
              System.out.println("Sleeping for: " + (end-now) + " ms");
              Thread.sleep(end-now);
            } catch (InterruptedException e) {break;}
          }
          else
          {
            // Calculate sleepMs using the previous value minus the time
            // over the second
            sleepMs = sleepMs - ((now-end)/(RATE_PER_SECOND*1.0));
          }
          burst++;
        }
      }
    } 
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch (Throwable e) {
      e.printStackTrace();
    }

    long end_time = System.currentTimeMillis();

    System.out.println("Processed " + trade_count + " trades in "
        + (end_time - start_time) + " millies.");
  }


  protected void vendorPublishingAPI(String xml) throws Throwable {

    Trade t = new Trade ( xml );
    t.setGFLatencyStart();
    bridgeTradeCache.put ( new Long ( t.TradeId ), t );

  }
}
