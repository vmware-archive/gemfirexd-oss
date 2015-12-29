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

package hydra.timeserver;

import hydra.Log;
import hydra.timeserver.TimeProtocolHandler.SkewData;

import java.io.IOException;
import java.math.BigInteger;
import java.net.SocketTimeoutException;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.internal.NanoTimer;

/**
 * A thread that fetches the time from the time server on the specified host
 * and port with the given frequency.
 */
public class TimeClient extends Thread {

  private final String serverHost; // time server host
  private final int serverPort;    // time server port
  private final int frequencyMs;   // frequency with which to fetch time from server
  private final int latencyThresholdMs; //threshold to filter out round trips which are too high (due to network jitter, gc, etc).
  private final int samplesToAverage; //number of requests to make before setting the clock skew.
  
  ClockSkewStats statistics;

  // current skew between this host and the server
  private long clockSkew = Long.MAX_VALUE;
  private long networkLatency = Long.MAX_VALUE;
  /** big integer is required because the sum can be greater than Long.MAX_VALUE
    * and we need full precision */
  public BigInteger skewSum = BigInteger.valueOf(0);
  public long networkLatencySum = 0;
  public long packetCount = 0;
  
  public TimeClient(String host, int port, int frequencyMs, int latencyThresholdMs, int samplesToAverage) throws IOException {
    super("TimeClient");
    this.serverHost = host;
    this.serverPort = port;
    this.frequencyMs = frequencyMs;
    this.latencyThresholdMs = latencyThresholdMs;
    this.samplesToAverage = samplesToAverage;
  }
  
  public int getFrequencyMs() {
    return this.frequencyMs;
  }
  
  public synchronized long getClockSkew() {
    return this.clockSkew;
  }
  
  public synchronized long getNetworkLatency() {
    return this.networkLatency;
    
  }
  
  private synchronized void setClockSkew(long skew, long networkLatency) {
    boolean initialization = this.clockSkew == Long.MAX_VALUE;
    this.clockSkew = skew;
    this.networkLatency = networkLatency;
    if (this.statistics != null) {
      this.statistics.setClockSkew(skew);
      this.statistics.setLatency(networkLatency);
    }
    if(initialization) {
      this.notify();
    }
  }
  
  public synchronized void openStatistics() {
    if (this.statistics == null) {
      this.statistics = ClockSkewStats.getInstance();
    }
  }
  public synchronized void closeStatistics() {
    if (this.statistics != null) {
      this.statistics.close();
      this.statistics = null;
    }
  }
  public String toString() {
    long skew = getClockSkew();
    if (skew == Long.MAX_VALUE) {
      return serverHost + ":" + serverPort + " (every " + frequencyMs + " ms)"
                        + " with current skew uninitialized";
    } else {
      return serverHost + ":" + serverPort + " (every " + frequencyMs + " ms)"
                        + " with current skew " + getClockSkew();
    }
  }
  public void run() {
    for (int i = 0; i < 20000; i++) {
      NanoTimer.getTime();
    }
    TimeProtocolHandler handler = null;

    try {
      handler = new TimeProtocolHandler(serverHost, serverPort, latencyThresholdMs, true);
      while (true) {
        try {
          SkewData skewData = handler.checkSkew();

          if(skewData.getLatency() > latencyThresholdMs * 1000000L)
          {
            if(Log.getLogWriter().fineEnabled())
            {
              Log.getLogWriter().fine("Latency " + skewData.getLatency() + " exceeded threshold " + latencyThresholdMs);
            }
          }
          else
          {
            packetCount++;
            networkLatencySum += skewData.getLatency();
            skewSum = skewSum.add(BigInteger.valueOf(skewData.getSkew()));

            //average several samples into a clock skew
            if(packetCount % samplesToAverage == 0)
            {
              long networkLatency = networkLatencySum / packetCount;
              long skew =  skewSum.divide(BigInteger.valueOf(packetCount)).longValue();
              setClockSkew(skew, networkLatency);
              packetCount = 0;
              networkLatencySum = 0;
              skewSum = BigInteger.valueOf(0);
            }

          }

          // sleep in between requests
          sleep();
        } catch (IOException e) {
          if(e instanceof SocketTimeoutException)
          {
            Log.getLogWriter().fine("Timeout waiting for a response from time server");
          }
          else
          {
            Log.getLogWriter().warning("Error in time client", e);
          }
          sleep();
        }
      }
    } catch (InterruptedException e1) {
      return;
    } 
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch (Throwable e1) {
      Log.getLogWriter().error("Error in time client", e1);
      return;
    } finally {
      if (handler != null) {
        handler.close();
      }
    }
  }
  
  private void sleep() throws InterruptedException
  {
    //  To save time, make the first batch of requests quickly so that we have a clock skew to use.
    if(packetCount < samplesToAverage)
    {
      Thread.sleep(frequencyMs / samplesToAverage);
    }
    else
    {
      Thread.sleep(frequencyMs);
    }
  }
  
  /**
   * Wait until the clock skew is set to a real value.
   * @throws InterruptedException if interrupted while waiting
   */
  public void waitForInitialization() throws InterruptedException
  {
    synchronized(this)
    {
      while(getClockSkew() == Long.MAX_VALUE)
      {
        this.wait();
      }
    }
  }
}
