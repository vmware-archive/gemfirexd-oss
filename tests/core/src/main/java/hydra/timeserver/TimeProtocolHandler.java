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


import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.internal.NanoTimer;

/**
 * Manages a connection to a time server.
 * @author dsmith
 *
 */
public class TimeProtocolHandler {
  private final String serverHost; // time server host
  private final int serverPort;    // time server port
  private final boolean useNanoTime;
  private byte packetId = 0;
  private final DatagramSocket socket;
  private final InetAddress address;
  private final int socketTimeout;

  public TimeProtocolHandler(String serverHost, int serverPort, int socketTimeout, boolean useNanoTime) throws SocketException, UnknownHostException
  {
    this.serverHost = serverHost;
    this.serverPort = serverPort;
    this.socketTimeout = socketTimeout;
    this.useNanoTime = useNanoTime;

    this.address = InetAddress.getByName(this.serverHost);

//  get a datagram socket
    this.socket = new DatagramSocket();
    try
    {
      this.socket.setSoTimeout(socketTimeout);
      this.socket.setReceiveBufferSize(32);
      this.socket.setSendBufferSize(32);
    } catch(SocketException e)
    {
      try
      {
        this.socket.close();
      } catch (Exception e2)
      {
        //do nothing
      }
      throw e;
    }
  }

  /**
   * Query the time server for the current skew.
   * @return an object containing the clock skew and the latency in the request
   * @throws SocketTimeoutException if there was a timeout waiting for a response from the server
   * @throws IOException if there was an IO error writing or reading from the socket.
   */
  public SkewData checkSkew() throws SocketTimeoutException, IOException
  {
    TimeRequest request = new TimeRequest(++packetId, useNanoTime);
    byte[] buf = request.toByteArray();
    DatagramPacket packet = new DatagramPacket(buf, buf.length, address, this.serverPort);
    long localStartTime = getLocalTime();
    
    // Don't proceed if JVM has been corrupted
    if (SystemFailure.getFailure() != null) {
      // Allocate no objects here!
      DatagramSocket s = this.socket;
      if (s != null) {
        s.close();
      }
      SystemFailure.checkFailure(); // throws
    }
    
    // send request and get response
    this.socket.send(packet);
    buf = new byte[32];
    packet = new DatagramPacket(buf, buf.length);
    
    boolean receivedResponse = false;
    long endTime = System.currentTimeMillis() + socketTimeout;
    long serverTime = -1;
    
    while(!receivedResponse && (System.currentTimeMillis() < endTime))
    {
      try
      {
        this.socket.receive(packet);
      } catch(SocketTimeoutException e)
      {
        throw e;
      }

      TimeResponse serverData = TimeResponse.fromByteArray(packet.getData());

      receivedResponse = serverData.getPacketId() == this.packetId;
      serverTime = serverData.getTimeStamp();
    }
    
    if(!receivedResponse)
    {
      throw new SocketTimeoutException();
    }

    long localEndTime = getLocalTime();

    return new SkewData(localStartTime, localEndTime, serverTime);
  }

  private long getLocalTime()
  {
    if(this.useNanoTime)
    {
      return NanoTimer.getTime();
    }
    else
    {
      return System.currentTimeMillis();
    }
  }

  /**
   * Close the listening socket
   *
   */
  public void close()
  {
    this.socket.close();
  }

  public static class SkewData
  {
    private final long localStartTime;
    private final long localEndTime;
    private final long serverTime;

    public SkewData(final long localStartTime, final long localEndTime, final long serverTime) {
      this.localStartTime = localStartTime;
      this.localEndTime = localEndTime;
      this.serverTime = serverTime;
    }

    /** get the time that the request ended */
    public long getLocalEndTime() {
      return this.localEndTime;
    }
    /** get the time that the request started */
    public long getLocalStartTime() {
      return this.localStartTime;
    }
    /** get the time that the server reported */
    public long getServerTime() {
      return this.serverTime;
    }

    /** get the clock skew between the server and the client. The clock skew is 
     * calculated as client end time - server time - 1/2 round trip time. */
    public long getSkew()
    {
      return this.localEndTime - this.serverTime - getLatency();
    }

    /**
     * Get the latency in the request (approximated as 1/2 round trip time).
     * @return
     */
    public long getLatency()
    {
      return (this.localEndTime - this.localStartTime) / 2;
    }
  }
}
