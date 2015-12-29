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

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.NanoTimer;

import hydra.HostHelper;
import hydra.Log;

import java.io.*;
import java.net.*;

/**
 * A thread that serves up the time on localhost using a random port.
 */
public class TimeServer extends Thread {

  private final String host;
  private final int port;
  protected final DatagramSocket socket;

  public TimeServer() throws IOException {
    this("TimeServer");
  }
  
  public TimeServer(String name) throws IOException {
    super(name);
    host = HostHelper.getCanonicalHostName();
    port = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    socket = new DatagramSocket(port);
  }
  
  public String getHost() {
    return this.host;
  }
  
  public int getPort() {
    return this.port;
  }
  
  public String toString() {
    return host + ":" + port + "(" + socket + ")";
  }
  
  public void run() {
    for (int i = 0; i < 20000; i++) {
      NanoTimer.getTime();
    }

    while (true) {
      try {
        byte[] buf = new byte[32];

        // receive a request
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        socket.receive(packet);
        
        // In the unlikely case that this JVM has been corrupted,
        // don't let the time server respond to this request...
        if (SystemFailure.getFailure() != null) {
          // Allocate no objects here!
          DatagramSocket s = socket;
          if (s != null) {
            s.close();
          }
          SystemFailure.checkFailure(); // close
        }
        
        InetAddress address = packet.getAddress();
        int srcport = packet.getPort();
        TimeRequest request = TimeRequest.fromByteArray(packet.getData());

        long time = request.isNanoTime() ? NanoTimer.getTime() : System.currentTimeMillis();
        TimeResponse response = new TimeResponse(request.getPacketId(), time);
        
        buf = response.toByteArray();

        // send the response to the client
        packet = new DatagramPacket(buf, buf.length, address, srcport);
        socket.send(packet);

      } catch (IOException e) {
        Log.getLogWriter().error("Error in time server", e);
        socket.close();
        return;
      }
    }
  }
}
