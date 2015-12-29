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
package hydra;
import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketException;
import java.net.BindException;
import java.net.ServerSocket;
import java.rmi.server.RMISocketFactory;

/**
 * RMIHydraSocketFactory
 * has bigger backlog in ServerSocket and retries on Connection
 * refused.
 */
public class RMIHydraSocketFactory extends RMISocketFactory {
  /** Number of attempts to bind socket before giving up. */
  private static final int INITIAL_RETRY_COUNT = 120;
  /** Time in milliseconds between retries. */
  private static final long THROTTLE_PERIOD = 1000;
  public Socket createSocket(String host, int port) throws IOException
  {
    int retryCount = INITIAL_RETRY_COUNT;
    do {
      retryCount--;
      try {
        return new Socket(host, port);
      } catch (SocketException se) {
        //IBM J9 sometimes reports "listen failed" instead of BindException
        //see bug #40589
        final String msg = se.getMessage();
        boolean reattempt = se instanceof BindException || 
                            se instanceof ConnectException || 
                            (msg != null && 
                             msg.contains("Invalid argument: listen failed"));
 
        if (!reattempt || retryCount <= 0) {
          throw se;
        } else {
          try {Thread.sleep(THROTTLE_PERIOD);} catch (InterruptedException ie) {
            retryCount = 0;
            Thread.currentThread().interrupt();
          }
        }
      }
    } while (true);
  }

  public ServerSocket createServerSocket(int port) throws IOException
  {
    return new ServerSocket(port, 255);
  }
}
