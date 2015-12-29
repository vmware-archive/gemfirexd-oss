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
package hydra.training;

import hydra.Log;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import util.TestException;

/**
 * A client that connects to a server and verifies that it sends the
 * correct data.
 */
public class Client {

  /**
   * Creates a new client
   */
  public Client() {

  }

  /**
   * Connects to a server on a given port and verified that the server
   * sends appropriate data.
   *
   * @throws IOException
   *         If something goes wrong while connecting or communicating
   *         with the server.
   */
  public void connect(int port) throws IOException {
    Socket socket = new Socket(InetAddress.getLocalHost(), port);

    if (ServerPrms.debug()) {
      String s = "Connected to server on " + socket;
      Log.getLogWriter().info(s);
    }

    // First, write a head int
    DataOutputStream dos =
      new DataOutputStream(socket.getOutputStream());
    dos.writeInt(this.getHeaderInt());

    DataInputStream dis =
      new DataInputStream(socket.getInputStream());
    try {
      for (int i = 0; i < 1000; i++) {
        int data = dis.readInt();
        if (data != i) {
          String s = "Expected " + i + " from server, got " + data;
          throw new TestException(s);
        }
        processData(data);
      }

    } finally {
      dos.close();
      dis.close();
      socket.close();
    }
  }

  /**
   * Returns the header <code>int</code> that is written by the client
   * when it first connects.  This allows the server to distinguish
   * between a regular client and a "shutdown" client.
   */
  protected int getHeaderInt() {
    return 42;
  }

  /**
   * Processes a piece of data that was read from the server
   */
  protected void processData(int data) {
    // By default, do nothing
  }

}
