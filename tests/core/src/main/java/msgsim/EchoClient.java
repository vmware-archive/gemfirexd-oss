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
package msgsim;

import java.io.DataInput;
import java.io.DataOutput;
import java.net.Socket;
import java.util.Scanner;

/**
 * @author John Blum
 */
public class EchoClient extends AbstractClientServerSupport {

  public static void main(final String... args) throws Exception {
    parseCommandLineArguments(args);

    assert (getServerAddress() != null && getServerAddress().isReachable(5000)) :
      formatMessage("The server address ({0}) cannot be null and must be reachable!", getServerAddress());

    final Socket clientSocket = openSocket(getServerAddress(), getPort(), true);

    assert (clientSocket != null && clientSocket.isConnected()) :
      formatMessage("Failed to connect to Echo Server @ ({0})) on port ({1,number,integer})!",
        getServerAddress(), getPort());

    System.out.printf("Connected to Echo Server @ (%1$s) on port (%2$d)...%n", getServerAddress().toString(), getPort());

    final DataInput in = getInputStream(clientSocket);
    final DataOutput out = getOutputStream(clientSocket);

    final Scanner userIn = new Scanner(System.in);

    String line;

    try {
      while (!"exit".equalsIgnoreCase(line = userIn.nextLine().trim())) {
        System.out.printf("Client says... \"%1$s\"%n", line);
        out.writeBytes(line + "\n");
        System.out.printf("Server says... \"%1$s\"%n", in.readLine());
      }
    }
    finally {
      close(clientSocket);
    }
  }

}
