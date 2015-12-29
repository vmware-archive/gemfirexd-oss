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
import java.io.IOException;
import java.net.Socket;

public class MessageServer extends AbstractClientServerSupport {

  public static void main(final String... args) throws Exception {
    parseCommandLineArguments(args);

    startServer("Message Server", new ClientRequestHandler() {
      public Runnable service(final Socket clientSocket) {
        return createMessageClientRunnable(clientSocket);
      }
    });

    waitForUserInput("exit", "Please enter 'exit' to stop the Message Server.");
    System.out.println("Exiting...");
    setRunning(false);
  }

  private static Runnable createMessageClientRunnable(final Socket clientSocket) {
    return new Runnable() {
      public void run() {
        try {
          final DataInput in = getInputStream(clientSocket);
          final DataOutput out = getOutputStream(clientSocket);

          boolean reading = true;

          while (reading) {
            try {
              final int payloadSize = in.readInt();

              if (isDebug()) {
                System.out.printf("Reading (%1$d) bytes from client (%2$s)...%n", payloadSize,
                  clientSocket.getInetAddress().toString());
              }

              in.readFully(new byte[payloadSize]);

              if (isDebug()) {
                System.out.println("Sending ack...");
              }

              out.writeByte(1);
            }
            catch (IOException ignore) {
              reading = false;
            }
          }
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
        finally {
          close(clientSocket);
        }
      }
    };
  }

}
