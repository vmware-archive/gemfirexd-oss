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
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;

/**
 * The ExchangeTest class tests what happens when a client sends a message, the server receives the message and
 * acknowledges it (aka sends an ACK), however, the client discards the ACK and proceeds to send another message
 * for which the server will replay again, only this time, the client expects a response from the server...
 * What will the client read given the client never read the ACK response from the server on the same socket?
 * <p/>
 * @author John Blum
 */
public class ExchangeTest extends AbstractClientServerSupport {

  private static final CountDownLatch latch = new CountDownLatch(1);

  public static void main(final String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("usage: java msgsim.ExhangeTest <port-number>");
      System.exit(1);
    }

    final int port = Integer.parseInt(args[0]);

    runServer(port);
    latch.await();

    final Socket socket = openSocket(InetAddress.getLocalHost(), port, true);

    final DataInput in = getInputStream(socket);
    final DataOutput out = getOutputStream(socket);

    //runClientMsgComm(in, out);
    runClientNumberComm(in, out);

    close(socket);
  }

  private static void runClientMsgComm(final DataInput in, final DataOutput out) throws IOException {
    System.out.println("client says 'hello'");

    out.writeBytes("hello\n");
    out.writeBytes("I say po-ta-toe, you say...\n");

    String value = in.readLine();

    System.out.printf("client: server said '%1$s'%n", value);
  }

  private static void runClientNumberComm(final DataInput in, final DataOutput out) throws IOException {
    System.out.println("client: sending 1...");
    out.writeByte(1);
    System.out.println("client: receiving 2...");
    in.readByte();
    System.out.println("client: sending 3...");
    out.writeByte(3);
    System.out.println("client: receiving (byte) 4...");
    System.out.printf("client: %1$d%n", in.readByte());
    System.out.println("client: sending 5...");
    out.writeByte(5);
    System.out.println("client: receiving (byte) 6...");
    System.out.printf("client: %1$d%n", in.readByte());
    System.out.printf("client: %1$d%n", in.readByte());
    System.out.printf("client: %1$d%n", in.readByte());
    System.out.printf("client: %1$d%n", in.readByte());
  }

  private static void runServer(final int port) {
    final Runnable serverThreadRunnable = new Runnable() {
      public void run() {
        ServerSocket serverSocket = null;

        try {
          serverSocket = openServerSocket(InetAddress.getLocalHost(), port, true, 0);
          latch.countDown();

          final Socket clientSocket = serverSocket.accept();
          clientSocket.setTcpNoDelay(true);

          final DataInput in = getInputStream(clientSocket);
          final DataOutput out = getOutputStream(clientSocket);

          //runServerMsgComm(in, out);
          runServerNumberComm(in, out);
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
        finally {
          close(serverSocket);
        }
      }

      private void runServerMsgComm(final DataInput in, final DataOutput out) throws IOException {
        String value = in.readLine();

        System.out.printf("server: client said '%1$s'%n", value);
        System.out.println("server is acknowledging");

        out.writeBytes("ACK\n");

        value = in.readLine();

        System.out.printf("server: client said '%1$s'%n", value);

        out.writeBytes("potato\n");
      }

      private void runServerNumberComm(final DataInput in, final DataOutput out)  throws IOException {
        System.out.println("server: receiving 1...");
        in.readByte();
        System.out.println("server: sending 2...");
        out.writeByte(2);
        System.out.println("server: receiving 3...");
        in.readByte();
        System.out.println("server: sending (int) 4...");
        out.writeInt(4);
        System.out.println("server: receiving 5...");
        in.readByte();
        System.out.println("server: sending 6...");
        out.writeByte(6);
      }
    };

    final Thread serverThread = new Thread(mainThreadGroup, serverThreadRunnable, "Server Thread");
    serverThread.setDaemon(true);
    serverThread.start();
  }

}
