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
import java.util.Calendar;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by IntelliJ IDEA. User: jblum Date: 11/14/11 Time: 3:58 PM To change this template use File | Settings | File
 * Templates.
 */
public class MessageClient extends AbstractClientServerSupport {

  private static final int DEFAULT_MESSAGE_COUNT = 10000;
  private static final int DEFAULT_MESSAGE_THREADS = 8;
  private static final int DEFAULT_PAYLOAD_SIZE = 10000;
  private static final int DEFAULT_POOl_SIZE = 2;

  private static int messageCount = DEFAULT_MESSAGE_COUNT;
  private static int messageThreads = DEFAULT_MESSAGE_THREADS;
  private static int payloadSize = DEFAULT_PAYLOAD_SIZE;
  private static int poolSize = DEFAULT_POOl_SIZE;

  private static byte[] payload;

  private static SocketFactory socketFactory;

  private static SocketFactoryType socketFactoryType = SocketFactoryType.NEW_SOCKET;

  public static void main(final String... args) throws Exception {
    init(args);

    final long t0 = System.currentTimeMillis();

    ThreadCollection.spawn(messageThreads, createMessageClientRunnable(), "Message Client Thread").join();

    final long t1 = System.currentTimeMillis();

    System.out.printf("Ran (%1$d) messaging client each sending (%2$d) messages having a payload of (%3$d) bytes using (%4$s) socket connections in (%5$d) milliseconds.%n",
      messageThreads, messageCount, payloadSize, socketFactoryType.equals(SocketFactoryType.NEW_SOCKET) ? "unlimited" : String.valueOf(poolSize), (t1 - t0));
  }

  private static void init(final String... args) throws Exception {
    customParseCommandLineArguments(args);
    initPayload();
    initSocketFactory();
  }

  private static void customParseCommandLineArguments(final String... args) throws Exception {
    parseCommandLineArguments(true, args);

    for (int index = 0; index < args.length; index++) {
      if ("messageCount".equalsIgnoreCase(args[index])) {
        messageCount = Integer.parseInt(args[++index]);
      }
      else if ("messageThreads".equalsIgnoreCase(args[index])) {
        messageThreads = Integer.parseInt(args[++index]);
      }
      else if ("payloadSize".equalsIgnoreCase(args[index])) {
        payloadSize = Integer.parseInt(args[++index]);
      }
      else if ("poolSize".equalsIgnoreCase(args[index])) {
        poolSize = Integer.parseInt(args[++index]);
      }
      else if ("socketFactoryType".equalsIgnoreCase(args[index])) {
        socketFactoryType = SocketFactoryType.findBy(Integer.parseInt(args[++index]));
      }
      else {
        System.err.printf("Ignoring command line argument (%1$s)!%n", args[index]);
      }
    }
  }

  protected static void initPayload() {
    final Random random = new Random(Calendar.getInstance().getTimeInMillis());

    payload = new byte[payloadSize];

    for (int index = 0; index < payload.length; index++) {
      payload[index] = (byte) random.nextInt(64);
    }
  }

  private static void initSocketFactory() throws IOException {
    switch (socketFactoryType) {
      case POOLED_SOCKET:
        socketFactory = new PooledSocketFactory(poolSize);
        break;
      case NEW_SOCKET:
      default:
        socketFactory = new NewSocketFactory();
    }

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override public void run() {
        socketFactory.destroy();
      }
    });
  }

  protected static Runnable createMessageClientRunnable() {
    return new Runnable() {
      public void run() {
        final int logCount = Math.max(messageCount / 10, 1);

        try {
          for (int count = 0; count < messageCount; count++) {
            final Socket clientSocket = socketFactory.getSocket();

            final DataInput in = getInputStream(clientSocket);
            final DataOutput out = getOutputStream(clientSocket);

            out.writeInt(payloadSize);
            out.write(payload);
            in.readByte();

            if (count % logCount == 0) {
              System.out.printf("(%1$s) sent (%2$s) messages...%n", Thread.currentThread().getName(), count);
            }

            socketFactory.releaseSocket(clientSocket);
          }
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  protected static enum SocketFactoryType {
    NEW_SOCKET(1, "newsock", "Create New Socket for Each Request"),
    POOLED_SOCKET(2, "poolsock", "Use Pooled Socket for Each Request");

    private final int id;

    private final String code;
    private final String description;

    SocketFactoryType(final int id, final String code, final String description) {
      this.id = id;
      this.code = code;
      this.description = description;
    }

    public static SocketFactoryType findBy(final int id) {
      for (SocketFactoryType strategy : values()) {
        if (strategy.getId() == id) {
          return strategy;
        }
      }

      throw new IllegalArgumentException("(" + id + ") is not a valid SocketFactoryType ID!");
    }

    public static SocketFactoryType findBy(final String code) {
      for (SocketFactoryType strategy : values()) {
        if (strategy.getCode().equals(code)) {
          return strategy;
        }
      }

      throw new IllegalArgumentException("(" + code + ") is not a valid SocketFactoryType code!");
    }

    public String getCode() {
      return code;
    }

    public String getDescription() {
      return description;
    }

    public int getId() {
      return id;
    }

    @Override
    public String toString() {
      return getDescription();
    }
  }

  protected static interface SocketFactory {

    public void destroy();

    public Socket getSocket() throws IOException;

    public SocketFactoryType getType();

    public void releaseSocket(Socket socket) throws IOException;

  }

  protected static final class NewSocketFactory implements SocketFactory {

    public void destroy() {
    }

    public Socket getSocket() throws IOException {
      return openSocket(getServerAddress(), getPort(), true);
    }

    public SocketFactoryType getType() {
      return SocketFactoryType.NEW_SOCKET;
    }

    public void releaseSocket(final Socket socket) {
      close(socket);
    }
  }

  protected static final class PooledSocketFactory implements SocketFactory {

    private final BlockingQueue<Socket> socketPool;

    public PooledSocketFactory(int numberOfConnections) throws IOException {
      socketPool = new ArrayBlockingQueue<Socket>(numberOfConnections, true);

      while (numberOfConnections-- > 0) {
        socketPool.add(openSocket(getServerAddress(), getPort(), true));
      }
    }

    public void destroy() {
      for (Socket socket = socketPool.poll(); socket != null; socket = socketPool.poll()) {
        close(socket);
      }
    }

    public Socket getSocket() throws IOException {
      try {
        return socketPool.take();
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    public SocketFactoryType getType() {
      return SocketFactoryType.POOLED_SOCKET;
    }

    public void releaseSocket(final Socket socket) throws IOException {
      socketPool.add(socket);
    }
  }

}
