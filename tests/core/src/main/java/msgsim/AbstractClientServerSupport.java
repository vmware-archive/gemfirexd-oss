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
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The AbstractClientServerSupport class is a base class for supporting the creation of both Client and Server programs
 * by maintaining operations and methods common to both.
 * <p/>
 * @author John Blum
 */
public abstract class AbstractClientServerSupport {

  protected static final boolean DEFAULT_DEBUG = false;
  protected static final boolean DEFAULT_RUNNING = true;

  private static boolean debug = DEFAULT_DEBUG;
  private static volatile boolean running = DEFAULT_RUNNING;

  protected static final int DEFAULT_PORT = 10235;

  private static int port = DEFAULT_PORT;

  private static InetAddress serverAddress;

  protected static final ThreadGroup mainThreadGroup = new ThreadGroup("Main Thread Group") {
    @Override public void uncaughtException(final Thread t, final Throwable e) {
      System.err.printf("Uncaught Exception in Thread (%1$s):%n", t.getName());
      e.printStackTrace(System.err);
      super.uncaughtException(t, e);
    }
  };

  protected static boolean isDebug() {
    return debug;
  }

  protected static DataInput getInputStream(final Socket socket) throws IOException {
    assert socket != null : "The Socket to read from cannot be null!";
    return new DataInputStream(socket.getInputStream());
  }

  protected static DataOutput getOutputStream(final Socket socket)  throws IOException {
    assert socket != null : "The Socket to write to cannot be null!";
    return new DataOutputStream(socket.getOutputStream());
  }

  protected static int getPort() {
    return port;
  }

  protected static boolean isRunning() {
    return running;
  }

  protected static void setRunning(final boolean running) {
    AbstractClientServerSupport.running = running;
  }

  protected static InetAddress getServerAddress() {
    return serverAddress;
  }

  protected static boolean close(final ServerSocket serverSocket) {
    if (serverSocket != null) {
      try {
        serverSocket.close();
        return true;
      }
      catch (IOException ignore) {
        return false;
      }
    }

    return false;
  }

  protected static boolean close(final Socket socket) {
    if (socket != null) {
      try {
        socket.close();
        return true;
      }
      catch (IOException ignore) {
        return false;
      }
    }

    return false;
  }

  protected static ExecutorService createExecutor(final String threadName, final boolean daemon) {
    return Executors.newCachedThreadPool(new ThreadFactory() {
      final AtomicLong threadNumber = new AtomicLong(0);
      public Thread newThread(final Runnable command) {
        final Thread thread = new Thread(mainThreadGroup, command, threadName + " " + threadNumber.getAndIncrement());
        thread.setDaemon(daemon);
        return thread;
      }
    });
  }

  protected static String formatMessage(final String message, final Object... arguments) {
    return MessageFormat.format(message, arguments);
  }

  protected static ServerSocket openServerSocket(final InetAddress address, final int port, final boolean reuseAddress, final int socketTimeout)
    throws IOException
  {
    final ServerSocket serverSocket = new ServerSocket();
    serverSocket.setReuseAddress(reuseAddress);
    serverSocket.setSoTimeout(socketTimeout);
    serverSocket.bind(new InetSocketAddress(address, port));
    return serverSocket;
  }

  protected static Socket openSocket(final InetAddress address, final int port, final boolean tcpNoDelay)
    throws IOException
  {
    final Socket socket = new Socket();
    socket.setTcpNoDelay(tcpNoDelay);
    socket.connect(new InetSocketAddress(address, port));
    return socket;
  }

  protected static void parseCommandLineArguments(final String... args) throws Exception {
    parseCommandLineArguments(false, args);
  }

  protected static void parseCommandLineArguments(final boolean ignoreUnknownCommands, final String... args) throws Exception {
    assert args != null : "The command line arguments should not be null!";

    for (int index = 0; index < args.length; index++) {
      if ("server".equals(args[index])) {
        serverAddress = InetAddress.getByName(args[++index]);
      }
      else if ("port".equals(args[index])) {
        port = Integer.parseInt(args[++index]);
      }
      else if ("debug".equals(args[index])) {
        debug = true;
      }
      else {
        if (!ignoreUnknownCommands) {
          System.err.printf("Unknown command line argument (%1$s)!%n", args[index]);
        }
      }
    }
  }

  protected static boolean pause(final int milliseconds) {
    try {
      Thread.sleep(milliseconds);
      return true;
    }
    catch (InterruptedException ignore) {
      return false;
    }
  }

  protected static void startServer(final String serverName, final ClientRequestHandler handler) {
    final Runnable serverRunnable = new Runnable() {

      private Executor executor = createExecutor("Client Request Handler Thread", true);

      private ServerSocket serverSocket;

      public void run() {
        try {
          serverSocket = openServerSocket(InetAddress.getLocalHost(), getPort(), true, 500);

          System.out.printf("Started %1$s on port (%2$d) @ address (%3$s)...%n", serverName, getPort(),
            serverSocket.getInetAddress().toString());

          while (isRunning()) {
            try {
              final Socket clientSocket = serverSocket.accept();
              clientSocket.setTcpNoDelay(true);
              executor.execute(handler.service(clientSocket));
            }
            catch (SocketTimeoutException ignore) {
            }
          }
        }
        catch (IOException e) {
          e.printStackTrace(System.err);
          setRunning(false);
        }
        finally {
          close(serverSocket);
          System.out.printf("%1$s on port (%2$d) @ address (%3$s) stopped.%n", serverName, getPort(),
            serverSocket.getInetAddress().toString());
        }
      }
    };

    final Thread serverThread = new Thread(mainThreadGroup, serverRunnable, serverName + " Thread");
    serverThread.setDaemon(false);
    serverThread.start();
  }

  protected static void waitForUserInput(final String input, final String message) {
    final Scanner in = new Scanner(System.in);

    while (!input.equalsIgnoreCase(in.nextLine().trim())) {
      System.err.println(message);
    }
  }

  protected static interface ClientRequestHandler {

    public Runnable service(Socket clientSocket);

  }

  protected static final class ThreadCollection {

    private final Collection<Thread> threads = new LinkedList<Thread>();

    private ThreadCollection() {
    }

    public static ThreadCollection spawn(int numberOfThreads, final Runnable runner, final String threadBasename) {
      final ThreadCollection collection = new ThreadCollection();
      while (numberOfThreads-- > 0) {
        collection.startThread(runner, threadBasename);
      }
      return collection;
    }

    private Thread startThread(final Runnable runnable, final String threadBasename) {
      final Thread thread = createThread(runnable, threadBasename);
      thread.start();
      threads.add(thread);
      return thread;
    }

    private Thread createThread(final Runnable runnable, final String threadBasename) {
      assert threadBasename != null : "The basename of the Thread cannot be null!";
      final Thread thread = new Thread(runnable, threadBasename.trim() + " " + threads.size());
      thread.setDaemon(false);
      return thread;
    }

    public void join() throws InterruptedException {
      for (final Thread thread : threads) {
        thread.join();
      }
    }
  }

}
