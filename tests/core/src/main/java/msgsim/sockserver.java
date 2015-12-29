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
import java.io.EOFException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * msgsim.sockserver -DnumServers=xxx [server nn] [scenario xx] [scenarioThreads yy] [nio]
 *
 * scenarios are:<br>
 * 1: conserve-sockets=true, no listeners<br>
 * 2: conserve-sockets=false, ditto<br>
 * 3: conserve-sockets=false with listeners that send messages to other servers
 * 99: tell servers to exit
 *
 * @author Bruce Schuchardt
 * @author John Blum
 */
@SuppressWarnings("synthetic-access")
public class sockserver implements commands {

  /** Determines whether information logged at DEBUG will be output to Standard Out. */
  public static final boolean DEBUG = Boolean.getBoolean("debugServer");

  /**
   * The default is to have 3 servers running, including the one driving the messaging scenario.  Each JVM must be given
   * a different server number and all servers must be present when the messaging scenario is started.
   * Start servers 0-(n-2) first and then start server (n-1) with a messaging scenario (based on number)
   * to drive the scenario.
   */
  private static final int NUMBER_OF_SERVERS = Integer.getInteger("numServers", 3);

  private static final InetAddress[] serverAddresses = new InetAddress[NUMBER_OF_SERVERS];

  static {
    final String commaDelimitedServerAddresses = System.getProperty("serverAddr", null);

    try {
      if (commaDelimitedServerAddresses == null || "".equals(commaDelimitedServerAddresses.trim())) {
        for (int index = 0; index < serverAddresses.length; index++) {
          serverAddresses[index] = InetAddress.getLocalHost();
        }
      }
      else {
        final String[] serverAddresses = commaDelimitedServerAddresses.split(",");

        assert serverAddresses.length == NUMBER_OF_SERVERS : "The number of server addresses must match the number of servers"
          + " specified with the numServers System property!";

        for (int index = 0; index < serverAddresses.length; index++) {
          sockserver.serverAddresses[index] = InetAddress.getByName(serverAddresses[index]);
        }
      }
    }
    catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  /** each server has a sequential, unique number starting with zero */
  // TODO this class member variable should be an instance variable
  private static int serverNumber = -1;

  /**
   * reply processor simulation.  We gather the number of responses in the int[] and the message sender waits for this
   * to reach the expected count.
   */
  private static final AtomicInteger ProcessorId = new AtomicInteger(1);

  private static final Map<Integer, int[]> ProcessorIdHolder = new ConcurrentHashMap<Integer, int[]>();

  /**
   * pool of socket queues.  New sockets are created if a queue becomes exhausted.  Sockets must be returned
   * to the pool after a thread is done with them.
   */
  private static final ConnectionPool connectionPool = new QueuedConnectionsPool(NUMBER_OF_SERVERS);

  /**
   * shared connections to other servers.  This is used for conserve-sockets=true simulation to send commands
   * and responses
   */
  private static final Socket[] sharedConnections = new Socket[NUMBER_OF_SERVERS];

  /**
   * shared ACK connections to other servers.  This is used for conserve-sockets=true simulation to send commands
   * and responses.
   */
  private static final Socket[] sharedUndorderedConnections = new Socket[NUMBER_OF_SERVERS];

  /** unshared connections to other servers */
  private static final ThreadLocal<Socket[]> threadOwnedConnections = new ThreadLocal<Socket[]>() {
    @Override protected Socket[] initialValue() {
      return new Socket[NUMBER_OF_SERVERS];
    }
  };

  /** unshared ACK connections to other servers */
  private static final ThreadLocal<Socket[]> threadOwnedUnorderedConnections = new ThreadLocal<Socket[]>() {
    @Override protected Socket[] initialValue() {
      return new Socket[NUMBER_OF_SERVERS];
    }
  };

  /**
   * this can be used to tell if a thread is a "reader" thread that accepts commands from another server
   */
  private static final ThreadLocal<Boolean> isReaderThread = new ThreadLocal<Boolean>() {
    @Override protected Boolean initialValue() {
      return Boolean.FALSE;
    }
  };

  private volatile boolean exit = false;  // flag for the acceptor thread to terminate

  private boolean handOffListeners = false; // true if we hand off connections for cs=true before replying
  private boolean nio = false;
  private boolean preferPooledSockets = false; // use the queued connection pool (see QueuedConnectionPool)
  private boolean preferThreadOwnedAckSockets = false; // this isn't in GemFire at this point - prefer thread-owned unordered sockets
  private boolean preferThreadOwnedSockets = false; // true if conserve-sockets=false is being simulated

  /**
   * The messages sent in the simulation can have a byte-array payload. Set the "payloadSize" System property
   * to the length you want the byte array to be.
   */
  private byte[] payload = new byte[Integer.getInteger("payloadSize", 0)]; // the optional message payload

  /** how many scenario threads should be created */
  private int scenarioThreads = 1;

  /** handler threads execute connection runnables.  This currently is implemented as a non-serial executor. */
  private Executor handlers;

  /** the scenario that this server is executing */
  private MessagingScenario scenario = MessagingScenario.SERVER;

  /**
   * each server has a single server thread accepting connection requests from other servers and schedules
   * their execution in the "handlers" pool.
   */
  private Thread serverThread;

  /**
   * All threads should be in this thread group so we don't miss exceptions
   */
  private ThreadGroup handlerGroup = new ThreadGroup("Handler Group") {
    @Override
    public void uncaughtException(final Thread t, final Throwable e) {
      System.err.println("Unhandled exception in thread " + t.getName() + ":");
      e.printStackTrace(System.err);
      super.uncaughtException(t, e);
    }
  };

  /**
   * Creates a SocketAddress based on the specified IP address and service port number.
   * <p/>
   * @param address
   * @param port
   * @return
   */
  private static SocketAddress createSocketAddress(final InetAddress address, final int port) {
    return new InetSocketAddress(address, port);
  }

  /**
   * Creates a SocketAddress for the specified server indicated by the server's number (index).
   * <p/>
   * @param serverNumber
   * @return
   */
  private static SocketAddress createSocketAddress(final int serverNumber) {
    assert serverAddresses != null : "The server addresses were not properly initialized!";
    assert serverNumber >= 0 && serverNumber < NUMBER_OF_SERVERS : "The server number (" + serverNumber
      + ") must be between 0 inclusive and " + NUMBER_OF_SERVERS + "exclusive!";
    return createSocketAddress(serverAddresses[serverNumber], BASE_PORT + serverNumber);
  }

  /**
   * Utility method to get a binary input stream to read from the given Socket.
   */
  private static DataInput getInputStream(final Socket socket) throws IOException {
    return new DataInputStream(socket.getInputStream());
  }

  /**
   * Utility method to get a binary output stream to write to the given Socket.
   */
  private static DataOutput getOutputStream(final Socket socket) throws IOException {
    return new DataOutputStream(socket.getOutputStream());
  }

  /**
   * Causes the current Thread to wait for the specified Thread to complete it's function.
   */
  private static void join(final Thread thread) {
    try {
      thread.join();
    }
    catch (InterruptedException ignore) {
    }
  }

  /**
   * Writes the specified message to Standard Out only if the 'debugServer' System property is set.
   */
  private static void logForDebugging(final String message) {
    if (DEBUG) {
      logForInfo(message);
    }
  }

  /**
   * Writes the message and stack trace of the Throwable object to Standard Out.
   */
  private static void logForError(final String message, final Throwable e) {
    final StringWriter writer = new StringWriter(500);
    writer.append(message);
    writer.append(System.getProperty("line.separator"));
    e.printStackTrace(new PrintWriter(writer));
    logForInfo(writer.toString());
  }

  /**
   * Writes the specified message to Standard Out.
   */
  private static void logForInfo(final String message) {
    System.out.printf("Server %1$d [%2$s]: %3$s%n", serverNumber, Thread.currentThread().getName(), message);
  }

  /**
   * Writes the byte array of data to the output stream if the data reference is not null and actually contains data
   * (bytes).
   */
  private static void writeBytes(final DataOutput out, final byte[] data) throws IOException {
    if (data != null && data.length > 0) {
      out.write(data);
    }
  }

  /**
   * Bootstrap method for launching the sockserver application and running various messaging scenarios common in
   * GemFire.
   * <p/>
   * @param args a String array containing the program command line arguments.
   * @throws Exception if the sockserver program fails to function accordingly.
   */
  public static void main(final String[] args) throws Exception {
    final sockserver server = new sockserver();

    server.parseCommandLineArguments(args);
    server.validateServerNumber();

    if (!server.scenario.isExit()) {
      server.serve();
    }

    server.runScenario();
  }

  /**
   * Default package-private constructor for instantiating an instance of the sockserver class and running one of the
   * user-specified scenarios.
   */
  private sockserver() {
    handlers = Executors.newCachedThreadPool(new ThreadFactory() {
      private AtomicLong threadNumber = new AtomicLong(0);
      public Thread newThread(final Runnable runnable) {
        final Thread thread = new Thread(handlerGroup, runnable, "Handler Thread " + threadNumber.getAndIncrement());
        thread.setDaemon(true);
        return thread;
      }
    });
  }

  private void parseCommandLineArguments(final String[] args) {
    for (int index = 0; index < args.length; index++) {
      if ("nio".equals(args[index])) {
        nio = true;
      }
      else if ("scenario".equals(args[index])) {
        scenario = MessagingScenario.valueOf(Integer.parseInt(args[++index]));
      }
      else if ("scenarioThreads".equals(args[index])) {
        scenarioThreads = Integer.parseInt(args[++index]);
      }
      else if ("server".equals(args[index])) {
        serverNumber = Integer.parseInt(args[++index]);
      }
      else {
        System.err.println("Unknown command line argument (" + args[index] + ")!");
      }
    }
  }

  private void validateServerNumber() {
    assert (serverNumber >= 0 & serverNumber < NUMBER_OF_SERVERS) : "The server number (" + serverNumber
      + ") is not valid; the server number should be between 0 inclusive and " + NUMBER_OF_SERVERS + " exclusive!";
  }
  /**
   * This starts the server thread to listen for and accept peer member connections.  The server thread owns
   * the server socket for this server and schedules reader threads in the handlers pool.
   */
  private void serve() {
    final Runnable serverThreadRunnable = new Runnable() {
      public void run() {
        try {
          final ServerSocket serverSocket = (nio ? ServerSocketChannel.open().socket() : new ServerSocket());
          serverSocket.setReuseAddress(true);
          serverSocket.bind(createSocketAddress(serverNumber));

          logForInfo("server (" + serverNumber + ") is listening for and accepting connections...");

          while (!exit) {
            final Socket clientSocket = serverSocket.accept();
            clientSocket.setTcpNoDelay(true);
            handlers.execute(createReaderThreadRunnable(clientSocket));
          }
        }
        catch (IOException e) {
          logForInfo("Server thread for server (" + serverNumber + ") threw an IOException: " + e.getMessage());
          e.printStackTrace();
        }
      }
    };

    serverThread = new Thread(handlerGroup, serverThreadRunnable, "Server Thread");
    serverThread.setDaemon(true);
    serverThread.start();
  }

  /** this is the receiving side of messaging - the "reader threads" */
  private Runnable createReaderThreadRunnable(final Socket clientSocket) {
    return new Runnable() {
      public void run() {
        try {
          isReaderThread.set(true);

          final DataInput in = getInputStream(clientSocket);
          final DataOutput out = getOutputStream(clientSocket);

          while (true) {
            final MessageProtocolCommand command = readCommand(in);

            logForDebugging("received command: " + command);

            if (command.isInvalid()) {
              return;
            }

            switch (command) {
              case INIT:
                logForInfo("Initializing server (" + serverNumber + ")...");
                preferThreadOwnedSockets = false;
                preferThreadOwnedAckSockets = false;
                preferPooledSockets = false;
                handOffListeners = false;
                out.writeByte(1);
                break;
              case HAND_OFF_LISTENERS:
                logForInfo("Setting handOffListeners to true...");
                handOffListeners = true;
                out.writeByte(1);
                break;
              case SET_PAYLOAD_SIZE:
                final int payloadSize = in.readInt();
                logForInfo("Setting payload size to (" + payloadSize + ") bytes...");
                payload = new byte[payloadSize];
                out.writeByte(1);
                break;
              case USE_THREAD_OWNED:
                logForInfo("Setting preference for thread-owned sockets...");
                preferThreadOwnedSockets = true;
                out.writeByte(1);
                break;
              case USE_THREAD_OWNED_UNORDERED:
                logForInfo("Setting preference for thread-owned sockets on ACK...");
                preferThreadOwnedAckSockets = true;
                out.writeByte(1);
                break;
              case USE_SHARED:
                logForInfo("Setting preference for shared sockets...");
                preferThreadOwnedSockets = false;
                out.writeByte(1);
                break;
              case USE_POOLED_CONNECTIONS:
                logForInfo("Setting preference for pooled sockets...");
                preferPooledSockets = true;
                connectionPool.prefill(4);
                out.writeByte(1);
                break;
              case NORMAL_MESSAGE: // process message from another server and ack it
                logForDebugging("Received message from (" + clientSocket.getInetAddress().toString() + ")...");
                processAndAck(in, clientSocket);
                logForDebugging("NORMAL_MESSAGE processing and acknowledgement complete.");
                break;
              case ACK_MESSAGE: // receive acknowledgement (ACK) from a reader thread in another server
                final int processorId = in.readInt();
                  logForDebugging("Received ACK for message with processorId (" + processorId + ") from ("
                  + clientSocket.getInetAddress() + ")...");
                int[] processor = ProcessorIdHolder.get(processorId);
                if (processor == null) {
                  logForDebugging("could not find processor for id (" + processorId + ")!");
                }
                else {
                  synchronized (processor) {
                    processor[0] += 1;
                    processor.notify();
                  }
                }
                break;
              case PRINT_QUEUE_SIZES:
                logForDebugging("Printing connection pool (queue) sizes...");
                connectionPool.logQueueSizes();
                out.write(1);
                break;
              case EXIT:
                logForInfo("exiting...");
                out.write(1);
                exit = true;
                System.exit(0);
                break;
              default:
                logForInfo("Unknown command: " + command);
            }
          }
        }
        catch (Exception e) {
          if (!clientSocket.isClosed()) {
            logForError("", e);
          }
          exit = true;
        }
        finally {
          isReaderThread.set(false);
        }
      }

      private MessageProtocolCommand readCommand(final DataInput in) throws IOException {
        try {
          return MessageProtocolCommand.valueOf(in.readByte());
        }
        catch (EOFException e) {
          return MessageProtocolCommand.INVALID;
        }
        catch (SocketException e) {
          if (e.getMessage().equals("Connection reset")) {
            return MessageProtocolCommand.INVALID;
          }
          throw e;
        }
      }

      @Override
      public String toString() {
        return MessageFormat.format("reader thread ({0}) connection @ ({1}:{2})", Thread.currentThread().getName(),
          clientSocket.getInetAddress().toString(), clientSocket.getPort());
      }
    };
  }

  /**
   * spawn threads to run the scenario in parallel and wait for them to finish
   */
  private void runScenario() throws Exception {
    logForInfo("running messaging scenario (" + scenario + ")...");

    switch (scenario) {
      case SERVER:
        break;
      case THREAD_OWNED_CONNECTIONS:
        initPayload();
        commandAll(MessageProtocolCommand.USE_THREAD_OWNED, false);
        break;
      case THREAD_OWNED_CONNECTIONS_WITH_DOMINO:
        initPayload();
        commandAll(MessageProtocolCommand.USE_THREAD_OWNED, false);
        break;
      case SHARED_CONNECTIONS:
        initPayload();
        commandAll(MessageProtocolCommand.USE_SHARED, false);
        break;
      case SHARED_CONNECTIONS_WITH_DOMINO:
        initPayload();
        commandAll(MessageProtocolCommand.USE_THREAD_OWNED_UNORDERED, false);
        break;
      case POOLED_CONNECTIONS:
        initPayload();
        commandAll(MessageProtocolCommand.USE_POOLED_CONNECTIONS, false);
        break;
      case POOLED_CONNECTIONS_WITH_DOMINO:
        initPayload();
        commandAll(MessageProtocolCommand.USE_POOLED_CONNECTIONS, false);
        break;
      case HANDOFF_LISTENERS:
        initPayload();
        commandAll(MessageProtocolCommand.USE_SHARED, false);
        commandAll(MessageProtocolCommand.HAND_OFF_LISTENERS, false);
        break;
      case HANDOFF_LISTENERS_WITH_DOMINO:
        initPayload();
        commandAll(MessageProtocolCommand.USE_THREAD_OWNED_UNORDERED, false);
        commandAll(MessageProtocolCommand.HAND_OFF_LISTENERS, false);
        break;
      case EXIT:
        commandAll(MessageProtocolCommand.EXIT, false);
        return;
      default:
        logForDebugging("unimplemented scenario: " + scenario);
        System.exit(1);
    }

    // Now, create each scenario thread and start it.  Afterward, we wait for the scenario threads to finish executing.
    ThreadCollection.spawn(scenarioThreads, createScenarioThreadRunnable(), "Messaging Thread").join();
    commandAll(MessageProtocolCommand.PRINT_QUEUE_SIZES, false);
    commandAll(MessageProtocolCommand.EXIT, false);
  }

  /** tell all the servers to initialize and then tell them what the current payload size is... */
  private void initPayload() throws Exception {
    commandAll(MessageProtocolCommand.INIT, false);
    commandAll(MessageProtocolCommand.SET_PAYLOAD_SIZE, false);
    sendToAll(payload.length, true);
  }

  /** send a command to all servers (including this one) w/o a payload or processorId */
  private void commandAll(final MessageProtocolCommand command, final boolean requireAck) throws Exception {
    final Socket[] serverConnections = getServerConnections(true);

    try {
      for (int serverNumber = 0; serverNumber < NUMBER_OF_SERVERS; serverNumber++) {
        if (!(serverConnections[serverNumber] == null || serverConnections[serverNumber].isClosed())) {
          try {
            synchronized (serverConnections[serverNumber]) {
              logForDebugging("sending " + command + " to server " + serverNumber + "...");
              getOutputStream(serverConnections[serverNumber]).writeByte(command.toByte());
              if (requireAck) {
                getInputStream(serverConnections[serverNumber]).readByte();
              }
            }
          }
          catch (SocketException e) {
            logForInfo("exception '" + e.getMessage() + "' reported for server " + serverNumber);
          }
        }
      }
    }
    finally {
      returnSockets(serverConnections);
    }
  }

  /** send a message with an integer payload */
  private void sendToAll(final int data, final boolean includeSelf) throws Exception {
    final Socket[] serverConnections = getServerConnections(includeSelf);

    try {
      for (int serverNumber = 0; serverNumber < NUMBER_OF_SERVERS; serverNumber++) {
        if (includeSelf || serverNumber != this.serverNumber) {
          try {
            logForDebugging("sending " + data + " to " + serverNumber);

            synchronized (serverConnections[serverNumber]) {
              getOutputStream(serverConnections[serverNumber]).writeInt(data);
              getInputStream(serverConnections[serverNumber]).readByte();
            }
          }
          catch (SocketException e) {
            logForInfo("exception '" + e.getMessage() + "' reported for server " + serverNumber);
          }
        }
      }
    }
    finally {
      returnSockets(serverConnections);
    }
  }

  private static final class ThreadCollection {

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

  /** these runnables are used in scenario threads.  They are not run in a thread pool */
  private Runnable createScenarioThreadRunnable() {
    return new Runnable() {
      public void run() {
        try {
          final int numOps = Integer.getInteger("numOps", 500000);
          switch (scenario) {
            case SERVER:
              join(serverThread);
              break;
            case THREAD_OWNED_CONNECTIONS:
              preferThreadOwnedSockets = true;
              distributedAckDriver(numOps, false);
              break;
            case THREAD_OWNED_CONNECTIONS_WITH_DOMINO:
              preferThreadOwnedSockets = true;
              distributedAckDriver(numOps, true);
              break;
            case SHARED_CONNECTIONS:
              preferThreadOwnedSockets = false;
              distributedAckDriver(numOps, false);
              break;
            case SHARED_CONNECTIONS_WITH_DOMINO: // same as SHARED_CONNECTIONS but other server threads get unshared ack sockets
              preferThreadOwnedSockets = false;
              distributedAckDriver(numOps, false);
              break;
            case POOLED_CONNECTIONS:
              preferPooledSockets = true;
              distributedAckDriver(numOps, false);
              break;
            case POOLED_CONNECTIONS_WITH_DOMINO:
              preferPooledSockets = true;
              distributedAckDriver(numOps, true);
              break;
            case HANDOFF_LISTENERS:
              preferThreadOwnedSockets = false; // use shared here but thread-owned in other servers
              handOffListeners = true;
              distributedAckDriver(numOps, false);
              break;
            case HANDOFF_LISTENERS_WITH_DOMINO:
              preferThreadOwnedSockets = false;
              handOffListeners = true;
              distributedAckDriver(numOps, false);
              break;
            default:
              logForDebugging("unimplemented scenario: " + scenario);
              System.exit(1);
          }
        }
        catch (Exception e) {
          logForDebugging("uncaught exception in scenario thread: " + e.getMessage());
          e.printStackTrace();
        }
      }
    };
  }

  private void distributedAckDriver(final int numberOfOperations, final boolean simulateListeners) throws Exception {
    final int logCount = Math.max(numberOfOperations / 10, 1); // avoid division by zero in modulus calculation below

    logForInfo("Sending (" + numberOfOperations + ") messages...");

    for (int count = 0; count < numberOfOperations; count++) {
      if (count % logCount == 0) {
        logForInfo("Completed (" + count + ") message operations...");
      }
      sendMessageAndWaitForAck(simulateListeners);
    }

    logForInfo("Completed all (" + numberOfOperations + ") message operations.");
  }

  /** basic message processing for reader threads */
  private void processAndAck(final DataInput in, final Socket socket) throws Exception {
    final int pid = in.readInt();
    final int serverId = in.readUnsignedByte();
    final boolean simulateListener = in.readBoolean();

    readPayload(in);

    final Runnable r = new Runnable() {
      public void run() {
        try {
          if (simulateListener) {
            sendMessageAndWaitForAck(false);
          }
          sendAck(serverId, pid, socket);
        }
        catch (Exception e) {
          throw new RuntimeException("error sending ack", e);
        }
      }
    };

    if (handOffListeners) {
      handlers.execute(r);
    }
    else {
      r.run();
    }
  }

  /** read a message payload transmitted on the given socket input stream */
  private void readPayload(final DataInput in) throws Exception {
    if (payload.length > 0) {
      logForDebugging("reading payload...");
      in.readFully(new byte[payload.length]);
    }
  }

  /**
   * send a message and wait for a response.  This will use a reply-processor simulation if we're simulating
   * conserve-sockets=true
   */
  private void sendMessageAndWaitForAck(final boolean simulateListener) throws Exception {
    final byte[] pbytes = new byte[6];

    pbytes[4] = (byte) serverNumber;
    pbytes[5] = (byte) (simulateListener ? 1 : 0);

    final Integer processorId = (preferPooledSockets || preferThreadOwnedSockets ? 0 : getProcessorId(pbytes));

    try {
      sendCommandAndWaitForAck(MessageProtocolCommand.NORMAL_MESSAGE, processorId, pbytes, payload);
    }
    finally {
      // clean up the processor object - it's no longer needed
      if (processorId > 0) {
        ProcessorIdHolder.remove(processorId);
      }
    }
  }

  /** get a reply processor ID and register it with the processor keeper so other threads can find it */
  private Integer getProcessorId(final byte[] pbytes) {
    assert !(pbytes == null || pbytes.length < 6) : "Expected a byte array of size 6!";
    final int processorId = ProcessorId.getAndIncrement();
    pbytes[0] = (byte) ((processorId / 0x1000000) & 0xff);
    pbytes[1] = (byte) ((processorId / 0x10000) & 0xff);
    pbytes[2] = (byte) ((processorId / 0x100) & 0xff);
    pbytes[3] = (byte) (processorId & 0xff);
    ProcessorIdHolder.put(processorId, new int[1]);
    return processorId;
  }

  /**
   * send a command to all other servers that may include a processorId and/or payload.
   * It's up to the sender and receiver to coordinate whether a processor ID or payload
   * is expected.  This is currently done by sending commands that set flags like
   * preferThreadOwnedSockets in each server.
   */
  private void sendCommandAndWaitForAck(final MessageProtocolCommand command,
                                        final int processorId,
                                        final byte[] processorBytes,
                                        final byte[] payload)
    throws Exception
  {
    final Socket[] serverSockets = getServerConnections(true);

    try {
      for (int serverNumber = 0; serverNumber < NUMBER_OF_SERVERS; serverNumber++) {
        if (serverNumber != this.serverNumber) {
          logForDebugging("sending " + command + " to server " + serverNumber + "...");

          synchronized (serverSockets[serverNumber]) {
            final DataOutput out = getOutputStream(serverSockets[serverNumber]);
            out.writeByte(command.toByte());
            writeBytes(out, processorBytes);
            writeBytes(out, payload);
          }
        }
      }

      if (processorId > 0) { // shared sockets (only)
        waitForAcks(processorId);
      }
      else { // thread-owned or pooled sockets
        readAcksFromOthers(serverSockets);
      }
    }
    finally {
      returnSockets(serverSockets);
    }
  }

  /**
   * wait for replies from other servers.  The replies are received by reader threads.
   */
  private void waitForAcks(final Integer processorId) throws Exception {
    final int[] count = ProcessorIdHolder.get(processorId);
    int lastCount = 0;

    logForDebugging("waiting for " + (NUMBER_OF_SERVERS - 1) + " acks for processorId " + processorId);

    // TODO we should probably wait for the count to go to zero.  In that, way we can set the count based on
    // the number of servers that were sent messages supporting targetted messaging as opposed to send the message
    // to everyone.
    while (count[0] < NUMBER_OF_SERVERS - 1) {
      synchronized (count) {
        count.wait(10);
      }
      if (DEBUG && count[0] != lastCount) {
        logForDebugging("received " + count[0] + " acks for processorId " + processorId);
        lastCount = count[0];
      }
    }

    logForDebugging("done waiting for acks");

    ProcessorIdHolder.remove(processorId);
  }

  /**
   * read conserve-sockets=false acks from other servers.  As in gemfire, the acks are a single byte.
   */
  private void readAcksFromOthers(final Socket[] serverConnections) throws Exception {
    for (int serverNumber = 0; serverNumber < NUMBER_OF_SERVERS; serverNumber++) {
      if (serverNumber != this.serverNumber) {
        getInputStream(serverConnections[serverNumber]).readByte();
      }
    }
  }

  /** send an ack to a specific socket, assuming the other end is waiting for one */
  private void sendAck(final int serverId, final int processorId, final Socket socket) throws Exception {
    if (processorId == 0) {
      logForDebugging("sending a direct ack");
      getOutputStream(socket).writeByte(1);
    }
    else {
      logForDebugging("sending ack to server " + serverId + " for processor " + processorId);
      final Socket ackSocket = getAckSocket(serverId);
      try {
        synchronized (ackSocket) {
          final DataOutput out = getOutputStream(ackSocket);
          out.writeByte(MessageProtocolCommand.ACK_MESSAGE.toByte());
          out.writeInt(processorId);
        }
      }
      finally {
        returnAckSocket(serverId, ackSocket);
      }
    }
  }

  /** get the socket set that should be used by the current thread */
  private Socket[] getServerConnections(final boolean includeSelf) throws Exception {
    Socket[] serverConnections;

    if (preferPooledSockets) {
      serverConnections = connectionPool.getFromPool(includeSelf);
    }
    else if (preferThreadOwnedSockets) {
      serverConnections = threadOwnedConnections.get();
    }
    else {
      serverConnections = sharedConnections;
    }

    // the connection (socket) pool creates new sockets as needed
    if (!preferPooledSockets) {
      createMissingSockets(serverConnections, includeSelf);
    }

    return serverConnections;
  }

  /** this allocates sockets if necessary, allowing for lazy startup of servers and threads */
  private void createMissingSockets(final Socket[] serverConnections, final boolean includeSelf) throws Exception {
    boolean needNewSocket = false;

    for (int serverNumber = 0; serverNumber < NUMBER_OF_SERVERS; serverNumber++) {
      if (serverConnections[serverNumber] == null || serverConnections[serverNumber].isClosed()) {
        if (includeSelf || serverNumber != this.serverNumber) {
          needNewSocket = true;
          break;
        }
      }
    }

    if (needNewSocket) {
      synchronized (serverConnections) {
        for (int serverNumber = 0; serverNumber < NUMBER_OF_SERVERS; serverNumber++) {
          if (serverConnections[serverNumber] == null || serverConnections[serverNumber].isClosed()) {
            if (includeSelf || serverNumber != this.serverNumber) {
              createSocket(serverConnections, serverNumber);
            }
          }
        }
      }
    }
  }

  private void createSocket(final Socket[] serverConnections, final int serverNumber) throws Exception {
    serverConnections[serverNumber] = null;

    try {
      serverConnections[serverNumber] = new Socket();
      serverConnections[serverNumber].setTcpNoDelay(true);
      serverConnections[serverNumber].connect(createSocketAddress(serverNumber));
    }
    catch (java.net.ConnectException e) {
      logForError("Failed to connect to server #" + serverNumber + "!", e);
    }
  }

  /**
   * Ack sockets correspond to the "unordered" connections in GemFire and are used for sending replies or other high
   * priority messages that shouldn't be blocked by serial operation messages.
   */
  private Socket getAckSocket(final int serverNumber) throws Exception {
    Socket[] serverConnections = null;

    if (preferPooledSockets) {
      logForDebugging("using pooled connection for ack");
      return connectionPool.get(serverNumber);
    }
    else if (preferThreadOwnedAckSockets) {
      logForDebugging("using thread-owned connection for ack");
      serverConnections = threadOwnedUnorderedConnections.get();
    }
    else {
      logForDebugging("using shared connection for ack");
      serverConnections = sharedUndorderedConnections;
    }

    if (serverConnections[serverNumber] == null || serverConnections[serverNumber].isClosed()) {
      createSocket(serverConnections, serverNumber);
    }

    return serverConnections[serverNumber];
  }


  /** after using a socket for sending an Ack, it must be returned using this method */
  void returnAckSocket(int serverId, Socket socket) {
    if (preferPooledSockets) {
      connectionPool.returnToPool(serverId, socket);
    }
  }

  /** after using a socket array for sending a message, the sockets must be returned using this method */
  private boolean returnSockets(final Socket[] serverSocks) {
    if (preferPooledSockets) {
      connectionPool.returnToPool(serverSocks);
      return true;
    }
    return false;
  }

  private static interface ConnectionPool {

    // get a socket for a specific server
    public Socket get(int serverId) throws Exception;

    // get sockets to all servers
    public Socket[] getFromPool(boolean includeSelf) throws Exception;

    // return a specific socket to the pool
    public void returnToPool(int serverId, Socket socket);

    // return a collection of sockets from getFromPool back to the pool
    public void returnToPool(Socket[] sockets);

    // prefill the pool
    public void prefill(int numServers) throws Exception;

    public void logQueueSizes();

  }

  private static class QueuedConnectionPool implements ConnectionPool {
    Queue<Socket> socketQueues[];

    QueuedConnectionPool(int numServers) {
      socketQueues = new Queue[numServers];
      for (int i=0; i<socketQueues.length; i++) {
        socketQueues[i] = new ConcurrentLinkedQueue<Socket>();
      }
    }

    public Socket get(int serverId) throws Exception {
      Socket sock = socketQueues[serverId].poll();

      if (sock == null) {
        logForDebugging("creating new socket for server " + serverId);
        try {
          sock = new Socket(serverAddresses[serverId], BASE_PORT + serverId);
          sock.setTcpNoDelay(true);
        }
        catch (java.net.ConnectException e) {
          logForInfo("unable to connect to server #" + serverId);
          e.printStackTrace();
        }
      }

      return sock;
    }

    public Socket[] getFromPool(final boolean includeSelf) throws Exception {
      final Socket[] result = new Socket[NUMBER_OF_SERVERS];

      for (int index = 0; index < NUMBER_OF_SERVERS; index++) {
        if (includeSelf || index != serverNumber) {
          result[index] = get(index);
        }
      }

      return result;
    }

    public void returnToPool(int serverId, Socket socket) {
      socketQueues[serverId].add(socket);
    }

    public void returnToPool(final Socket[] serverSockets) {
      for (int serverId = 0; serverId < serverSockets.length; serverId++) {
        if (serverSockets[serverId] != null) {
          socketQueues[serverId].add(serverSockets[serverId]);
        }
      }
    }

    public void prefill(final int numberOfSocketSets) throws Exception {
      final Object[] socketSets = new Object[numberOfSocketSets];

      logForInfo("pre-filling socket pool with " + numberOfSocketSets + " connections per server");

      for (int count = 0; count < numberOfSocketSets; count++) {
        socketSets[count] = getFromPool(true);
      }

      for (int count = 0; count < numberOfSocketSets; count++) {
        returnToPool((Socket[]) socketSets[count]);
      }
    }


    public void logQueueSizes() {
      StringBuilder sb = new StringBuilder(200);
      sb.append("Server " + serverNumber + " pool sizes = [");
      int len = socketQueues.length;
      for (int i=0; i<len; i++) {
        sb.append(socketQueues[i].size());
        if ( (i+1) < len ) {
          sb.append(", ");
        }
      }
      sb.append("]");

      System.out.println(sb.toString());
    }
  }


  /**
   * this is like QueuedConnectionPool, but there is only one queue and it
   * holds a complete collection of connections to other servers.  In GemFire
   * this would be equivalent to storing a Stub->Connection map in the queue
   * in ConnectionTable and handing the map out to a thread that is sending
   * a message.  After sending the message the map is returned to the queue.
   * @author bruces
   *
   */
  private static class QueuedConnectionsPool implements ConnectionPool {

    private Queue<Socket[]> connectionsQueue;

    // this thread-local caches the array of sockets that was used to send a reply message
    private ThreadLocal<Socket[]> allocatedConnections = new ThreadLocal<Socket[]>();

    private QueuedConnectionsPool(int numServers) {
      connectionsQueue = new ConcurrentLinkedQueue<Socket[]>();
    }

    // this is used to get a socket for sending a reply message
    public Socket get(int serverId) throws Exception {
      Socket[] s = getFromPool(true);
      allocatedConnections.set(s);
      return s[serverId];
    }

    /** get all of the sockets needed for messaging */
    public Socket[] getFromPool(boolean includeSelf) throws Exception {
      Socket[] socks = connectionsQueue.poll();

      if (socks == null) {
        socks = new Socket[NUMBER_OF_SERVERS];
        logForDebugging("creating new socket collection");
        // In GemFire this would not pre-fill the collection with connections.
        // They would be created and added to the map as needed for sending the current message.
        for (int index = 0; index < NUMBER_OF_SERVERS; index++) {
          try {
            socks[index] = new Socket(serverAddresses[index], BASE_PORT + index);
            socks[index].setTcpNoDelay(true);
          }
          catch (java.net.ConnectException e) {
            logForInfo("unable to connect to server #" + index);
            e.printStackTrace();
          }
        }
      }
      return socks;
    }

    /** return a single socket to the pool */
    public void returnToPool(int serverId, Socket socket) {
      connectionsQueue.add(allocatedConnections.get());
      allocatedConnections.set(null);
    }

    /** return sockets to the pool */
    public void returnToPool(Socket[] sockets) {
      connectionsQueue.add(sockets);
    }

    public void prefill(final int numberOfSocketSets) throws Exception {
      final Object[] socketSets = new Object[numberOfSocketSets];

      logForInfo("pre-filling socket pool with " + numberOfSocketSets + " connections per server");

      for (int index = 0; index < numberOfSocketSets; index++) {
        socketSets[index] = getFromPool(true);
      }

      for (int index = 0; index < numberOfSocketSets; index++) {
        returnToPool((Socket[]) socketSets[index]);
      }
    }


    public void logQueueSizes() {
      StringBuilder sb = new StringBuilder(200);
      sb.append("Server " + serverNumber + " pool size = " + connectionsQueue.size());
      System.out.println(sb.toString());
    }
  }

}
