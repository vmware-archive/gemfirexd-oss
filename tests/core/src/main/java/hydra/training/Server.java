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

//import com.gemstone.gemfire.internal.LogWriterImpl;
import com.gemstone.gemfire.SystemFailure;

import hydra.Log;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.List;
import java.util.LinkedList;
import util.TestException;

/**
 * A simple Java server that listens on a given {@linkplain
 * ServerPrms#getPort port} for a {@link Client} to connect to it.
 *
 * @author David Whitlock
 * @since 4.0
 */
public class Server extends Thread {

  /** The header int written by a "shutdown" client */
  private static final int SHUTDOWN_HEADER = 43;

  /** An exception thrown in the server thread */
  protected static Throwable serverError = null;

  //////////////////////  Instance Methods  //////////////////////

  /** The port on which this server listens */
  private int port;

  /** The live {@link ClientConnection}s in this server */
  protected List clientConnections = 
    Collections.synchronizedList(new LinkedList());

  ///////////////////////  Constructors  ///////////////////////

  /**
   * Creates a new <code>Server</code> that listens on the given
   * {@linkplain ServerPrms#getPort port}.
   */
  public Server(int port) {
    super(new ThreadGroup("Server threads") {
        public void uncaughtException(Thread thread, Throwable ex) {
          if (ex instanceof VirtualMachineError) {
            SystemFailure.setFailure((VirtualMachineError)ex); // don't throw
          }
          String s = "Uncaught exception in thread " + thread;
          Log.getLogWriter().severe(s, ex);
          serverError = ex;
        }
      }, "Server thread");
    this.port = port;
  }

  ////////////////////  Instance Methods  ////////////////////

  /**
   * Listens for clients to connect and services the connections
   * appropriately.
   */
  public void run() {
    ServerSocket server = null;
    try {
      server = new ServerSocket(this.port, 5 /* backlog */);
      while (true) {
        if (Thread.currentThread().isInterrupted()) {
          return;
        }

        if (ServerPrms.debug()) {
          Log.getLogWriter().info("Listening for connection on " +
                                  server);
        }

        Socket socket = server.accept();
        
        // Don't continue if JVM has been corrupted
        if (SystemFailure.getFailure() != null) {
          // Allocate no objects here!
          ServerSocket s = server;
          if (s != null) {
            try {
              s.close();
            }
            catch (IOException e) {
              // don't care
            }
          }
          SystemFailure.checkFailure(); // throws
        }
        
        if (ServerPrms.debug()) {
          Log.getLogWriter().info("Handling connection from " + socket);
        }

        // Read the header byte
        DataInputStream dis =
          new DataInputStream(socket.getInputStream());
        int header = dis.readInt();
        if (ServerPrms.debug()) {
          Log.getLogWriter().info("Read header int " + header);
        }

        ClientConnection client = new ClientConnection(socket);
        client.start();

        if (header == SHUTDOWN_HEADER) {
          return;
        }
      }

    } catch (IOException ex) {
      if (this.isInterrupted()) {
        return;
      }

      Server.serverError = ex;
      String s = "While accepting connection";
      Log.getLogWriter().severe(s, ex);

    } finally {
      if (server != null) {
        try {
          server.close();

        } catch (IOException ex) {
          Log.getLogWriter().severe("Can't close ServerSocket", ex);
        }
      }
    }
  }

  /**
   * Stops this server.  Waits for all of the threads to finish.
   */
  public void close() throws IOException, InterruptedException {
    // Stop the listener thread by having a "shutdown" client connect
    // to it.
    Client client = 
      new Client() {
          protected int getHeaderInt() {
            return SHUTDOWN_HEADER;
          }
        };
    client.connect(this.port);

    this.join();

    // Stop the client threads (of which there should be none).  We
    // have to be careful about accessing the list because of
    // potential ConcurrentModificationExceptions.
    while (!this.clientConnections.isEmpty()) {
      ClientConnection conn =
        (ClientConnection) this.clientConnections.remove(0);
      conn.interrupt();
      conn.join();
    }

    if (Server.serverError != null) {
      String s = "Error in server";
      throw new TestException(s, Server.serverError);
    }
  }

  /**
   * Returns a brief description of this <code>Server</code>.
   */
  public String toString() {
    return "Server " + this.port + " with " +
      this.clientConnections.size() + " clients";
  }

  ////////////////////  Inner Classes  ////////////////////

  /**
   * A thread that communicates with the client.  Basically, the
   * server sends the client the integers 0 through 999.
   */
  class ClientConnection extends Thread {

    /** The socket used to communicate with client */
    private final Socket socket;

    ////////////////////  Constructors  ////////////////////

    /**
     * Creates a new <code>ClientConnection</code> that communicates
     * over the given socket.
     */
    ClientConnection(Socket socket) {
      super("Client connection to " + socket);
      this.socket = socket;
      Server.this.clientConnections.add(this);
    }

    /**
     * Performs the work of this connection by sending the numbers 0
     * through 999 to the client.
     */
    public void run() {
      try {
        DataOutputStream dos =
          new DataOutputStream(this.socket.getOutputStream());
        for (int i = 0; i < 1000; i++) {
          dos.writeInt(i);
        }
        dos.flush();
        dos.close();

      } catch (IOException ex) {
        if (this.isInterrupted()) {
          return;
        }

        Server.serverError = ex;
        String s = "While communicating with client " + this.socket;
        Log.getLogWriter().severe(s, ex);

      } finally {
        Server.this.clientConnections.remove(this);
      }
    }
  }

}
