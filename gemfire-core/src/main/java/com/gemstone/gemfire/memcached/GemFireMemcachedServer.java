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
package com.gemstone.gemfire.memcached;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.memcached.ConnectionHandler;

/**
 * This is the Server that listens for incoming memcached client connections.
 * This server understands the memcached ASCII protocol
 * documented in
 * <a href="https://github.com/memcached/memcached/blob/master/doc/protocol.txt">memcached source control</a>
 * It then translates these commands to the corresponding
 * GemFire commands, and stores the data in GemFire in a {@link Region}
 * named "gemcached".
 * <p>
 * "gemcached" region is {@link RegionShortcut#PARTITION} by default,
 * though a cache.xml can be provided to override region attributes.
 * 
 * This class has a Main method that can be used to
 * start the server.
 * 
 * @author Swapnil Bawaskar
 *
 */
public class GemFireMemcachedServer {

  /**
   * The protocol used by GemFireMemcachedServer
   */
  public enum Protocol {
    ASCII,
    BINARY
  }

  private static LogWriter logger;
  
  /**
   * Name of the GemFire region in which data is stored, value id "gemcached"
   */
  public static final String REGION_NAME = "gemcached";

  /**
   * version of gemcached server
   */
  public static final String version = "0.2";
  
  /**
   * the port to listen for client connections
   */
  private final int serverPort;
  
  private final int DEFAULT_PORT = 11212;

  /**
   * the thread executor pool to handle requests from clients.
   * We create one thread for each client.
   */
  private ExecutorService executor = Executors.newCachedThreadPool();

  /**
   * GemFire cache where data will be stored
   */
  private Cache cache;
  
  /**
   * thread that listens for client connections
   */
  private Thread acceptor;

  /**
   * The protocol that this server understands, ASCII by default
   */
  private final Protocol protocol;

  /**
   * Create an instance of the server. to start
   * the server {@link #start()} must be called.
   * 
   * @param port the port on which the server listens
   *        for new memcached client connections.
   */
  public GemFireMemcachedServer(int port) {
    if (port <= 0) {
      this.serverPort = DEFAULT_PORT;
    } else {
      this.serverPort = port;
    }
    this.protocol = Protocol.ASCII;
  }

  /**
   * Create an instance of the server. to start
   * the server {@link #start()} must be called.
   * 
   * @param port the port on which the server listens
   *        for new memcached client connections.
   * @param protocol the protocol that this server should understand
   * @see Protocol
   */
  public GemFireMemcachedServer(int port, Protocol protocol) {
    if (port <= 0 ) {
      this.serverPort = DEFAULT_PORT;
    } else {
      this.serverPort = port;
    }
    this.protocol = protocol;
  }

  /**
   * Starts an embedded GemFire caching node, and then
   * listens for new memcached client connections.
   */
  public void start() {
    startGemFire();
    try {
      startMemcachedServer();
    } catch (IOException e) {
      throw new RuntimeException("Could not start Server", e);
    } catch (InterruptedException e) {
      throw new RuntimeException("Could not start Server", e);
    }
  }
  
  private void startGemFire() {
    CacheFactory cacheFactory = new CacheFactory();
    this.cache = cacheFactory.create();
    logger = this.cache.getLogger();
  }
  
  private void startMemcachedServer() throws IOException, InterruptedException {
    ServerSocketChannel channel = ServerSocketChannel.open();
    final ServerSocket serverSocket = channel.socket();
    serverSocket.bind(new InetSocketAddress(SocketCreator.getLocalHost(), serverPort));
    final CountDownLatch latch = new CountDownLatch(1);
    acceptor = new Thread(new Runnable() {
      public void run() {
        for (;;) {
          Socket s = null;
          try {
            latch.countDown();
            s = serverSocket.accept();
            handleNewClient(s);
          } catch (ClosedByInterruptException e) {
            try {
              serverSocket.close();
            } catch (IOException e1) {
              e1.printStackTrace();
            }
            break;
          } catch (IOException e) {
            e.printStackTrace();
            break;
          }
        }
      }
    }, "AcceptorThread");
    acceptor.setDaemon(true);
    acceptor.start();
    latch.await();
    logger.config("GemFireMemcachedServer server started on host:"+SocketCreator.getLocalHost()+" port: "+this.serverPort);
  }
  
  private void handleNewClient(Socket s) {
    ConnectionHandler connHandler = new ConnectionHandler(s, cache, protocol);
    executor.execute(connHandler);
  }

  /**
   * shuts down this server and closes the embedded
   * GemFire caching node
   */
  public void shutdown() {
    this.acceptor.interrupt();
    this.executor.shutdownNow();
    this.cache.close();
  }
  
  /**
   * 
   * @param args
   */
  public static void main(String[] args) {
    int port = getPort(args);
    GemFireMemcachedServer server = new GemFireMemcachedServer(port);
    server.start();
    while(true) {
      try {
        System.in.read();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private static int getPort(String[] args) {
    int port = 0;
    if (args != null && args.length > 0) {
      for (int i=0; i<args.length; i++) {
        if (args[i].startsWith("-port")) {
          String p = args[i].substring(args[i].indexOf('='));
          p = p.trim();
          port = Integer.parseInt(p);
        }
      }
    }
    return port;
  }
}
