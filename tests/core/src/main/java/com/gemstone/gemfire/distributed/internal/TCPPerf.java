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
package com.gemstone.gemfire.distributed.internal;

import com.gemstone.gemfire.internal.AvailablePortHelper;
import dunit.*;
import hydra.*;

import java.io.*;
import java.net.*;

/**
 * This class tests the performance of sending {@link
 * MessagingPerf.Message messages} between two machines using TCP
 * {@link Socket sockets}.
 *
 * @author David Whitlock
 *
 */
public class TCPPerf extends MessagingPerf {

  public TCPPerf(String name) {
    super(name, false /* usesGemFire */);
  }

  ////////////////////////  Helper Methods  ///////////////////////

  /**
   * Listens for a connection on the given port
   */
  private static Socket waitForConnection(int port)
    throws IOException {

    Socket socket = (new ServerSocket(port)).accept();
    if (!Prms.useDefaultSocketBufferSize()) {
      int size = Prms.getSocketBufferSize();
      getLogWriter().info("Setting socket buffer size to " + size);
      socket.setReceiveBufferSize(size);
      socket.setSendBufferSize(size);
    }
    getLogWriter().info("Setting tcpNoDelay to " + Prms.getTcpNoDelay());
    socket.setTcpNoDelay(Prms.getTcpNoDelay());
    return socket;
  }

  /**
   * Repeatedly tries to contact a server running on the given port
   */
  private static Socket attemptConnection(String host, int port) 
    throws IOException {

    for (int i = 0; i < 100; i++) {
      try {
        Socket socket = new Socket(host, port);
        if (!Prms.useDefaultSocketBufferSize()) {
          int size = Prms.getSocketBufferSize();
          getLogWriter().info("Setting socket buffer size to " + size);
          socket.setReceiveBufferSize(size);
          socket.setSendBufferSize(size);
        }
        getLogWriter().info("Setting tcpNoDelay to " + Prms.getTcpNoDelay());
        socket.setTcpNoDelay(Prms.getTcpNoDelay());
        return socket;

      } catch (ConnectException ex) {
        // Sleep and try again
        try {
          Thread.sleep(500);
          getLogWriter().info("Retrying " + host + ":" + port);

        } catch (InterruptedException ex2) {
          String s = "Interrupted while sleeping";
          throw new InterruptedIOException(s);
        }
      }
    }

    String s = "Timed out!!";
    throw new IllegalStateException(s);
  }

  /**
   * "Ping pongs" messages back and forth between two machines over
   * two sockets.
   * 
   * Accessed via reflection.  DO NOT REMOVE
   *
   * @param sendingPort
   *        The port on which data is sent
   * @param receivingPort
   *        The port on which data is received
   * @param otherHost
   *        The name of the other host to contact
   * @param sendFirstMessage
   *        Should this VM send the first message, or wait to receive
   *        the first message?  The VM that sends the first message is
   *        the server.
   */
  protected static void work(int sendingPort, int receivingPort,
                           String otherHost, boolean sendFirstMessage)
    throws IOException, ClassNotFoundException {

    OutputStream out = null;
    InputStream in = null;
    Message m = null;
    boolean startTimeSet = !sendFirstMessage;

    if (sendFirstMessage) {
      getLogWriter().info("Starting server on port " + sendingPort);

      // Create the server, send the first message, wait for a response
      out = waitForConnection(sendingPort).getOutputStream();
      in = attemptConnection(otherHost, receivingPort).getInputStream();

      m = new Message();
      m.number = 0;

    } else {
      getLogWriter().info("Starting client on " + receivingPort);
      in = attemptConnection(otherHost, receivingPort).getInputStream();
      out = waitForConnection(sendingPort).getOutputStream();
    }

    final long WARMUP_COUNT = MessagingPerf.Prms.getWarmupCount();
    final long messageCount = MessagingPerf.Prms.getMessageCount() + WARMUP_COUNT;

    while (true) {
      if (m != null) {
        // Process message
        if (m.number >= messageCount) {
          // We received the last message, print out stats
          getLogWriter().info("Received last message");

          long end = System.currentTimeMillis();
          getLogWriter().info(noteTiming(messageCount - WARMUP_COUNT, "messages", m.begin, end,
                             "milliseconds"));
          break;
        } else if (!startTimeSet && m.number >= WARMUP_COUNT) {
          m.begin = System.currentTimeMillis();
          startTimeSet = true;
        }

        // Send next message
        Message m2 = new Message();
        m2.begin = m.begin;
        long nextNumber = m.number + 1;
        m2.number = nextNumber;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(m2);
        oos.flush();
        byte[] m2bytes = baos.toByteArray();
        byte[] m2len = new byte[4];
        m2len[0] = (byte)((m2bytes.length / 0x1000000) & 0xff);
        m2len[1] = (byte)((m2bytes.length / 0x10000) & 0xff);
        m2len[2] = (byte)((m2bytes.length / 0x100) & 0xff);
        m2len[3] = (byte)(m2bytes.length & 0xff);
        out.write(m2len, 0, 4);
        out.write(m2bytes, 0, m2bytes.length);
        out.flush();

        if (nextNumber >= messageCount) {
          // We sent the last message
          String s = "Sent last message (" + messageCount + ")";
          getLogWriter().info(s);
          break;
        }
      }

      // Wait for next message
     byte[] mlen = new byte[4];
     in.read(mlen, 0, 4);
     int mleni = ((mlen[0]&0xff)*0x1000000)
                 + ((mlen[1]&0xff)*0x10000)
                 + ((mlen[2]&0xff)*0x100)
                 + (mlen[3]&0xff);
     byte[] mbytes = new byte[mleni];
     in.read(mbytes, 0, mleni);
     ByteArrayInputStream bais = new ByteArrayInputStream(mbytes);
     ObjectInputStream ois = new ObjectInputStream(bais);
     m = (Message) ois.readObject();
    }
  }

  ///////////////////////  Test Methods  ///////////////////////

  /**
   * In VMs on separate machines, send messages back and forth over
   * TCP sockets.
   */
  public void testSendingMessages() throws InterruptedException {
    final Host host0 = Host.getHost(0);
    final Host host1 = Host.getHost(1); 
    VM vm0 = host0.getVM(0);
    VM vm1 = host1.getVM(0);

    int[] freeTCPPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    int port1 = freeTCPPorts[0];
    int port2 = freeTCPPorts[0];

    AsyncInvocation ai0 =
      vm0.invokeAsync(this.getClass(), "work", new Object[] {
        new Integer(port1), new Integer(port2), getServerHostName(host1),
        Boolean.TRUE });
    AsyncInvocation ai1 =
      vm1.invokeAsync(this.getClass(), "work", new Object[] {
        new Integer(port2), new Integer(port1), getServerHostName(host0),
        Boolean.FALSE });

    DistributedTestCase.join(ai0, 30 * 1000, getLogWriter());
    if (ai0.exceptionOccurred()) {
      fail("Exception during " + ai0, ai0.getException());
    }

    DistributedTestCase.join(ai1, 30 * 1000, getLogWriter());
    if (ai1.exceptionOccurred()) {
      fail("Exception during " + ai1, ai1.getException());
    }
  }

  ///////////////////////  Inner Classes  ///////////////////////

  /**
   * Configuration parameters for the the {@link TCPPerf} test
   */
  public static class Prms extends MessagingPerf.Prms {
    
    /** The size of the socket buffers of the sockets created
     * by a {@link TCPPerf} test. */
    public static Long socketBufferSize;

    /** Is {@link Socket#setTcpNoDelay(boolean) TCP no delay} used for
     * the sockets used by this test */
    public static Long tcpNoDelay;

    static {
      BasePrms.setValues(Prms.class);
    }

    protected static boolean getTcpNoDelay() {
      return TestConfig.tab().booleanAt(Prms.tcpNoDelay);
    }

    protected static int getSocketBufferSize() {
      return TestConfig.tab().intAt(Prms.socketBufferSize);
    }

    /**
     * Returns whether or not the user wants a non-default socket
     * buffer size.
     */
    protected static boolean useDefaultSocketBufferSize() {
      return TestConfig.tab().get(Prms.socketBufferSize)
        != null;
    }

  }


}
