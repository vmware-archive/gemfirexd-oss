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
package com.gemstone.gemfire.internal;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;

import io.snappydata.test.dunit.DistributedTestBase;
import junit.framework.TestCase;


/**
 * Test creation of server sockets and client sockets with various JSSE
 * configurations.
 */
public class JSSESocketTest extends TestCase {

  ServerSocket acceptor;
  Socket server;
  
  static ByteArrayOutputStream baos = new ByteArrayOutputStream( );
  static LocalLogWriter log = new LocalLogWriter(LogWriterImpl.ALL_LEVEL, new PrintWriter( baos )); 
  
  private String name;
  private int randport = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
  
  public JSSESocketTest( String name ) {
    super( name );
    this.name = name;
  }
  
  public void setUp( ) throws Exception {
    System.out.println( "\n\n########## setup " + name + " ############\n\n" );
    server = null;
    acceptor = null;
    baos.reset();
  }
  
  public void tearDown( ) throws Exception {
    System.out.println( "\n\n########## teardown " + name + " ############\n\n" );
    
    if ( server != null ) {
      server.close();
    }
    if ( acceptor != null ) {
      acceptor.close();
    }
    System.out.println( baos.toString() );
  }
  
  //----- test methods ------
  
  public void testSSLSocket( ) throws Exception {
    final Object[] receiver = new Object[1];
    
    {
      System.setProperty( "gemfire.mcast-port", "0");
      System.setProperty( "gemfire.ssl-enabled", "true" );
      System.setProperty( "gemfire.ssl-require-authentication", "true" );
      System.setProperty( "gemfire.ssl-ciphers", "SSL_RSA_WITH_RC4_128_MD5" );
      System.setProperty( "gemfire.ssl-protocols", "SSLv3" );
      
      File jks = findTestJKS();
      System.setProperty( "javax.net.ssl.trustStore", jks.getCanonicalPath() );
      System.setProperty( "javax.net.ssl.trustStorePassword", "password" );
      System.setProperty( "javax.net.ssl.keyStore", jks.getCanonicalPath() );
      System.setProperty( "javax.net.ssl.keyStorePassword", "password" );
    }
    
    assertTrue(SocketCreator.getDefaultInstance().useSSL());
    
    Thread serverThread = startServer( receiver );
    
    Socket client = SocketCreator.getDefaultInstance().connectForServer( InetAddress.getByName("localhost"), randport, log );
    ObjectOutputStream oos = new ObjectOutputStream( client.getOutputStream() );
    String expected = new String( "testing " + name );
    oos.writeObject( expected );
    oos.flush();

    DistributedTestBase.join(serverThread, 30 * 1000, null);
    
    client.close();
    if ( expected.equals( receiver[0] ) ) {
      System.out.println( "received " + receiver[0] + " as expected." );
    } else {
      throw new Exception( "Expected \"" + expected + "\" but received \"" + receiver[0] + "\"" );
    }
    
    String logOutput = baos.toString();
    StringReader sreader = new StringReader( logOutput );
    LineNumberReader reader = new LineNumberReader( sreader );
    int peerLogCount = 0;
    String line = null;
    while( (line = reader.readLine()) != null ) {
      
      if (line.matches( ".*peer CN=.*" ) ) {
        System.out.println( "Found peer log statement." );
        peerLogCount++;
      }
    }
    if ( peerLogCount != 2 ) {
      throw new Exception( "Expected to find to peer identities logged." );
    }
  }
  
  /** not actually related to this test class, but this is as good a place
      as any for this little test of the client-side ability to tell gemfire
      to use a given socket factory.  We just test the connectForClient method
      to see if it's used */
  public void testClientSocketFactory() {
    System.getProperties().put("gemfire.clientSocketFactory",
      TSocketFactory.class.getName());
    System.getProperties().remove( "gemfire.ssl-enabled");
    SocketCreator.getDefaultInstance(new Properties());
    factoryInvoked = false;
    try {
      try {
        Socket sock = SocketCreator.getDefaultInstance().connectForClient("localhost", 12345,
          new LocalLogWriter(LogWriterImpl.ALL_LEVEL, System.out),0);
        sock.close();
        fail("socket factory was not invoked");
      } catch (IOException e) {
        assertTrue("socket factory was not invoked: " + factoryInvoked, factoryInvoked);
      }
    } finally {
      System.getProperties().remove("gemfire.clientSocketFactory");
      SocketCreator.getDefaultInstance().initializeClientSocketFactory();
    }
  }
  
  static boolean factoryInvoked;
  
  public static class TSocketFactory implements com.gemstone.gemfire.distributed.ClientSocketFactory
  {
    public TSocketFactory() {
    }
    
    public Socket createSocket(InetAddress address, int port) throws IOException {
      JSSESocketTest.factoryInvoked = true;
      throw new IOException("splort!");
    }
  }
  
  
  //------------- utilities -----
  
  private File findTestJKS() {
    String location = JSSESocketTest.class.getProtectionDomain().getCodeSource().getLocation().getPath();
    File classesDir = new File( location );
    File ssldir = new File( classesDir, "ssl" );
    return new File( ssldir, "trusted.keystore" );
  }
  
  private Thread startServer( final Object[] receiver ) throws Exception {
    final ServerSocket ss = SocketCreator.getDefaultInstance().createServerSocket( randport, 0, InetAddress.getByName( "localhost" ), log );
    
    
    Thread t = new Thread( new Runnable() {
      public void run( ) {
        try {
        Socket s = ss.accept();
        SocketCreator.getDefaultInstance().configureServerSSLSocket( s, log );
        ObjectInputStream ois = new ObjectInputStream( s.getInputStream() );
        receiver[0] = ois.readObject( );
        server = s;
        acceptor = ss;
        } catch ( Exception e ) {
          e.printStackTrace();
          receiver[0] = e;
        }
      }
    }, name + "-server" );
    t.start();
    return t;
  }
  
}
