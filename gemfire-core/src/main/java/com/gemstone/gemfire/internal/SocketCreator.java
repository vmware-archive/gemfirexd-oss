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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.*;
import java.nio.channels.ServerSocketChannel;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.*;
import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;
import javax.net.ssl.*;

import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.SystemConnectException;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.admin.internal.InetAddressUtil;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;
import com.gemstone.gemfire.distributed.ClientSocketFactory;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.GfeConsoleReaderFactory.GfeConsoleReader;
import com.gemstone.gemfire.internal.cache.wan.TransportFilterServerSocket;
import com.gemstone.gemfire.internal.cache.wan.TransportFilterSocketFactory;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.shared.ClientSharedUtils;
import com.gemstone.gemfire.internal.util.PasswordUtil;
import com.gemstone.org.jgroups.util.ConnectionWatcher;
import com.gemstone.org.jgroups.util.GFLogWriter;

/**
 * Analyze configuration data (gemfire.properties) and configure sockets
 * accordingly for SSL.
 * <p>
 * gemfire.useSSL = (true|false) default false.<br/>
 * gemfire.ssl.debug = (true|false) default false.<br/>
 * gemfire.ssl.needClientAuth = (true|false) default true.<br/>
 * gemfire.ssl.protocols = <i>list of protocols</i><br/>
 * gemfire.ssl.ciphers = <i>list of cipher suites</i><br/>
 * <p>
 * The following may be included to configure the certificates used by the
 * Sun Provider.
 * <p>
 * javax.net.ssl.trustStore = <i>pathname</i><br/>
 * javax.net.ssl.trustStorePassword = <i>password</i><br/>
 * javax.net.ssl.keyStore = <i>pathname</i><br/>
 * javax.net.ssl.keyStorePassword = <i>password</i><br/>
 * <p>
 * Additional properties will be set as System properties to be available
 * as needed by other provider implementations.
 */
public class SocketCreator  implements com.gemstone.org.jgroups.util.SockCreator {

  private static final Map<InetAddress, String> hostNames;

  /** flag to force always using DNS (regardless of the fact that these lookups can hang) */
  public static final boolean FORCE_DNS_USE = Boolean.getBoolean("gemfire.forceDnsUse");
  
  /** set this to false to inhibit host name lookup */
  public static volatile boolean resolve_dns = true;

  /** The default instance for use in GemFire socket creation */
  private static SocketCreator DEFAULT_INSTANCE = new SocketCreator();
  
  /**
   * Allow unlimited concurrent reads (uses of SocketCreator).  Re-initializing 
   * SocketCreator requires the write lock which will block out all reads until 
   * it's done.  If this causes performance loss, removal of ReadWriteLock 
   * should only impact the Console or Admin APIs.
   */
  //private final ReadWriteLock rw = new ReentrantReadWriteLock();
  
  /** True if this SocketCreator has been initialized and is ready to use */
  private boolean ready = false;
  
  /** True if configured to use SSL */
  private boolean useSSL;
  
  /** True if configured to require client authentication */
  private boolean needClientAuth;
  
  /** Space-delimited list of SSL protocols to use, 'any' allows any */
  private String[] protocols;
  
  /** Space-delimited list of SSL ciphers to use, 'any' allows any */
  private String[] ciphers;
  
  /** Only print this SocketCreator's config once */
  private boolean configShown = false;

  /** context for SSL socket factories */
  private SSLContext sslContext;

  static {
    // add localhost to hostnames
    hostNames = new HashMap<InetAddress, String>();
    final InetAddress host = ClientSharedUtils.getCachedLocalHost();
    if (host != null) {
      hostNames.put(host, host.getHostName());
    }
  }

  /** A factory used to create client <code>Sockets</code>. */
  private ClientSocketFactory clientSocketFactory;
  
  /**
   * Whether to enable TCP keep alive for sockets. This boolean is controlled by
   * the gemfire.setTcpKeepAlive java system property.  If not set then GemFire
   * will enable keep-alive on server->client and p2p connections.
   */
  public static final boolean ENABLE_TCP_KEEP_ALIVE;
  
  
  
  static {
    // bug #49484 - customers want tcp/ip keep-alive turned on by default
    // to avoid dropped connections.  It can be turned off by setting this
    // property to false
    String str = System.getProperty("gemfire.setTcpKeepAlive");
    if (str != null) {
      ENABLE_TCP_KEEP_ALIVE = Boolean.getBoolean("gemfire.setTcpKeepAlive");
    } else {
      ENABLE_TCP_KEEP_ALIVE = true;
    }
  }

  // -------------------------------------------------------------------------
  //   Constructor
  // -------------------------------------------------------------------------
  
  /** Constructs new SocketCreator instance. */
  private SocketCreator() {}
  
  // -------------------------------------------------------------------------
  //   Static instance accessors
  // -------------------------------------------------------------------------
  
  /** 
   * Returns the default instance for use in GemFire socket creation. 
   * <p>
   * If not already initialized, the default instance of SocketCreator will be 
   * initialized using defaults in {@link 
   * com.gemstone.gemfire.distributed.internal.DistributionConfig}. If any 
   * values are specified in System properties, those values will be used to 
   * override the defaults.
   * <p>
   * Synchronizes on the DEFAULT_INSTANCE.
   */
  public static SocketCreator getDefaultInstance() {
    synchronized (DEFAULT_INSTANCE) {
      if (!DEFAULT_INSTANCE.ready) {
        DEFAULT_INSTANCE.initialize();
      }
    }
    return DEFAULT_INSTANCE;
  }
  
  /**
   * Returns the default instance for use in GemFire socket creation after
   * initializing it using the DistributionConfig.
   * <p>
   * This will reinitialize the SocketCreator if it was previously initialized.
   * <p>
   * Synchronizes on the DEFAULT_INSTANCE.
   */
  public static SocketCreator getDefaultInstance(DistributionConfig config) {
    synchronized (DEFAULT_INSTANCE) {
      DEFAULT_INSTANCE.initialize(config);
    }
    return DEFAULT_INSTANCE;
  }
  
  /**
   * Returns the default instance for use in GemFire socket creation after
   * initializing it using defaults in {@link 
   * com.gemstone.gemfire.distributed.internal.DistributionConfig}. If any 
   * values are specified in the provided properties or in System properties,
   * those values will be used to override the defaults.
   * <p>
   * This will reinitialize the SocketCreator if it was previously initialized.
   * <p>
   * Call will synchronize on the DEFAULT_INSTANCE.
   */
  public static SocketCreator getDefaultInstance(Properties props) {
    return getDefaultInstance(new DistributionConfigImpl(props));
  }
  
  /** 
   * Create and initialize a new non-default instance of SocketCreator. 
   * <p>
   * Synchronizes on the new instance.
   *
   * @param useSSL          true if ssl is to be enabled
   * @param needClientAuth  true if client authentication is required
   * @param protocols       space-delimited list of ssl protocols to use
   * @param ciphers         space-delimited list of ssl ciphers to use
   * @param sysProps        vendor properties to be set as System properties
   */
  public static SocketCreator createNonDefaultInstance(boolean useSSL,
                                                       boolean needClientAuth,
                                                       String protocols,
                                                       String ciphers,
                                                       Properties sysProps) {
    SocketCreator sc = new SocketCreator();
    synchronized (sc) {
      sc.initialize(useSSL, needClientAuth, readArray(protocols), readArray(ciphers), sysProps);
    }
    return sc;
  }

  /**
   * All GemFire code should use this method instead of
   * InetAddress.getLocalHost(). See bug #40623
   */
  public static InetAddress getLocalHost() throws UnknownHostException {
    return ClientSharedUtils.getLocalHost();
  }

  /** All classes should use this instead of relying on the JRE system property */
  public static boolean preferIPv6Addresses() {
    return ClientSharedUtils.preferIPv6Addresses();
  }

  /**
   * returns the host name for the given inet address, using a local cache
   * of names to avoid dns hits and duplicate strings
   */
  public static synchronized String getHostName(InetAddress addr) {
    String result = hostNames.get(addr);
    if (result == null) {
      result = addr.getHostName();
      hostNames.put(addr, result);
    }
    return result;
  }

  /**
   * returns the host name for the given inet address, using a local cache of
   * names to avoid dns hits and duplicate strings
   */
  public static synchronized String getCanonicalHostName(InetAddress addr,
      String hostName) {
    String result = hostNames.get(addr);
    if (result == null) {
      hostNames.put(addr, hostName);
      return hostName;
    }
    return result;
  }

  // -------------------------------------------------------------------------
  //   Initializers (change SocketCreator state)
  // -------------------------------------------------------------------------
  
  /**
   * Initialize this SocketCreator. 
   * <p>
   * Caller must synchronize on the SocketCreator instance.
   *
   * @param useSSL          true if ssl is to be enabled
   * @param needClientAuth  true if client authentication is required
   * @param protocols       array of ssl protocols to use
   * @param ciphers         array of ssl ciphers to use
   * @param props        vendor properties passed in through gfsecurity.properties
   */
  private void initialize(boolean useSSL,
                          boolean needClientAuth,
                          String[] protocols,
                          String[] ciphers,
                          Properties props) {
    Assert.assertHoldsLock(this, true); 
    try {
//       rw.writeLock().lockInterruptibly();
//       try {
        this.useSSL = useSSL;
        this.needClientAuth = needClientAuth;
        
        this.protocols = protocols;
        this.ciphers = ciphers;
        
        if (this == DEFAULT_INSTANCE) {
          // set p2p values...
          if (this.useSSL) {
            System.setProperty( "p2p.useSSL", "true" );
            System.setProperty( "p2p.oldIO", "true" );
            System.setProperty( "p2p.nodirectBuffers", "true" );
            
            try {
              if (sslContext == null) {
                sslContext = createAndConfigureSSLContext(protocols, props);
                SSLContext.setDefault(sslContext);
              }
            } catch (Exception e) {
               throw new GemFireConfigException("Error configuring GemFire ssl ",e);
            }
          }
          else {
            System.setProperty( "p2p.useSSL", "false" );
          }
          // make sure TCPConduit picks up p2p properties...
          com.gemstone.gemfire.internal.tcp.TCPConduit.init();
        } else if (this.useSSL && sslContext == null) {
          try {
            sslContext = createAndConfigureSSLContext(protocols, props);
          } catch (Exception e) {
            throw new GemFireConfigException("Error configuring GemFire ssl ",e);
          }
        }
        
        initializeClientSocketFactory();
        this.ready = true;
//       }
//       finally {
//         rw.writeLock().unlock();
//       }
    }
    catch ( Error t ) {
      if (SystemFailure.isJVMFailureError(t)) {
        SystemFailure.initiateFailure(t);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw t;
      }
      // Whenever you catch Error or Throwable, you must also
      // check for fatal JVM error (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      t.printStackTrace();
      throw t;
    } 
    catch ( RuntimeException re ) {
      re.printStackTrace();
      throw re;
    }
  }
  
  /**
   * Creates & configures the SSLContext when SSL is enabled.
   * 
   * @param protocolNames
   *          valid SSL protocols for this connection
   * @param props
   *          vendor properties passed in through gfsecurity.properties
   * @return new SSLContext configured using the given protocols & properties
   * 
   * @throws GeneralSecurityException
   *           if security information can not be found
   * @throws IOException
   *           if information can not be loaded
   */
  private SSLContext createAndConfigureSSLContext(String[] protocolNames, Properties props) 
      throws GeneralSecurityException, IOException {

    SSLContext     newSSLContext = getSSLContextInstance(protocolNames);
    KeyManager[]   keyManagers   = getKeyManagers(props);
    TrustManager[] trustManagers = getTrustManagers(props);
    
    newSSLContext.init(keyManagers, trustManagers, null /* use the default secure random*/);
    return newSSLContext;
  }

  /**
   * Used by CacheServerLauncher and SystemAdmin to read the properties from
   * console
   * 
   * @param env
   *          Map in which the properties are to be read from console.
   */
  public static void readSSLProperties(Map<String, String> env) {
    readSSLProperties(env, false);
  }

  /**
   * Used to read the properties from console. AgentLauncher calls this method
   * directly & ignores gemfire.properties. CacheServerLauncher and SystemAdmin
   * call this through {@link #readSSLProperties(Map)} and do NOT ignore
   * gemfire.properties.
   * 
   * @param env
   *          Map in which the properties are to be read from console.
   * @param ignoreGemFirePropsFile
   *          if <code>false</code> existing gemfire.properties file is read, if
   *          <code>true</code>, properties from gemfire.properties file are
   *          ignored.
   */
  public static void readSSLProperties(Map<String, String> env, 
      boolean ignoreGemFirePropsFile) {
    Properties props = new Properties();
    DistributionConfigImpl.loadGemFireProperties(props, ignoreGemFirePropsFile);
    for (Object entry : props.entrySet()) {
      @SuppressWarnings("unchecked")
      Map.Entry<String, String> ent = (Map.Entry<String, String>)entry;
      // if the value of ssl props is empty, read them from console
      if (ent.getKey().startsWith(DistributionConfig.SSL_SYSTEM_PROPS_NAME)
          || ent.getKey().startsWith(DistributionConfig.SYS_PROP_NAME)) {
        String key = ent.getKey();
        if (key.startsWith(DistributionConfig.SYS_PROP_NAME)) {
          key = key.substring(DistributionConfig.SYS_PROP_NAME.length());
        }
        if (ent.getValue() == null || ent.getValue().trim().equals("")) {
          GfeConsoleReader consoleReader = GfeConsoleReaderFactory.getDefaultConsoleReader();
          if (!consoleReader.isSupported()) {
            throw new GemFireConfigException("SSL properties are empty, but a console is not available");
          }
          if (key.toLowerCase().contains("password")) {
            char[] password = consoleReader.readPassword("Please enter "+key+": ");
            env.put(key, PasswordUtil.encrypt(new String(password), false));
          } else {
            String val = consoleReader.readLine("Please enter "+key+": ");
            env.put(key, val);
          }
          
        }
      }
    }
  }
  
  private static SSLContext getSSLContextInstance(String[] protocols) {
    SSLContext c = null;
    if (protocols != null && protocols.length > 0) {
      for (String protocol : protocols) {
        if (!protocol.equals("any")) {
          try {
            c = SSLContext.getInstance(protocol);
            break;
          } catch (NoSuchAlgorithmException e) {
            // continue
          }
        }
      }
    }
    if (c != null) {
      return c;
    }
    // lookup known algorithms
    String[] knownAlgorithms = {"SSL", "SSLv2", "SSLv3", "TLS", "TLSv1", "TLSv1.1"};
    for (String algo : knownAlgorithms) {
      try {
        c = SSLContext.getInstance(algo);
        break;
      } catch (NoSuchAlgorithmException e) {
        // continue
      }
    }
    return c;
  }

  private TrustManager[] getTrustManagers(Properties sysProps)
      throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {
    TrustManager[] trustManagers = null;
    String trustStoreType = sysProps.getProperty("javax.net.ssl.trustStoreType");
    GfeConsoleReader consoleReader = GfeConsoleReaderFactory.getDefaultConsoleReader();
    if (trustStoreType == null) {
      trustStoreType = System.getProperty("javax.net.ssl.trustStoreType", KeyStore.getDefaultType());
    } else if (trustStoreType.trim().equals("")) {
      //read from console, default on empty
      if (consoleReader.isSupported()) {
        trustStoreType = consoleReader.readLine("Please enter the trustStoreType (javax.net.ssl.trustStoreType) : ");
      }
      if (isEmpty(trustStoreType)) {
        trustStoreType = KeyStore.getDefaultType();
      }
    }
    KeyStore ts = KeyStore.getInstance(trustStoreType);
    String trustStorePath = System.getProperty("javax.net.ssl.trustStore");
    if (trustStorePath == null) {
      trustStorePath = sysProps.getProperty("javax.net.ssl.trustStore");
    }
    if (trustStorePath != null) {
      if (trustStorePath.trim().equals("")) {
        trustStorePath = System.getenv("javax.net.ssl.trustStore");
        //read from console
        if (isEmpty(trustStorePath) && consoleReader.isSupported()) {
          trustStorePath = consoleReader.readLine("Please enter the trustStore location (javax.net.ssl.trustStore) : ");
        }
      }
      FileInputStream fis = new FileInputStream(trustStorePath);
      String passwordString = System.getProperty("javax.net.ssl.trustStorePassword");
      if (passwordString == null) {
        passwordString = sysProps.getProperty("javax.net.ssl.trustStorePassword");
      }
      char [] password = null;
      if (passwordString != null) {
        if (passwordString.trim().equals("")) {
          String encryptedPass = System.getenv("javax.net.ssl.trustStorePassword");
          if (!isEmpty(encryptedPass)) {
            String toDecrypt = "encrypted(" + encryptedPass + ")";
            passwordString = PasswordUtil.decrypt(toDecrypt);
            password = passwordString.toCharArray();
          }
          //read from the console
          if (isEmpty(passwordString) && consoleReader.isSupported()) {
            password = consoleReader.readPassword("Please enter password for trustStore (javax.net.ssl.trustStorePassword) : ");
          }
        } else {
          password = passwordString.toCharArray();
        }
      }
      ts.load(fis, password);
      
      // default algorithm can be changed by setting property "ssl.TrustManagerFactory.algorithm" in security properties
      TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(ts);
      trustManagers = tmf.getTrustManagers();
      // follow the security tip in java doc
      if (password != null) {
        java.util.Arrays.fill(password, ' ');
      }
    }
    return trustManagers;
  }

  private KeyManager[] getKeyManagers(Properties sysProps)
      throws KeyStoreException, FileNotFoundException, IOException,
      NoSuchAlgorithmException, CertificateException, UnrecoverableKeyException {
    KeyManager[] keyManagers = null;
    String keyStoreType = sysProps.getProperty("javax.net.ssl.keyStoreType");
    GfeConsoleReader consoleReader = GfeConsoleReaderFactory.getDefaultConsoleReader();
    if (keyStoreType == null) {
      keyStoreType = System.getProperty("javax.net.ssl.keyStoreType", KeyStore.getDefaultType());
    } else if (keyStoreType.trim().equals("")) {
      // read from console, default on empty
      if (consoleReader.isSupported()) {
        keyStoreType = consoleReader.readLine("Please enter the keyStoreType (javax.net.ssl.keyStoreType) : ");
      }
      if (isEmpty(keyStoreType)) {
        keyStoreType = KeyStore.getDefaultType();
      }
    }
    KeyStore ks = KeyStore.getInstance(keyStoreType);
    String keyStoreFilePath = sysProps.getProperty("javax.net.ssl.keyStore");
    if (keyStoreFilePath == null) {
      keyStoreFilePath = System.getProperty("javax.net.ssl.keyStore");
    }
    if (keyStoreFilePath != null) {
      if (keyStoreFilePath.trim().equals("")) {
        keyStoreFilePath = System.getenv("javax.net.ssl.keyStore");
        //read from console
        if (isEmpty(keyStoreFilePath) && consoleReader.isSupported()) {
          keyStoreFilePath = consoleReader.readLine("Please enter the keyStore location (javax.net.ssl.keyStore) : ");
        }
        if (isEmpty(keyStoreFilePath)) {
          keyStoreFilePath = System.getProperty("user.home") + System.getProperty("file.separator") + ".keystore";
        }
      }
      FileInputStream fis = null;
      fis = new FileInputStream(keyStoreFilePath);
      String passwordString = sysProps.getProperty("javax.net.ssl.keyStorePassword");
      if (passwordString == null) {
        passwordString = System.getProperty("javax.net.ssl.keyStorePassword");
      }
      char [] password = null;
      if (passwordString != null) {
        if (passwordString.trim().equals("")) {
          String encryptedPass = System.getenv("javax.net.ssl.keyStorePassword");
          if (!isEmpty(encryptedPass)) {
            String toDecrypt = "encrypted(" + encryptedPass + ")";
            passwordString = PasswordUtil.decrypt(toDecrypt);
            password = passwordString.toCharArray();
          }
          //read from the console
          if (isEmpty(passwordString) && consoleReader != null) {
            password = consoleReader.readPassword("Please enter password for keyStore (javax.net.ssl.keyStorePassword) : ");
          }
        } else {
          password = passwordString.toCharArray();
        }
      }
      ks.load(fis, password);
      // default algorithm can be changed by setting property "ssl.KeyManagerFactory.algorithm" in security properties
      KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      kmf.init(ks, password);
      keyManagers = kmf.getKeyManagers();
      // follow the security tip in java doc
      if (password != null) {
        java.util.Arrays.fill(password, ' ');
      }
    }
    return keyManagers;
  }

  private boolean isEmpty(String string) {
    return (string == null || string.trim().equals(""));
  }
  
  /**
   * Perform initialization using defaults in {@link 
   * com.gemstone.gemfire.distributed.internal.DistributionConfig}.  If any
   * values are specified in System properties, those values will be used to 
   * override the defaults.
   * <p>
   * Caller must synchronize on the SocketCreator instance.
   */
  private void initialize() {
    initialize(new DistributionConfigImpl(new Properties()));
  }
  
  /**
   * Initialize this SocketCreator using the DistributionConfig.
   * <p>
   * Caller must synchronize on the SocketCreator instance.
   */
  private void initialize(DistributionConfig config) {
    DistributionConfig conf = config;
    if (conf == null) {
      conf = new DistributionConfigImpl(new Properties());
    }

    initialize(conf.getSSLEnabled(),
               conf.getSSLRequireAuthentication(),
               readArray(conf.getSSLProtocols()),
               readArray(conf.getSSLCiphers()),
               conf.getSSLProperties());
  }
  
  // -------------------------------------------------------------------------
  //   Public methods
  // -------------------------------------------------------------------------
  
  /** Returns true if this SocketCreator is configured to use SSL. */
  public boolean useSSL() {
    return this.useSSL;
  }

  /** 
   * Return a ServerSocket possibly configured for SSL.
   *  SSL configuration is left up to JSSE properties in java.security file.
   */
  public ServerSocket createServerSocket( int nport, int backlog,
      GFLogWriter log ) throws IOException {
    return createServerSocket( nport, backlog, null, log );
  }
  
  public ServerSocket createServerSocket(int nport, int backlog,
      InetAddress bindAddr, GFLogWriter log,
      List<GatewayTransportFilter> transportFilters, int socketBufferSize)
      throws IOException {
    if (transportFilters.isEmpty()) {
      return createServerSocket(nport, backlog, bindAddr, log, socketBufferSize);
    }
    else {
      printConfig(log);
      ServerSocket result = new TransportFilterServerSocket(transportFilters);
      result.setReuseAddress(true);
      // Set the receive buffer size before binding the socket so
      // that large buffers will be allocated on accepted sockets (see
      // java.net.ServerSocket.setReceiverBufferSize javadocs)
      result.setReceiveBufferSize(socketBufferSize);
      try {
        result.bind(new InetSocketAddress(bindAddr, nport), backlog);
      }
      catch (BindException e) {
        BindException throwMe = new BindException(
            LocalizedStrings.SocketCreator_FAILED_TO_CREATE_SERVER_SOCKET_ON_0_1
                .toLocalizedString(new Object[] { bindAddr,
                    Integer.valueOf(nport) }));
        throwMe.initCause(e);
        throw throwMe;
      }
      return result;
    }
  }
  
  /** 
   * Return a ServerSocket possibly configured for SSL.
   *  SSL configuration is left up to JSSE properties in java.security file.
   */
  public ServerSocket createServerSocket( int nport, int backlog,
      InetAddress bindAddr, GFLogWriter log ) throws IOException {
    return createServerSocket( nport, backlog, null, log, -1 );
  }

  public ServerSocket createServerSocket(int nport, int backlog,
      InetAddress bindAddr, GFLogWriter log, int socketBufferSize)
      throws IOException {
    //       rw.readLock().lockInterruptibly();
//       try {
        printConfig( log );
        if ( this.useSSL ) {
          if (this.sslContext == null) {
            throw new GemFireConfigException("SSL not configured correctly, Please look at previous error");
          }
          ServerSocketFactory ssf = this.sslContext.getServerSocketFactory();
          SSLServerSocket serverSocket = (SSLServerSocket)ssf.createServerSocket();
          serverSocket.setReuseAddress(true);
          // If necessary, set the receive buffer size before binding the socket so
          // that large buffers will be allocated on accepted sockets (see
          // java.net.ServerSocket.setReceiverBufferSize javadocs)
          if (socketBufferSize != -1) {
            serverSocket.setReceiveBufferSize(socketBufferSize);
          }
          serverSocket.bind(new InetSocketAddress(bindAddr, nport), backlog);
          finishServerSocket(serverSocket);
          return serverSocket;
        } 
        else {
          //log.info("Opening server socket on " + nport, new Exception("SocketCreation"));
          ServerSocket result = new ServerSocket();
          result.setReuseAddress(true);
          // If necessary, set the receive buffer size before binding the socket so
          // that large buffers will be allocated on accepted sockets (see
          // java.net.ServerSocket.setReceiverBufferSize javadocs)
          if (socketBufferSize != -1) {
            result.setReceiveBufferSize(socketBufferSize);
          }
          try {
            result.bind(new InetSocketAddress(bindAddr, nport), backlog);
          }
          catch (BindException e) {
            BindException throwMe = new BindException(LocalizedStrings.SocketCreator_FAILED_TO_CREATE_SERVER_SOCKET_ON_0_1.toLocalizedString(new Object[] {bindAddr, Integer.valueOf(nport)}));
            throwMe.initCause(e);
            throw throwMe;
          }
          return result;
        }
//       }
//       finally {
//         rw.readLock().unlock();
//       }
  }

  /**
   * Creates or bind server socket to a random port selected
   * from tcp-port-range which is same as membership-port-range.
   * @param ba
   * @param backlog
   * @param isBindAddress
   * @param logger
   * @param tcpBufferSize
   * @return Returns the new server socket.
   * @throws IOException
   */
  public ServerSocket createServerSocketUsingPortRange(InetAddress ba, int backlog,
      boolean isBindAddress, boolean useNIO, GFLogWriter logger,
      int tcpBufferSize, int[] tcpPortRange) throws IOException {
    
    ServerSocket socket = null;
    int localPort = 0;
    int startingPort = 0;
    
    // Get a random port from range.
    Random rand = new SecureRandom();
    int portLimit = tcpPortRange[1];
    int randPort = tcpPortRange[0] + rand.nextInt(tcpPortRange[1] - tcpPortRange[0] + 1);

    startingPort = randPort;
    localPort = startingPort;

    while (true) {
      if (localPort > portLimit) {
        if (startingPort != 0) {
          localPort = tcpPortRange[0];
          portLimit = startingPort - 1;
          startingPort = 0;
        } else {
          throw new SystemConnectException(
              LocalizedStrings.TCPConduit_UNABLE_TO_FIND_FREE_PORT
                  .toLocalizedString(tcpPortRange[0], tcpPortRange[1]));
        }
      }
      try {
        if (useNIO) {
          ServerSocketChannel channl = ServerSocketChannel.open();
          socket = channl.socket();

          InetSocketAddress addr = new InetSocketAddress(isBindAddress ? ba : null, localPort);
          socket.bind(addr, backlog);
        } else {
          socket = SocketCreator.getDefaultInstance().createServerSocket(localPort, backlog, isBindAddress? ba : null, logger, tcpBufferSize);
        }
        break;
      } catch (java.net.SocketException ex) {
        if (useNIO || SocketCreator.treatAsBindException(ex)) {
          localPort++;
        } else {
          throw ex;
        }
      }
    }
    return socket;
  }

  public static boolean treatAsBindException(SocketException se) {
    if(se instanceof BindException) {
      return true;
    }
    final String msg = se.getMessage();
    return (msg != null && msg.contains("Invalid argument: listen failed"));
  }

  /**
   * Return a client socket. This method is used by client/server clients.
   */
  public Socket connectForClient(String host, int port, GFLogWriter log,
      int timeout) throws IOException {
    return connect(InetAddress.getByName(host), port, log, timeout,
        null, true, -1, this.useSSL);
  }

  /**
   * Return a client socket. This method is used by client/server clients.
   */
  public Socket connectForClient(String host, int port, GFLogWriter log,
      int timeout, int socketBufferSize) throws IOException {
    return connect(InetAddress.getByName(host), port, log, timeout,
        null, true, socketBufferSize, this.useSSL);
  }

  /**
   * Return a client socket. This method is used by peers.
   */
  public Socket connectForServer(InetAddress inetadd, int port,
      GFLogWriter log) throws IOException {
    return connect(inetadd, port, log, 0, null, false, -1, this.useSSL);
  }

  /**
   * Return a client socket. This method is used by peers.
   */
  public Socket connectForServer(InetAddress inetadd, int port,
      GFLogWriter log, int socketBufferSize) throws IOException {
    return connect(inetadd, port, log, 0, null, false, socketBufferSize,
        this.useSSL);
  }

  /**
   * Return a client socket, timing out if unable to connect and timeout > 0 (millis).
   * The parameter <i>timeout</i> is ignored if SSL is being used, as there is no
   * timeout argument in the ssl socket factory
   */
  @Override
  public Socket connect(InetAddress inetadd, int port, GFLogWriter log,
      int timeout, ConnectionWatcher optionalWatcher, boolean clientSide)
      throws IOException {
    return connect(inetadd, port, log, timeout, optionalWatcher, clientSide,
        -1, this.useSSL);
  }

  @Override
  /**
   * Return a client socket, timing out if unable to connect and timeout > 0 (millis).
   * The parameter <i>timeout</i> is ignored if SSL is being used, as there is no
   * timeout argument in the ssl socket factory
   */
  public Socket connect(InetAddress inetadd, int port, GFLogWriter log,
      int timeout, ConnectionWatcher optionalWatcher, boolean clientSide,
      int socketBufferSize, boolean sslConnection) throws IOException {
      Socket socket = null;
      SocketAddress sockaddr = new InetSocketAddress(inetadd, port);
      printConfig( log );
      try {
        if (sslConnection) {
          if (this.sslContext == null) {
            throw new GemFireConfigException("SSL not configured correctly, Please look at previous error");
          }
          SocketFactory sf = this.sslContext.getSocketFactory();
          socket = sf.createSocket();
          
          // If this is a client socket, optionally enable SO_KEEPALIVE in the OS
          // network protocol.
          if (clientSide) {
            socket.setKeepAlive(ENABLE_TCP_KEEP_ALIVE);
          }

          // If necessary, set the receive buffer size before connecting the
          // socket so that large buffers will be allocated on accepted sockets
          // (see java.net.Socket.setReceiverBufferSize javadocs for details)
          if (socketBufferSize != -1) {
            socket.setReceiveBufferSize(socketBufferSize);
          }

          if (optionalWatcher != null) {
            optionalWatcher.beforeConnect(socket);
          }
          if (timeout > 0) {
            socket.connect( sockaddr, timeout );
          }
          else {
            socket.connect( sockaddr );
          }
          configureClientSSLSocket( socket, log );
          return socket;
        } 
        else {
          if (clientSide && this.clientSocketFactory != null) {
            socket = this.clientSocketFactory.createSocket( inetadd, port );
          } else {
            socket = new Socket( );

            // If this is a client socket, optionally enable SO_KEEPALIVE in the
            // OS network protocol.
            if (clientSide) {
              socket.setKeepAlive(ENABLE_TCP_KEEP_ALIVE);
            }

            // If necessary, set the receive buffer size before connecting the
            // socket so that large buffers will be allocated on accepted sockets
            // (see java.net.Socket.setReceiverBufferSize javadocs for details)
            if (socketBufferSize != -1) {
              socket.setReceiveBufferSize(socketBufferSize);
            }

            if (optionalWatcher != null) {
              optionalWatcher.beforeConnect(socket);
            }
            if (timeout > 0) {
              socket.connect(sockaddr, timeout);
            }
            else {
              socket.connect(sockaddr);
            }
          }
          return socket;
        }
      }
      finally {
        if (optionalWatcher != null) {
          optionalWatcher.afterConnect(socket);
        }
      }
//       }
//       finally {
//         rw.readLock().unlock();
//       }
  }
  
  /** has the isReachable method been looked up already? */
  volatile boolean isReachableChecked;
  
  /** InetAddress.isReachable() is in v1.5 and later */
  volatile Method isReachableMethod;
  
  public boolean isHostReachable(InetAddress host) {
    boolean result = true;
    try {
      Method m = null;
      if (isReachableChecked) {
        m = isReachableMethod;
      }
      else {
        // deadcoded - InetAddress.isReachable uses the ECHO port
        // if we don't have root permission, and the ECHO port may
        // be blocked
        //m = InetAddress.class.getMethod("isReachable", new Class[] { int.class });
        //isReachableMethod = m;
        isReachableChecked = true;
      }
      if (m != null) {
        result = ((Boolean)m.invoke(host, new Object[] {Integer.valueOf(250)})).booleanValue();
        return result;
      }
    }
    catch (InvocationTargetException e) {
    }
//    catch (NoSuchMethodException e) {
//    }
    catch (IllegalAccessException e) {
    }
    // any other bright ideas?  attempts to connect a socket to a missing
    // machine may hang, so don't try the echo port or anything requiring
    // full Sockets
    return result;
  }
  
  /** Will be a server socket... this one simply registers the listeners. */
  public void configureServerSSLSocket(Socket socket,
      GFLogWriter log) throws IOException {
//       rw.readLock().lockInterruptibly();
//       try {
        if (socket instanceof SSLSocket) {
          SSLSocket sslSocket = (SSLSocket)socket;
          try {
            sslSocket.startHandshake();
            if (log != null) { // fix this to be logwriter level...
              SSLSession session = sslSocket.getSession();
              Certificate[] peer = session.getPeerCertificates();
              log.info(LocalizedStrings.SocketCreator_SSL_CONNECTION_FROM_PEER_0,
                  ((X509Certificate)peer[0]).getSubjectDN());
            } // ...if
          }
          catch (SSLPeerUnverifiedException ex) {
            if (this.needClientAuth) {
              if (log != null) log.severe(LocalizedStrings.SocketCreator_SSL_ERROR_IN_AUTHENTICATING_PEER_0_1, new Object[] { socket.getInetAddress(), Integer.valueOf(socket.getPort())}, ex );
              throw ex;
            }
          }
          catch (SSLException ex) {
            if (log != null) log.severe(LocalizedStrings.SocketCreator_SSL_ERROR_IN_CONNECTING_TO_PEER_0_1, new Object[] { socket.getInetAddress(), Integer.valueOf(socket.getPort())}, ex );
            throw ex;
          }
        } // ...if
// }
// finally {
// rw.readLock().unlock();
// }
  }
  
  // -------------------------------------------------------------------------
  //   Private implementation methods
  // -------------------------------------------------------------------------
  
  /** Configure the SSLServerSocket based on this SocketCreator's settings. */
  private void finishServerSocket(SSLServerSocket serverSocket) throws IOException {
    serverSocket.setUseClientMode( false );
    if ( this.needClientAuth ) {  
      //serverSocket.setWantClientAuth( true );
      serverSocket.setNeedClientAuth( true );
    }
    serverSocket.setEnableSessionCreation( true );
    
    // restrict cyphers
    if ( ! "any".equalsIgnoreCase( this.protocols[0] ) ) {
      serverSocket.setEnabledProtocols( this.protocols );
    }
    if ( ! "any".equalsIgnoreCase( this.ciphers[0] ) ) {
      serverSocket.setEnabledCipherSuites( this.ciphers );
    }
  }
  
  /** 
   * When a socket is accepted from a server socket, it should be passed to 
   * this method for SSL configuration.
   */
  private void configureClientSSLSocket(Socket socket,
      GFLogWriter log) throws IOException {
    if ( socket instanceof SSLSocket ) {
      SSLSocket sslSocket = (SSLSocket) socket;
      
      sslSocket.setUseClientMode( true );
      sslSocket.setEnableSessionCreation( true );
      
      // restrict cyphers
      if ( this.protocols != null && !"any".equalsIgnoreCase(this.protocols[0]) ) {
        sslSocket.setEnabledProtocols( this.protocols );
      }
      if ( this.ciphers != null && !"any".equalsIgnoreCase(this.ciphers[0]) ) {
        sslSocket.setEnabledCipherSuites( this.ciphers );
      }

      try {
        sslSocket.startHandshake();
        if (log != null && log.infoEnabled()) {
          SSLSession session = sslSocket.getSession();
          Certificate[] peer = session.getPeerCertificates();
          log.info(LocalizedStrings.SocketCreator_SSL_CONNECTION_FROM_PEER_0,
              ((X509Certificate)peer[0]).getSubjectDN());
        }
      }
      catch (SSLPeerUnverifiedException ex) {
        if (this.needClientAuth) {
          if(log !=null) log.severe(LocalizedStrings.SocketCreator_SSL_ERROR_IN_AUTHENTICATING_PEER, ex);
          throw ex;
        }
      }
      catch (SSLException ex) {
        if(log != null) log.severe(LocalizedStrings.SocketCreator_SSL_ERROR_IN_CONNECTING_TO_PEER_0_1,
          new Object[] {socket.getInetAddress(), Integer.valueOf(socket.getPort())}, ex);
        throw ex;
      }
      // catch ( IOException e ) {
      // if ( this.needClientAuth ) {
      // logSevere( log, "SSL Error in authenticating peer.", e );
      // throw e;
      // }
      // else {
      // logWarning( log, "SSL Error in authenticating peer.", e );
      //          }
      //        }
    }
  }

  /** Print current configured state to log. */
  private void printConfig( GFLogWriter log ) {
    if ( ! configShown && log.fineEnabled()) {
      configShown = true;
      StringBuilder sb = new StringBuilder();
      sb.append( "SSL Configuration: \n" );
      sb.append( "  ssl-enabled = " + this.useSSL ).append( "\n" );
      // add other options here....
      for (String key: System.getProperties().stringPropertyNames()) { // fix for 46822
        if ( key.startsWith( "javax.net.ssl" ) ) {
          sb.append( "  " ).append( key ).append( " = " ).append( System.getProperty( key ) ).append( "\n" );
        }
      }
      log.fine( sb.toString() );
    }
  }

  /** Read an array of values from a string, whitespace separated. */
  private static String[] readArray(String text) {
    if (text == null || text.trim().equals("")) {
      return null;
    }

    StringTokenizer st = new StringTokenizer(text);
    ArrayList<String> v = new ArrayList<String>();
    while (st.hasMoreTokens()) {
      v.add(st.nextToken());
    }
    return v.toArray(new String[v.size()]);
  }

  /**
   * Closes the specified socket in a background thread and waits a limited
   * amount of time for the close to complete. In some cases we see close
   * hang (see bug 33665).
   * Made public so it can be used from CacheClientProxy.
   * @param sock the socket to close
   * @param log the log writer to use in the thread (must not be null)
   * @param who who the socket is connected to
   * @param extra an optional Runnable with stuff to execute in the async thread
   */
  public static void asyncClose(final Socket sock, final LogWriterI18n log, String who, final Runnable extra) {
    if (sock == null || sock.isClosed()) {
      return;
    }
    try {
    ThreadGroup tg = LogWriterImpl.createThreadGroup("Socket asyncClose", log);

    Thread t = new Thread(tg, new Runnable() {
        public void run() {
          if (extra != null) {
            extra.run();
          }
          inlineClose(sock, log);
        }
      }, "AsyncSocketCloser for " + who);
    t.setDaemon(true);
    try {
      t.start();
    } catch (OutOfMemoryError ignore) {
      // If we can't start a thread to close the socket just do it inline.
      // See bug 50573.
      inlineClose(sock, log);
      return;
    }
    try {
      // [bruce] if the network fails, this will wait the full amount of time
      // on every close, so it must be kept very short.  it was 750ms before,
      // causing frequent hangs in net-down hydra tests
      t.join(50/*ms*/);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
    }
    catch (Error e) {
      if (SystemFailure.isJVMFailureError(e)) {
        SystemFailure.initiateFailure(e);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
      }
      throw e;
    }
  }


  /**
   * Closes the specified socket
   * @param sock the socket to close
   * @param log the log writer to use (must not be null)
   */
  public static void inlineClose(final Socket sock, final LogWriterI18n log) {
    
    // the next two statements are a mad attempt to fix bug
    // 36041 - segv in jrockit in pthread signaling code.  This
    // seems to alleviate the problem.
    try {
      sock.shutdownInput();
      sock.shutdownOutput();
    }
    catch (Exception e) {
    }
    try {
      sock.close();
    } catch (IOException ignore) {
    } 
    catch (java.security.ProviderException pe) {
      // some ssl implementations have trouble with termination and throw
      // this exception.  See bug #40783
    }
    catch (Error e) {
      if (SystemFailure.isJVMFailureError(e)) {
        SystemFailure.initiateFailure(e);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw e;
      }
      // Whenever you catch Error or Throwable, you must also
      // check for fatal JVM error (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      // Sun's NIO implementation has been known to throw Errors
      // that are caused by IOExceptions.  If this is the case, it's
      // okay.
      if (e.getCause() instanceof IOException) {
        // okay...

      } else {
        throw e;
      }
    }
  }
  
  protected void initializeClientSocketFactory() {
    this.clientSocketFactory = null;
    String className = System.getProperty("gemfire.clientSocketFactory");
    if (className != null) {
      Object o;
      try {
        Class<?> c = ClassPathLoader.getLatest().forName(className);
        o = c.newInstance();
      }
      catch (Exception e) {
        // No cache exists yet, so this can't be logged.
        String s = "An unexpected exception occurred while instantiating a "
            + className + ": " + e;
        throw new IllegalArgumentException(s);
      }
      if (o instanceof ClientSocketFactory) {
        this.clientSocketFactory = (ClientSocketFactory) o;
      } else {
        String s = "Class \"" + className + "\" is not a ClientSocketFactory";
        throw new IllegalArgumentException(s);
      }
    }
  }
  
  public void initializeTransportFilterClientSocketFactory(GatewaySender sender) {
    this.clientSocketFactory = new TransportFilterSocketFactory()
        .setGatewayTransportFilters(sender.getGatewayTransportFilters());
  }
  
//   // -------------------------------------------------------------------------
//   //   dummy ReadWriteLock impl's used to compare performance impact of 
//   //     WriterPreferenceReadWriteLock usage
//   // -------------------------------------------------------------------------
  
//   private class RW implements ReadWriteLock {
//     public Sync readLock() {
//       return new S();
//     }
//     public Sync writeLock() {
//       return new S();
//     }
//   }
//   private class S implements Sync {
//     public void acquire() throws InterruptedException {}
//     public boolean attempt(long msecs) throws InterruptedException { return true; }
//     public void release() {}
//   }
  

  /** returns a set of the non-loopback InetAddresses for this machine */
  public static Set<InetAddress> getMyAddresses(boolean includeLocal) {
    try {
      return ClientSharedUtils.getMyAddresses(includeLocal);
    } catch (SocketException se) {
      throw new IllegalArgumentException(
          LocalizedStrings.StartupMessage_UNABLE_TO_EXAMINE_NETWORK_INTERFACES
              .toLocalizedString(), se);
    }
  }

  /**
   * Returns true if host matches the LOCALHOST or any valid bind addresses of
   * this machine.
   */
  public static boolean isLocalHost(Object host) {
    if (host instanceof InetAddress) {
      final InetAddress addr = (InetAddress)host;
      if (InetAddressUtil.LOCALHOST.equals(host)) {
        return true;
      }
      else {
        try {
          Enumeration<NetworkInterface> en = NetworkInterface
              .getNetworkInterfaces();
          while (en.hasMoreElements()) {
            NetworkInterface i = en.nextElement();
            for (Enumeration<InetAddress> en2 = i.getInetAddresses(); en2
                .hasMoreElements();) {
              if (addr.equals(en2.nextElement())) {
                return true;
              }
            }
          }
          return false;
        } catch (SocketException e) {
          throw new IllegalArgumentException(
              LocalizedStrings.InetAddressUtil_UNABLE_TO_QUERY_NETWORK_INTERFACE
                  .toLocalizedString(), e);
        }
      }
    }
    else {
      return isLocalHost(toInetAddress(host.toString()));
    }
  }

  /** 
   * Converts the string host to an instance of InetAddress.  Returns null if
   * the string is empty.  Fails Assertion if the conversion would result in
   * <code>java.lang.UnknownHostException</code>.
   * <p>
   * Any leading slashes on host will be ignored.
   *
   * @param   host  string version the InetAddress
   * @return  the host converted to InetAddress instance
   */
  public static InetAddress toInetAddress(String host) {
    if (host == null || host.length() == 0) {
      return null;
    }
    try {
      if (host.indexOf("/") > -1) {
        return InetAddress.getByName(host.substring(host.indexOf("/") + 1));
      }
      else {
        return InetAddress.getByName(host);
      }
    } catch (java.net.UnknownHostException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }
}

