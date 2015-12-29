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

import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;

/**
 * Test that DistributionConfigImpl handles SSL options correctly.
 * 
 */
public class SSLConfigTest extends TestCase {

  private static final Properties SSL_PROPS_MAP     = new Properties();
  private static final Properties JMX_SSL_PROPS_MAP = new Properties();
  private static final Properties JMX_SSL_PROPS_SUBSET_MAP = new Properties();

  static {
    // SSL Properties for GemFire in-cluster connections
    SSL_PROPS_MAP.put("javax.net.ssl.keyStoreType", "jks");
    SSL_PROPS_MAP.put("javax.net.ssl.keyStore", "/export/gemfire-configs/gemfire.keystore");
    SSL_PROPS_MAP.put("javax.net.ssl.keyStorePassword", "password");
    SSL_PROPS_MAP.put("javax.net.ssl.trustStore", "/export/gemfire-configs/gemfire.truststore");
    SSL_PROPS_MAP.put("javax.net.ssl.trustStorePassword", "");

    // SSL Properties for GemFire JMX Manager connections
    JMX_SSL_PROPS_MAP.put("javax.net.ssl.keyStoreType", "jks");
    JMX_SSL_PROPS_MAP.put("javax.net.ssl.keyStore", "/export/gemfire-configs/manager.keystore");
    JMX_SSL_PROPS_MAP.put("javax.net.ssl.keyStorePassword", "manager-key-password");
    JMX_SSL_PROPS_MAP.put("javax.net.ssl.trustStore", "/export/gemfire-configs/manager.truststore");
    JMX_SSL_PROPS_MAP.put("javax.net.ssl.trustStorePassword", "manager-trust-password");

    // Partially over-ridden SSL Properties for GemFire JMX Manager connections
    JMX_SSL_PROPS_SUBSET_MAP.put("javax.net.ssl.keyStore", "/export/gemfire-configs/manager.keystore");
    JMX_SSL_PROPS_SUBSET_MAP.put("javax.net.ssl.trustStore", "/export/gemfire-configs/manager.truststore");
  }

  public String name = null;
  
  public SSLConfigTest( String name ) {
    super( name );
    this.name = name;
  }
  
  public void setUp( ) throws Exception {
    System.out.println( "\n\n########## setup " + name + " ############\n\n" );
  }
  
  public void tearDown( ) throws Exception {
    System.out.println( "\n\n########## teardown " + name + " ############\n\n" );
  }
  
  //----- test methods ------

  public void testMCastPort() throws Exception {
    Properties props = new Properties( );
    // default mcast-port is not 0.
    props.setProperty( "ssl-enabled", "true" );
    
    try {
      new DistributionConfigImpl( props );
    } catch ( IllegalArgumentException e ) {
      if (! e.toString().matches( ".*Could not set \"ssl-enabled.*" ) ) {
        throw new Exception( "did not get expected exception, got this instead...", e );
      }
    }
    
    props.setProperty( "mcast-port", "0" );
    new DistributionConfigImpl( props );
  }
  
  public void testConfigCopy( ) throws Exception {
    boolean sslenabled = false;
    String sslprotocols = "any";
    String sslciphers = "any";
    boolean requireAuth = true;
    
    DistributionConfigImpl config = new DistributionConfigImpl( new Properties() );
    isEqual( config.getSSLEnabled(), sslenabled );
    isEqual( config.getSSLProtocols(), sslprotocols );
    isEqual( config.getSSLCiphers(), sslciphers );
    isEqual( config.getSSLRequireAuthentication(), requireAuth );
    
    Properties props = new Properties();
    sslciphers = "RSA_WITH_GARBAGE";
    props.setProperty("ssl-ciphers", sslciphers );

    config = new DistributionConfigImpl( props );
    isEqual( config.getSSLEnabled(), sslenabled );
    isEqual( config.getSSLProtocols(), sslprotocols );
    isEqual( config.getSSLCiphers(), sslciphers );
    isEqual( config.getSSLRequireAuthentication(), requireAuth );
    
    sslprotocols = "SSLv7";
    props.setProperty("ssl-protocols", sslprotocols );

    config = new DistributionConfigImpl( props );
    isEqual( config.getSSLEnabled(), sslenabled );
    isEqual( config.getSSLProtocols(), sslprotocols );
    isEqual( config.getSSLCiphers(), sslciphers );
    isEqual( config.getSSLRequireAuthentication(), requireAuth );

    requireAuth = false;
    props.setProperty("ssl-require-authentication", String.valueOf( requireAuth ) );

    config = new DistributionConfigImpl( props );
    isEqual( config.getSSLEnabled(), sslenabled );
    isEqual( config.getSSLProtocols(), sslprotocols );
    isEqual( config.getSSLCiphers(), sslciphers );
    isEqual( config.getSSLRequireAuthentication(), requireAuth );

    sslenabled = true;
    props.setProperty("ssl-enabled", String.valueOf( sslenabled ) );
    props.setProperty("mcast-port", "0" );

    config = new DistributionConfigImpl( props );
    isEqual( config.getSSLEnabled(), sslenabled );
    isEqual( config.getSSLProtocols(), sslprotocols );
    isEqual( config.getSSLCiphers(), sslciphers );
    isEqual( config.getSSLRequireAuthentication(), requireAuth );
    
    config = new DistributionConfigImpl( config );
    isEqual( config.getSSLEnabled(), sslenabled );
    isEqual( config.getSSLProtocols(), sslprotocols );
    isEqual( config.getSSLCiphers(), sslciphers );
    isEqual( config.getSSLRequireAuthentication(), requireAuth );
  }

  public void testManagerDefaultConfig() throws Exception {
    boolean sslenabled = false;
    String sslprotocols = "any";
    String sslciphers = "any";
    boolean requireAuth = true;

    boolean jmxManagerSslenabled = false;
    String jmxManagerSslprotocols = "any";
    String jmxManagerSslciphers = "any";
    boolean jmxManagerSslRequireAuth = true;

    DistributionConfigImpl config = new DistributionConfigImpl( new Properties() );
    isEqual( config.getSSLEnabled(), sslenabled );
    isEqual( config.getSSLProtocols(), sslprotocols );
    isEqual( config.getSSLCiphers(), sslciphers );
    isEqual( config.getSSLRequireAuthentication(), requireAuth );

    isEqual( config.getJmxManagerSSL(), jmxManagerSslenabled );
    isEqual( config.getJmxManagerSSLProtocols(), jmxManagerSslprotocols );
    isEqual( config.getJmxManagerSSLCiphers(), jmxManagerSslciphers );
    isEqual( config.getJmxManagerSSLRequireAuthentication(), jmxManagerSslRequireAuth );
  }

  public void testManagerConfig() throws Exception {
    boolean sslenabled = false;
    String  sslprotocols = "any";
    String  sslciphers = "any";
    boolean requireAuth = true;

    boolean jmxManagerSslenabled = true;
    String  jmxManagerSslprotocols = "SSLv7";
    String  jmxManagerSslciphers = "RSA_WITH_GARBAGE";
    boolean jmxManagerSslRequireAuth = true;

    Properties gemFireProps = new Properties();
    gemFireProps.put(DistributionConfig.SSL_ENABLED_NAME, String.valueOf(sslenabled));
    gemFireProps.put(DistributionConfig.SSL_PROTOCOLS_NAME, sslprotocols);
    gemFireProps.put(DistributionConfig.SSL_CIPHERS_NAME, sslciphers);
    gemFireProps.put(DistributionConfig.SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(requireAuth));

    gemFireProps.put(DistributionConfig.JMX_MANAGER_SSL_NAME, String.valueOf(jmxManagerSslenabled));
    gemFireProps.put(DistributionConfig.JMX_MANAGER_SSL_PROTOCOLS_NAME, jmxManagerSslprotocols);
    gemFireProps.put(DistributionConfig.JMX_MANAGER_SSL_CIPHERS_NAME, jmxManagerSslciphers);
    gemFireProps.put(DistributionConfig.JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(jmxManagerSslRequireAuth));

    DistributionConfigImpl config = new DistributionConfigImpl( gemFireProps );
    isEqual( config.getSSLEnabled(), sslenabled );
    isEqual( config.getSSLProtocols(), sslprotocols );
    isEqual( config.getSSLCiphers(), sslciphers );
    isEqual( config.getSSLRequireAuthentication(), requireAuth );

    isEqual( config.getJmxManagerSSL(), jmxManagerSslenabled );
    isEqual( config.getJmxManagerSSLProtocols(), jmxManagerSslprotocols );
    isEqual( config.getJmxManagerSSLCiphers(), jmxManagerSslciphers );
    isEqual( config.getJmxManagerSSLRequireAuthentication(), jmxManagerSslRequireAuth );
  }

  public void testCustomizedManagerSslConfig() throws Exception {
    boolean sslenabled = false;
    String  sslprotocols = "any";
    String  sslciphers = "any";
    boolean requireAuth = true;

    boolean jmxManagerSslenabled = true;
    String  jmxManagerSslprotocols = "SSLv7";
    String  jmxManagerSslciphers = "RSA_WITH_GARBAGE";
    boolean jmxManagerSslRequireAuth = true;

    Properties gemFireProps = new Properties();
    gemFireProps.put(DistributionConfig.SSL_ENABLED_NAME, String.valueOf(sslenabled));
    gemFireProps.put(DistributionConfig.SSL_PROTOCOLS_NAME, sslprotocols);
    gemFireProps.put(DistributionConfig.SSL_CIPHERS_NAME, sslciphers);
    gemFireProps.put(DistributionConfig.SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(requireAuth));

    gemFireProps.put(DistributionConfig.JMX_MANAGER_SSL_NAME, String.valueOf(jmxManagerSslenabled));
    gemFireProps.put(DistributionConfig.JMX_MANAGER_SSL_PROTOCOLS_NAME, jmxManagerSslprotocols);
    gemFireProps.put(DistributionConfig.JMX_MANAGER_SSL_CIPHERS_NAME, jmxManagerSslciphers);
    gemFireProps.put(DistributionConfig.JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(jmxManagerSslRequireAuth));

    gemFireProps.putAll(getGfSecurityProperties(false /*partialJmxSslConfigOverride*/));

    DistributionConfigImpl config = new DistributionConfigImpl( gemFireProps );
    isEqual( config.getSSLEnabled(), sslenabled );
    isEqual( config.getSSLProtocols(), sslprotocols );
    isEqual( config.getSSLCiphers(), sslciphers );
    isEqual( config.getSSLRequireAuthentication(), requireAuth );

    isEqual( config.getJmxManagerSSL(), jmxManagerSslenabled );
    isEqual( config.getJmxManagerSSLProtocols(), jmxManagerSslprotocols );
    isEqual( config.getJmxManagerSSLCiphers(), jmxManagerSslciphers );
    isEqual( config.getJmxManagerSSLRequireAuthentication(), jmxManagerSslRequireAuth );

    Properties sslProperties = config.getSSLProperties();
    isEqual( sslProperties, SSL_PROPS_MAP );

    Properties jmxSSLProperties = config.getJmxSSLProperties();
    isEqual( jmxSSLProperties, JMX_SSL_PROPS_MAP );
  }

  public void testPartialCustomizedManagerSslConfig() throws Exception {
    boolean sslenabled = false;
    String  sslprotocols = "any";
    String  sslciphers = "any";
    boolean requireAuth = true;

    boolean jmxManagerSslenabled = true;
    String  jmxManagerSslprotocols = "SSLv7";
    String  jmxManagerSslciphers = "RSA_WITH_GARBAGE";
    boolean jmxManagerSslRequireAuth = true;

    Properties gemFireProps = new Properties();
    gemFireProps.put(DistributionConfig.SSL_ENABLED_NAME, String.valueOf(sslenabled));
    gemFireProps.put(DistributionConfig.SSL_PROTOCOLS_NAME, sslprotocols);
    gemFireProps.put(DistributionConfig.SSL_CIPHERS_NAME, sslciphers);
    gemFireProps.put(DistributionConfig.SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(requireAuth));

    gemFireProps.put(DistributionConfig.JMX_MANAGER_SSL_NAME, String.valueOf(jmxManagerSslenabled));
    gemFireProps.put(DistributionConfig.JMX_MANAGER_SSL_PROTOCOLS_NAME, jmxManagerSslprotocols);
    gemFireProps.put(DistributionConfig.JMX_MANAGER_SSL_CIPHERS_NAME, jmxManagerSslciphers);
    gemFireProps.put(DistributionConfig.JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(jmxManagerSslRequireAuth));

    gemFireProps.putAll(getGfSecurityProperties(true /*partialJmxSslConfigOverride*/));

    DistributionConfigImpl config = new DistributionConfigImpl( gemFireProps );
    isEqual( config.getSSLEnabled(), sslenabled );
    isEqual( config.getSSLProtocols(), sslprotocols );
    isEqual( config.getSSLCiphers(), sslciphers );
    isEqual( config.getSSLRequireAuthentication(), requireAuth );

    isEqual( config.getJmxManagerSSL(), jmxManagerSslenabled );
    isEqual( config.getJmxManagerSSLProtocols(), jmxManagerSslprotocols );
    isEqual( config.getJmxManagerSSLCiphers(), jmxManagerSslciphers );
    isEqual( config.getJmxManagerSSLRequireAuthentication(), jmxManagerSslRequireAuth );

    Properties sslProperties = config.getSSLProperties();
    isEqual( sslProperties, SSL_PROPS_MAP );

    Properties jmxSSLProperties = config.getJmxSSLProperties();
    Properties propsToVerifyAgainst = new Properties();
    propsToVerifyAgainst.putAll(SSL_PROPS_MAP);
    propsToVerifyAgainst.putAll(JMX_SSL_PROPS_SUBSET_MAP); // over-ride some props for JMX

    isEqual( jmxSSLProperties, propsToVerifyAgainst );
  }

  private static Properties getGfSecurityProperties(boolean partialJmxSslConfigOverride) {
    Properties gfSecurityProps = new Properties();

    Set<Entry<Object, Object>> entrySet = SSL_PROPS_MAP.entrySet();
    for (Entry<Object, Object> entry : entrySet) {
      gfSecurityProps.put(entry.getKey(), entry.getValue());
    }

    if (partialJmxSslConfigOverride) {
      entrySet = JMX_SSL_PROPS_SUBSET_MAP.entrySet();
    } else {
      entrySet = JMX_SSL_PROPS_MAP.entrySet();
    }
    for (Entry<Object, Object> entry : entrySet) {
      // Add "-jmx" suffix for JMX Manager properties.
      gfSecurityProps.put(entry.getKey() + DistributionConfig.JMX_SSL_PROPS_SUFFIX, entry.getValue());
    }

    return gfSecurityProps;
  }

  public void isEqual( boolean a, boolean e ) throws AssertionFailedError {
    assertEquals( e, a );
  }
  
  public void isEqual( Object a, Object e ) throws AssertionFailedError {
    assertEquals( e, a );
  }
  
}
