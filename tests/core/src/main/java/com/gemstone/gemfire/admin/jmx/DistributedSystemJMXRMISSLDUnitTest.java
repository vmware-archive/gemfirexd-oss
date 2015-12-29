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
package com.gemstone.gemfire.admin.jmx;

//import com.gemstone.gemfire.admin.*;
//import com.gemstone.gemfire.admin.jmx.internal.*;
//import com.gemstone.gemfire.internal.LocalLogWriter;
//import com.gemstone.gemfire.internal.LogWriterImpl;

//import dunit.Host;
//import mx4j.tools.remote.rmi.SSLRMIClientSocketFactory;

//import java.io.File;
//import java.rmi.server.RMIClientSocketFactory;
//import java.rmi.server.RMIServerSocketFactory;
//import java.util.Map;
//import java.util.HashMap;
//import javax.management.*;
//import javax.management.remote.*;
//import javax.management.remote.rmi.RMIConnectorServer;

/**
 * Tests the functionality of the <code>DistributedSystem</code> admin
 * interface via JMX MBeans accessed remotely using RMI secured with SSL.
 *
 * @author Kirk Lund
 * @since 3.5
 */
public class DistributedSystemJMXRMISSLDUnitTest 
  extends DistributedSystemJMXDUnitTest {

  /** The URL on which the RMI agent runs */
//  private String urlString;

  ////////  Constructors

  /**
   * Creates a new <code>DistributedSystemJMXRMISSLDUnitTest</code>
   */
  public DistributedSystemJMXRMISSLDUnitTest(String name) {
    super(name);
  }

  protected boolean isSSL() {
    return true;
  }
}
