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
package dunit.tests;

import dunit.*;

/**
 * This class tests the distributed unit testing framework's
 * facilities for invoking methods on clients.
 */
public class HelloTest extends DistributedTestCase {

  public HelloTest(String name) {
    super(name);
  }

  ////////  Test Methods

  /**
   * Tells clients to say hello.
   */
  public void testHello() {
    int hostCount = Host.getHostCount();
    for (int i = 0; i < hostCount; i++) {
      Host host = Host.getHost(i);
//      int systemCount = host.getSystemCount();
      int vmCount = host.getVMCount();
      for (int j = 0; j < vmCount; j++) {
        VM vm = host.getVM(j);
        Object result = vm.invoke( HelloTest.class, "sayHello" );
        hydra.Log.getLogWriter().info( "HelloTest got result " + result );
      }
    }
  }

  /**
   * Accessed via reflection.  DO NOT REMOVE
   *
   */
  protected static void sayHello() {
    hydra.Log.getLogWriter().info( "HELLO WORLD!" );
  }

  /**
   * Tests the {@link VM}, etc. stuff
   */
  public void testDumpInfo() {
    DumpInfo.dumpInfo();
  }

}
