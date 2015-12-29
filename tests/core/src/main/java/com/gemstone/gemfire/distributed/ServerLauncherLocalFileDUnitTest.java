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

package com.gemstone.gemfire.distributed;

import com.gemstone.gemfire.internal.process.ProcessControllerFactory;

import dunit.SerializableRunnable;

/**
 * Subclass of ServerLauncherLocalDUnitTest which forces the code to not find 
 * the Attach API which is in the JDK tools.jar.  As a result ServerLauncher
 * ends up using the FileProcessController implementation.
 *
 * @author Kirk Lund
 * @since 8.0
 */
@SuppressWarnings("serial")
public class ServerLauncherLocalFileDUnitTest extends ServerLauncherLocalDUnitTest {

  public ServerLauncherLocalFileDUnitTest(String name) {
    super(name);
  }

  @Override
  protected void subSetUp2() throws Exception {
    disableAttachApi();
    invokeInEveryVM(new SerializableRunnable("disableAttachApi") {
      @Override
      public void run() {
        disableAttachApi();
      }
    });
  }
  
  @Override
  protected void subTearDown2() throws Exception {   
    enableAttachApi();
    invokeInEveryVM(new SerializableRunnable("enableAttachApi") {
      @Override
      public void run() {
        enableAttachApi();
      }
    });
  }
  
  @Override
  public void testIsAttachAPIFound() throws Exception {
    final ProcessControllerFactory factory = new ProcessControllerFactory();
    assertFalse(factory.isAttachAPIFound());

    invokeInEveryVM(new SerializableRunnable("isAttachAPIFound") {
      @Override
      public void run() {
        final ProcessControllerFactory factory = new ProcessControllerFactory();
        assertFalse(factory.isAttachAPIFound());
      }
    });
  }
  
  private static void disableAttachApi() {
    System.setProperty(ProcessControllerFactory.PROPERTY_DISABLE_ATTACH_API, "true");
  }

  private static void enableAttachApi() {
    System.clearProperty(ProcessControllerFactory.PROPERTY_DISABLE_ATTACH_API);
  }
}
