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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;

import com.gemstone.gemfire.cache.client.internal.locator.LocatorStatusRequest;
import com.gemstone.gemfire.cache.client.internal.locator.LocatorStatusResponse;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.LocalLogWriter;
import com.gemstone.gemfire.internal.LogWriterImpl;
import com.gemstone.gemfire.internal.cache.InternalCache;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * The ServerLocatorJUnitTest class is a test suite of test cases testing the contract and functionality of the
 * ServerLocator class.
 * </p>
 * @author John Blum
 * @see com.gemstone.gemfire.distributed.internal.ServerLocator
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since 7.0
 */
// TODO Dan, write more unit tests for this class...
public class ServerLocatorJUnitTest {

  protected ServerLocator createServerLocator() throws IOException {
    return new TestServerLocator();
  }

  @Test
  public void testProcessRequestProcessesLocatorStatusRequest() throws IOException {
    final ServerLocator serverLocator = createServerLocator();

    final Object response = serverLocator.processRequest(new LocatorStatusRequest());
    System.out.println("response="+response);
    assertTrue(response instanceof LocatorStatusResponse);
  }

  protected static class TestServerLocator extends ServerLocator {
    TestServerLocator() throws IOException {
      super();
    }
    @Override
    protected boolean readyToProcessRequests() {
      return true;
    }
    @Override
    LogWriterI18n getLogWriterI18n() {
      return new LocalLogWriter(LogWriterImpl.NONE_LEVEL);
    }
  }

}
