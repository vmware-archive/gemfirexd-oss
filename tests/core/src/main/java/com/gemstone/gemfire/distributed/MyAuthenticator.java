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

import java.net.InetAddress;
import java.security.Principal;
import java.util.Properties;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.security.Authenticator;

/** A security class used by LocatorTest */
public class MyAuthenticator implements Authenticator {

  public static Authenticator create() {
    return new MyAuthenticator();
  }

  public MyAuthenticator() {
  }

  public void init(Properties systemProps, LogWriter systemLogger,
      LogWriter securityLogger) throws AuthenticationFailedException {
  }

  public Principal authenticate(Properties props, DistributedMember member, InetAddress addr)
      throws AuthenticationFailedException {
    return MyPrincipal.create();
  }

  public Principal authenticate(Properties props, DistributedMember member)
      throws AuthenticationFailedException {
    return MyPrincipal.create();
  }

  public void close() {
  }

}