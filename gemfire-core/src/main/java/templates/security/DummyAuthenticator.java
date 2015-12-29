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
package templates.security;

import java.security.Principal;
import java.util.Properties;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.security.Authenticator;
import templates.security.UserPasswordAuthInit;
import templates.security.UsernamePrincipal;

/**
 * A dummy implementation of the {@link Authenticator} interface that expects a
 * user name and password allowing authentication depending on the format of the
 * user name.
 * 
 * @author Sumedh Wale
 * @since 5.5
 */
public class DummyAuthenticator implements Authenticator {

  public static Authenticator create() {
    return new DummyAuthenticator();
  }

  public DummyAuthenticator() {
  }

  public void init(Properties systemProps, LogWriter systemLogger,
      LogWriter securityLogger) throws AuthenticationFailedException {
  }

  public static boolean testValidName(String userName) {

    return (userName.startsWith("user") || userName.startsWith("reader")
        || userName.startsWith("writer") || userName.equals("admin")
        || userName.equals("root") || userName.equals("administrator"));
  }

  public Principal authenticate(Properties props, DistributedMember member)
      throws AuthenticationFailedException {

    String userName = props.getProperty(UserPasswordAuthInit.USER_NAME);
    if (userName == null) {
      throw new AuthenticationFailedException(
          "DummyAuthenticator: user name property ["
              + UserPasswordAuthInit.USER_NAME + "] not provided");
    }
    String password = props.getProperty(UserPasswordAuthInit.PASSWORD);
    if (password == null) {
      throw new AuthenticationFailedException(
          "DummyAuthenticator: password property ["
              + UserPasswordAuthInit.PASSWORD + "] not provided");
    }

    if (userName.equals(password) && testValidName(userName)) {
      return new UsernamePrincipal(userName);
    }
    else {
      throw new AuthenticationFailedException(
          "DummyAuthenticator: Invalid user name [" + userName
              + "], password supplied.");
    }
  }

  public void close() {
  }

}
