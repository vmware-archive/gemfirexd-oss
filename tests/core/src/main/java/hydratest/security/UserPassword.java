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

package hydratest.security;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.security.AuthInitialize;
import com.gemstone.gemfire.security.AuthenticationFailedException;

import hydra.DistributedSystemHelper;
import hydra.Log;
import java.util.Properties;

/**
 * Authentication using user/password scheme.
 */
public class UserPassword implements AuthInitialize {

  private static long FirstUseTime = 0;

  public static AuthInitialize create() {
    return new UserPassword();
  }

  public void init(LogWriter systemLogger, LogWriter securityLogger)
      throws AuthenticationFailedException {
  }

  public Properties getCredentials(Properties p, DistributedMember server,
      boolean isPeer) throws AuthenticationFailedException {

    if (Log.getLogWriter().fineEnabled()) {
      String s = "Test configured security properties: ";
      if (isPeer) {
        s += DistributedSystemHelper.getGemFireDescription()
            .getSecurityDescription().getPeerExtraProperties();
      }
      else {
        s += DistributedSystemHelper.getGemFireDescription()
            .getSecurityDescription().getClientExtraProperties();
      }
      Log.getLogWriter().fine(s);
    }
    Log.getLogWriter().fine("Test got security properties: " + p);

    String username = p.getProperty(UserPasswordPrms.USERNAME_NAME);
    String password = null;
    if (SecurityTestPrms.useBogusPassword()) {
      // want authentication to fail
      password = "BogusPassword";
    }
    else {
      password = p.getProperty(UserPasswordPrms.PASSWORD_NAME);
      if (password.equals(UserPasswordPrms.DEFAULT_PASSWORD)) {
        // see if the default credentials have expired
        if (FirstUseTime == 0) {
          FirstUseTime = System.currentTimeMillis();
        }
        else {
          if (System.currentTimeMillis() - FirstUseTime > SecurityTestPrms
              .getExpireSeconds() * 1000) {
            String s = "Use of default password has expired, please reset";
            Log.getLogWriter().info("AuthInitialize: " + s);
            return null; // or exception? or do this in Authenticator instead?
          }
        }
        p.setProperty(UserPasswordPrms.PASSWORD_NAME, password);
      }
    }
    Log.getLogWriter().info("AuthInitialize: set " + username + " " + password);
    return p;
  }

  public void close() {
    Log.getLogWriter().info("AuthInitialize: closed");
  }
}
