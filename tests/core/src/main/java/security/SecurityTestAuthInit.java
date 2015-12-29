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
package security;

import hydratest.security.SecurityTestPrms;

import java.sql.Timestamp;
import java.util.Properties;

import newWan.security.WanSecurity;

import templates.security.UserPasswordAuthInit;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.security.AuthInitialize;
import com.gemstone.gemfire.security.AuthenticationFailedException;

/**
 * An {@link AuthInitialize} implementation that obtains the user name and
 * password as the credentials from the given set of properties.
 * 
 * @author Aneesh Karayil
 * @since 5.5
 */
public class SecurityTestAuthInit extends UserPasswordAuthInit {

  public static AuthInitialize create() {
    return new SecurityTestAuthInit();
  }

  public SecurityTestAuthInit() {
    super();
  }

  public Properties getCredentials(Properties props, DistributedMember server,
      boolean isPeer) throws AuthenticationFailedException {
    
    Timestamp ts = new Timestamp(System.currentTimeMillis());
    Properties newProps = super.getCredentials(props, server, isPeer);
    if (SecurityTestPrms.useBogusPassword() && !isPeer) {
      newProps.setProperty(UserPasswordAuthInit.USER_NAME, "bogus");
    }
    else if (WanSecurity.isInvalid){ // 44650 - for newWan, the task thread and the connection thread are different, hence added a new check.
      newProps.setProperty(UserPasswordAuthInit.USER_NAME, "bogus");
    }
    return newProps;
  }

}
