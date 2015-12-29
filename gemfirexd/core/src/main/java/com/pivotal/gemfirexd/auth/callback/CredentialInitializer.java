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

package com.pivotal.gemfirexd.auth.callback;

import java.sql.SQLException;
import java.util.Properties;

/**
 * Specifies the mechanism to obtain credentials for server to join. 
 * It is mandatory to implement this when running in secure mode and 
 * custom implementation of {@link UserAuthenticator} is
 * configured on the server/locator.
 * <P>
 * To configure a custom user authenticator on server/locator, fully qualified
 * class name of the implementation of {@link UserAuthenticator} should be
 * mentioned in <code>gemfirexd.auth-provider</code> property.
 * </P>
 * 
 * @author soubhikc
 */
public interface CredentialInitializer {

  /**
   * Initialize with the given set of security properties and return the
   * credentials for the peer/client as properties.
   * 
   * This method can modify the given set of properties. For example it may
   * invoke external agents or even interact with the user.
   * 
   * Normally it is expected that implementations will filter out
   * <i>security-*</i> properties that are needed for credentials and return
   * only those.
   * 
   * @param securityProps
   *          the security properties obtained using a call to that will be used
   *          for obtaining the credentials
   * 
   * @throws SQLException
   *           in case of failure to obtain the credentials
   * 
   * @return the credentials to be used for the given <code>server</code>
   */
  public Properties getCredentials(Properties securityProps)
      throws SQLException;
}
