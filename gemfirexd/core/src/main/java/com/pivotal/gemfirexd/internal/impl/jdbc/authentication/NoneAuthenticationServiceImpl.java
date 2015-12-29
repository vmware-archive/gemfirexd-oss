/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.jdbc.authentication.NoneAuthenticationServiceImpl

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

/*
 * Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

package com.pivotal.gemfirexd.internal.impl.jdbc.authentication;




import com.pivotal.gemfirexd.Constants;
import com.pivotal.gemfirexd.auth.callback.UserAuthenticator;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.jdbc.AuthenticationService;
import com.pivotal.gemfirexd.internal.iapi.reference.MessageId;
import com.pivotal.gemfirexd.internal.iapi.services.i18n.MessageService;
import com.pivotal.gemfirexd.internal.iapi.services.property.PropertyUtil;

import java.util.Properties;

/**
 * This authentication service does not care much about authentication.
 * <p>
 * It is a quiescient authentication service that will basically satisfy
 * any authentication request, as JBMS system was not instructed to have any
 * particular authentication scheme to be loaded at boot-up time.
 *
 */
public final class NoneAuthenticationServiceImpl
	extends AuthenticationServiceBase implements UserAuthenticator {

        // GemStone changes BEGIN
        protected static final String AUTHFACTORYMETHOD = NoneAuthenticationServiceImpl.class
            .getName()
            + factoryMethodForGFEAuth;
      
        // GemStone changes END
	//
	// ModuleControl implementation (overriden)
	//

	/**
	 *  Check if we should activate this authentication service.
	 */
	public boolean canSupport(String identifier, Properties properties) {

// GemStone changes BEGIN
	  if (checkAndSetSchemeSupported(identifier, properties, "NONE")) {
	    return true;
	  }
// GemStone changes END
		return !requireAuthentication(properties);
	}

	/**
	 * @see com.pivotal.gemfirexd.internal.iapi.services.monitor.ModuleControl#boot
	 * @exception StandardException upon failure to load/boot the expected
	 * authentication service.
	 */
	public void boot(boolean create, Properties properties) 
	  throws StandardException {

		// we call the super in case there is anything to get initialized.
 		super.boot(create, properties);

		// nothing special to be done, other than setting other than
		// setting ourselves as being ready and loading the proper
		// authentication scheme for this service
		//.
		this.setAuthenticationService(this);
	}

	/*
	** UserAuthenticator
	*/

	/**
	 * Authenticate the passed-in user's credentials.
	 *
	 * @param userName		The user's name used to connect to JBMS system
	 * @param userPassword	The user's password used to connect to JBMS system
	 * @param databaseName	The database which the user wants to connect to.
	 * @param info			Additional jdbc connection info.
	 */
	public String /* GemStone changes boolean */	authenticateUser(String userName,
								 String userPassword,
								 String databaseName,
								 Properties info
									)
	{
		// Since this authentication service does not really provide
		// any particular authentication, therefore we satisfy the request.
		// and always authenticate successfully the user.
		//
		return null;
	}

// GemStone changes BEGIN
	@Override
	public String toString() {
	  return "NONE";
	}
// GemStone changes END
}
