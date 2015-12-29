/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.jdbc.authentication.JNDIAuthenticationService

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



import com.pivotal.gemfirexd.auth.callback.UserAuthenticator;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.jdbc.AuthenticationService;
import com.pivotal.gemfirexd.internal.iapi.services.property.PropertyUtil;
import com.pivotal.gemfirexd.internal.iapi.util.StringUtil;

import java.util.Properties;

/**
 * This is the JNDI Authentication Service base class.
 * <p>
 * It instantiates the JNDI authentication scheme defined by the user/
 * administrator. Derby supports LDAP JNDI providers.
 * <p>
 * The user can configure its own JNDI provider by setting the
 * system or database property gemfirexd.authentication.provider .
 *
 */

public class JNDIAuthenticationService
	extends AuthenticationServiceBase {

        // GemStone changes BEGIN
        protected static final String AUTHFACTORYMETHOD = JNDIAuthenticationService.class
            .getName()
            + factoryMethodForGFEAuth;
      
        // GemStone changes END
	private String authenticationProvider;

	//
	// constructor
	//

	// call the super
	public JNDIAuthenticationService() {
		super();
	}

	//
	// ModuleControl implementation (overriden)
	//

	/**
	 *  Check if we should activate the JNDI authentication service.
	 */
	public boolean canSupport(String identifier, Properties properties) {

//		if (!requireAuthentication(properties))
//			return false;

		//
		// we check 2 things:
		//
		// - if gemfirexd.authentication.required system
		//   property is set to true.
		// - if gemfirexd.authentication.provider is set to one
		// of the JNDI scheme we support (i.e. LDAP).
		//

		//GemStone changes BEGIN
                return checkAndSetSchemeSupported(
                    identifier,
                    properties,
                    com.pivotal.gemfirexd.Constants.AUTHENTICATION_PROVIDER_LDAP);
		/* original code
		authenticationProvider = PropertyUtil.getPropertyFromSet(
					properties,
						com.pivotal.gemfirexd.internal.iapi.reference.Property.AUTHENTICATION_PROVIDER_PARAMETER);

		 if ( (authenticationProvider != null) &&
			   (StringUtil.SQLEqualsIgnoreCase(authenticationProvider,
				  	com.pivotal.gemfirexd.internal.iapi.reference.Property.AUTHENTICATION_PROVIDER_LDAP)))
			return true;

		return false;
		*/
                //GemStone changes END
	}

	/**
	 * @see com.pivotal.gemfirexd.internal.iapi.services.monitor.ModuleControl#boot
	 * @exception StandardException upon failure to load/boot the expected
	 * authentication service.
	 */
	public void boot(boolean create, Properties properties)
	  throws StandardException {

		// We need authentication
		// setAuthentication(true);

		// we call the super in case there is anything to get initialized.
		super.boot(create, properties);

		// We must retrieve and load the authentication scheme that we were
		// told to.

		// Set ourselves as being ready and loading the proper
		// authentication scheme for this service
		UserAuthenticator aJNDIAuthscheme;

		// we're dealing with LDAP
		aJNDIAuthscheme = new LDAPAuthenticationSchemeImpl(this, properties);	
		this.setAuthenticationService(aJNDIAuthscheme);
	}
}
