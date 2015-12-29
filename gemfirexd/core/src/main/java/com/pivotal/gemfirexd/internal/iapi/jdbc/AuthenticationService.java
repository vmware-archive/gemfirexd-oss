/*

   Derby - Class com.pivotal.gemfirexd.internal.iapi.jdbc.AuthenticationService

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

package com.pivotal.gemfirexd.internal.iapi.jdbc;

import java.util.Properties;
import java.sql.SQLException;

/**
 *
 * The AuthenticationService provides a mechanism for authenticating
 * users willing to access JBMS.
 * <p>
 * There can be different and user defined authentication schemes, as long
 * the expected interface here below is implementing and registered
 * as a module when JBMS starts-up.
 * <p>
 */
public interface AuthenticationService 
{

	public static final String MODULE =
								"com.pivotal.gemfirexd.internal.iapi.jdbc.AuthenticationService";
	/**
	 * Authenticate a User inside JBMS.
	 * Returns null on success else the reason for failure.
	 *
	 * @param info			Connection properties info.
	 */
	public String /* GemStone change boolean */ authenticate(String databaseName, Properties info)
	  throws SQLException;
}
