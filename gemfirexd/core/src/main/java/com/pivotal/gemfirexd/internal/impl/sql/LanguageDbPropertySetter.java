/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.LanguageDbPropertySetter

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

package com.pivotal.gemfirexd.internal.impl.sql;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.Property;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextService;
import com.pivotal.gemfirexd.internal.iapi.services.daemon.Serviceable;
import com.pivotal.gemfirexd.internal.iapi.services.property.PropertySetCallback;
import com.pivotal.gemfirexd.internal.iapi.services.property.PropertyUtil;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;

import java.io.Serializable;
import java.util.Dictionary;

/**
 * A class to handle setting language database properties
 */
public class LanguageDbPropertySetter implements PropertySetCallback
{
	public void init(boolean dbOnly, Dictionary p) {
		// not called yet ...
	}
	/** @exception StandardException Thrown on error. */
	public boolean validate
	(
		String			key,
		Serializable	value,
		Dictionary		p
	) throws StandardException 
	{
		// Disallow changing sqlAuthorization from true to false or null after
		// switching to Standard authorization
		if (key.trim().equals(com.pivotal.gemfirexd.Property.SQL_AUTHORIZATION))
		{
			LanguageConnectionContext lcc = (LanguageConnectionContext)
					ContextService.getContext(LanguageConnectionContext.CONTEXT_ID);

			if (lcc != null && lcc.usesSqlAuthorization() && !Boolean.valueOf((String)value).booleanValue())
				throw StandardException.newException(SQLState.PROPERTY_UNSUPPORTED_CHANGE,
					key, value);
		}

		if (key.equals(Property.LANGUAGE_STALE_PLAN_CHECK_INTERVAL)) {
			PropertyUtil.intPropertyValue(
						Property.LANGUAGE_STALE_PLAN_CHECK_INTERVAL,
						value,
						Property.MIN_LANGUAGE_STALE_PLAN_CHECK_INTERVAL,
						Integer.MAX_VALUE,
						Property.DEFAULT_LANGUAGE_STALE_PLAN_CHECK_INTERVAL
						);
			return true;
		}

		return false;
	}

	public Serviceable apply
	(
		String			key,
		Serializable	value,
		Dictionary		p
	) 
	{
		return null;
	}

 	public Serializable map
	(
		String			key,
		Serializable	value,
		Dictionary		p
	) 
	{
		return null;
	}
}
