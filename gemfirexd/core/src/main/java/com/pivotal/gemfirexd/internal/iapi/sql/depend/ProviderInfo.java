/*

   Derby - Class com.pivotal.gemfirexd.internal.iapi.sql.depend.ProviderInfo

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

package com.pivotal.gemfirexd.internal.iapi.sql.depend;


import com.pivotal.gemfirexd.internal.catalog.DependableFinder;
import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.iapi.services.io.Formatable;

/**
 * A ProviderInfo associates a DependableFinder with a UUID that stands
 * for a database object.  For example, the tables used by a view have
 * DependableFinders associated with them, and a ProviderInfo associates
 * the tables' UUIDs with their DependableFinders.
 */
public interface ProviderInfo extends Formatable
{
	/**
	 * Get the DependableFinder.
	 */
	DependableFinder getDependableFinder();

	/**
	 * Get the object id
	 */
	UUID getObjectId();

	/**
	 * Get the provider's name.
	 */
	String getProviderName();
}
