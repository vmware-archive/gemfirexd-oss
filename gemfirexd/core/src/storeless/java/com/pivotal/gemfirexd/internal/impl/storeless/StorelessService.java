/*

   Derby - Class org.apache.impl.storeless.StorelessService

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
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
package com.pivotal.gemfirexd.internal.impl.storeless;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.EngineType;
import com.pivotal.gemfirexd.internal.iapi.reference.Property;
import com.pivotal.gemfirexd.internal.iapi.services.monitor.PersistentService;
import com.pivotal.gemfirexd.internal.io.StorageFactory;

/**
 * PersistentService for the storeless engine.
 * Type is 'storeless' which will correspond to
 * the JDBC URL 'jdbc:derby:storeless'.
 *
 */
public class StorelessService implements PersistentService {
	
	public StorelessService()
	{
	}
	
	public String getType() {
		return "storeless";
	}

	public Enumeration getBootTimeServices() {
		return null;
	}

	public Properties getServiceProperties(String serviceName, Properties defaultProperties) throws StandardException {
		
		Properties service = new Properties(defaultProperties);
		service.setProperty(Property.SERVICE_PROTOCOL,
                "com.pivotal.gemfirexd.internal.database.Database");
		service.setProperty(EngineType.PROPERTY,
                Integer.toString(getEngineType()));
		return service;
	}

	public void saveServiceProperties(String serviceName, StorageFactory storageFactory, Properties properties, boolean replace) throws StandardException {
		// TODO Auto-generated method stub
		
	}

	public void saveServiceProperties(String serviceName, Properties properties, boolean replace) throws StandardException {
		// TODO Auto-generated method stub
		
	}

	public String createServiceRoot(String name, boolean deleteExisting) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean removeServiceRoot(String serviceName) {
		// TODO Auto-generated method stub
		return false;
	}

	public String getCanonicalServiceName(String name) {
		return name;
	}

	public String getUserServiceName(String serviceName) {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean isSameService(String serviceName1, String serviceName2) {
		// TODO Auto-generated method stub
		return serviceName1.equals(serviceName2);
	}

	public boolean hasStorageFactory() {
		// TODO Auto-generated method stub
		return false;
	}

	public StorageFactory getStorageFactoryInstance(boolean useHome, String databaseName, String tempDirName, String uniqueName) throws StandardException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

    protected int getEngineType() {
        return EngineType.STORELESS_ENGINE;
    }

    //GemStone changes BEGIN
    @Override
    public String getServiceName() {
      throw new AssertionError("Method shouldn't have been called from bootModule");
    }
    //GemStone changes END
}
