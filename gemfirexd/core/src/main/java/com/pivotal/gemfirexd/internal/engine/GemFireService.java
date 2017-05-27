
/*

 Derived from source files from the Derby project.

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
 * Changes for GemFireXD distributed data platform.
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

package com.pivotal.gemfirexd.internal.engine;

import java.io.IOException;

import java.util.Enumeration;
import java.util.Properties;

import com.pivotal.gemfirexd.Attribute;
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.EngineType;
import com.pivotal.gemfirexd.internal.iapi.reference.Property;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.monitor.PersistentService;
import com.pivotal.gemfirexd.internal.io.StorageFactory;


/**
 * PersistentService for the GemFire storage.
 *
 * @author Eric Zoerner
 */
public final class GemFireService implements PersistentService {

  private final static String serviceType = "gemfirexd";

  private String serviceName;

  /**
   * Creates a new GemFireService object.
   */
  public GemFireService() {
  }

  public String getType() {
    return serviceType;
  }

  public Enumeration getBootTimeServices() {
    return null;
  }

  /**
   * Open the service properties in the directory identified by the service
   * name. The property SERVICE_ROOT (db2j.rt.serviceRoot) is added by this
   * method and set to the service directory.
   *
   * @return  A Properties object or null if serviceName does not represent a
   *          valid service.
   *
   * @exception  StandardException  Service appears valid but the properties
   *                                cannot be created.
   */
  public Properties getServiceProperties(String serviceName,
                                         Properties defaultProperties)
  throws StandardException {

    // !!!:ezoerner:20080225 Here we need to see if we can find service
    // properties in an existing instance of GemFire, otherwise we need to
    // return null; For now, just return null; later we will save the service
    // properties in the GemFire cache?
    /*
    return null;
     */
    Properties service = new Properties(defaultProperties);
    service.setProperty(Property.SERVICE_PROTOCOL, Property.DATABASE_MODULE);
    service.setProperty(EngineType.PROPERTY,
                        Integer.toString(getEngineType()));
    service.setProperty("gemfirexd.access", GemFireStore.IMPLEMENTATIONID);
    return service;
  }

  public void saveServiceProperties(String serviceName,
                                    StorageFactory storageFactory,
                                    Properties properties,
                                    boolean replace) throws StandardException {

  }

  public void saveServiceProperties(String serviceName,
                                    Properties properties,
                                    boolean replace) throws StandardException {

  }

  /**
   * Returns the canonical name of the service.
   *
   * @exception  StandardException  Service root cannot be created.
   */
  public String createServiceRoot(String name, boolean deleteExisting)
  throws StandardException {

    serviceName = name;
    // no need to create the service root, just return the service name
    return name;
  }

  public boolean removeServiceRoot(String serviceName) {

    // no service root
    return true;
  }

  public String getCanonicalServiceName(String name) throws StandardException {
    /*
     * The purpose is to cope with no database name specified in the data
     * source, such as jdbc:gemfirexd.
     * 
     * <P>
     * The source problem is the creation of several databases, which
     * subsequently creates the system regions with the same name.
     * </p>
     * 
     * <p>
     * Currently only let the system throws database not found exception instead
     * of the region exist exception
     * </p>
     * 
     * Note:
     * <p>
     * The final solution depends on the requirement of the GemFireXD. For
     * example, allowing several databases or no specified database name meaning
     * the default database.
     * </p>
     * 
     * @see connect.sql
     * @20080701
     * @author yjing
     */
      if (Attribute.GFXD_DBNAME.equals(name)
              || Attribute.SQLF_DBNAME.equals(name)) {
          return Attribute.GFXD_DBNAME;
      } else if (Attribute.SNAPPY_DBNAME.equals(name)) {
          return Attribute.SNAPPY_DBNAME;
      }
      else {
        throw StandardException.newException(SQLState.DATABASE_NOT_FOUND, name);
      }
    /*
    String protocolLeadIn = getType() + ":";
    int colon = name.indexOf(':');

    if (colon > 1) { // Subsubprotocols must be at least 2 characters long

      if (!name.startsWith(protocolLeadIn)) {
        return null; // It is not our database
      }

      String serviceName = name.substring(colon + 1).trim();
      if (serviceName.length() > 0) {
        throw StandardException.newException(SQLState.DATABASE_NOT_FOUND,
            serviceName);
      }
      else {
        return protocolLeadIn;
      }
    }
    else {
      // throw new AssertionError("unexpected name: " + name);
      // return name;
      throw StandardException.newException(SQLState.DATABASE_NOT_FOUND, name);
    }
    */
  }

  public String getUserServiceName(String serviceName) {

    try {
      return getCanonicalServiceName(serviceName);
    }
    catch (StandardException e) {

      // not possible since getCanonicalServiceName doesn't actually throw
      // a StandardException
      AssertionError ae = new AssertionError("unexpected exception");
      ae.initCause(e);
      throw ae;
    }
  }

  public boolean isSameService(String serviceName1, String serviceName2) {
    return serviceName1.equals(serviceName2);
  }

  public boolean hasStorageFactory() {
    return false;
  }

  public StorageFactory getStorageFactoryInstance(boolean useHome,
                                                  String databaseName,
                                                  String tempDirName,
                                                  String uniqueName)
  throws StandardException, IOException {
    return null;
  }

  /**
   * DOCUMENT ME!
   *
   * @return  DOCUMENT ME!
   */
  private int getEngineType() {
    return EngineType.STANDALONE_DB;
  }

  @Override
  public String getServiceName() {
    return serviceName;
  }
}
