
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

package com.pivotal.gemfirexd.internal.engine.access;

import java.io.Serializable;

import java.util.Properties;


import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.Attribute;
import com.pivotal.gemfirexd.internal.iapi.services.io.Formatable;
import com.pivotal.gemfirexd.internal.iapi.services.monitor.Monitor;
import com.pivotal.gemfirexd.internal.iapi.services.property.PropertyFactory;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.store.access.AccessFactory;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.store.access.conglomerate.TransactionManager;
import com.pivotal.gemfirexd.internal.iapi.store.raw.RawStoreFactory;

/**
 * Stores properties in a congolmerate with complete transactional support.
 *
 * <p>The PropertyConglomerate contains one row with 2 columns per property.
 * Column 0 is the UTF key, and column 1 is the data.
 *
 * <p>
 * <p>The property conglomerate manages the storage of database properties and
 * thier defaults. Each property is stored as a row in the PropertyConglomerate
 *
 * <OL>
 *   <LI>Column 0 is the UTF key,</li>
 *   <LI>Column 1 is the data.</li>
 * </OL>
 *
 * All the property defaults are stored in a single row of the Property
 * Conglomerate:
 *
 * <OL>
 *   <LI>Column 0 is the UTF key "gemfirexd.defaultPropertyName".</li>
 *   <LI>Column 1 is a FormatableProperties object with one</li> row per default
 *   property.
 * </OL>
 *
 * <p>In general a propery default defines it value if the property itself is
 * not defined.
 *
 * <p>Because the properties conglomerate is stored in a conglomerate the
 * information it contains is not available before the raw store runs recovery.
 * To make a small number of properties (listed in servicePropertyList)
 * available during early boot, this copies them to services.properties.
 *
 * @author Eric Zoerner
 */
public final class PropertyConglomerate {

  /** DOCUMENT ME! */
  protected final Properties serviceProperties;

  /** defaults for the properties stored in this */
  protected final Properties servicePropertiesDefaults;

  /*
  /** DOCUMENT ME! *
  protected long propertiesConglomId;

  /** DOCUMENT ME! *
  private Dictionary cachedSet;
  */

  /** DOCUMENT ME! */
  private final PropertyFactory pf;

  /**
   * Constructors for This class:
   *
   * @param  tc  DOCUMENT ME!
   * @param  create  DOCUMENT ME!
   * @param  serviceProperties  DOCUMENT ME!
   * @param  pf  DOCUMENT ME!
   *
   * @throws  StandardException  DOCUMENT ME!
   */
   public PropertyConglomerate(TransactionController tc,
                               boolean create,
                               Properties serviceProperties,
                               Properties servicePropertiesDefaults,
                               PropertyFactory pf) throws StandardException {
    this.pf = pf;

    /*
    if (!create) {
      String id = serviceProperties.getProperty(Property.PROPERTIES_CONGLOM_ID);

      if (id == null) {
        create = true;
      }
      else {

        try {
          propertiesConglomId = Long.valueOf(id).longValue();
        }
        catch (NumberFormatException nfe) {
          throw Monitor.exceptionStartingModule(nfe);
        }
      }
    }

    if (create) {
      DataValueDescriptor[] template = makeNewTemplate();

      Properties conglomProperties = new Properties();

      conglomProperties.put(Property.PAGE_SIZE_PARAMETER,
                            RawStoreFactory.PAGE_SIZE_STRING);

      conglomProperties.put(RawStoreFactory.PAGE_RESERVED_SPACE_PARAMETER,
                            RawStoreFactory.PAGE_RESERVED_ZERO_SPACE_STRING);

      AttributesFactory af = new AttributesFactory();
      af.setScope(Scope.LOCAL);
      af.setInitialCapacity(GemFireXDUtils.getDefaultInitialCapacity());
      conglomProperties.put(GfxdConstants.REGION_ATTRIBUTES_KEY, af.create());
      // put property conglomerates in sys schema
      conglomProperties.setProperty(GfxdConstants.PROPERTY_SCHEMA_NAME,
                                    GfxdConstants.SYSTEM_SCHEMA_NAME);
      conglomProperties.setProperty(GfxdConstants.PROPERTY_TABLE_NAME,
                                    GfxdConstants.PROP_CONGLOM_TABLE_NAME);

      propertiesConglomId = tc.createConglomerate(
                              AccessFactoryGlobals.HEAP,
                              template,
                              null,
                              (int[]) null, // use default collation for
                                            // property conglom.
                              conglomProperties,
                              TransactionController.IS_DEFAULT);
      
      serviceProperties.put(Property.PROPERTIES_CONGLOM_ID,
                            Long.toString(propertiesConglomId));
    }
    */

    this.serviceProperties = serviceProperties;
    this.servicePropertiesDefaults = servicePropertiesDefaults;

    /*
    PC_XenaVersion softwareVersion = new PC_XenaVersion();
    if (create)
      setProperty(tc,DataDictionary.PROPERTY_CONGLOMERATE_VERSION,
                  softwareVersion, true);
    else
      softwareVersion.upgradeIfNeeded(tc,this,serviceProperties);

    getCachedDbProperties(tc);
    */
  }

  /* Private/Protected methods of This class: */

  /*
  /**
   * Create a new PropertyConglomerate row, with values in it.
   *
  private DataValueDescriptor[] makeNewTemplate(String key,
                                                Serializable value) {
    DataValueDescriptor[] template = new DataValueDescriptor[2];

    template[0] = new UTF(key);
    template[1] = new UserType(value);

    return (template);
  }

  /**
   * Create a new empty PropertyConglomerate row, to fetch values into.
   *
  private DataValueDescriptor[] makeNewTemplate() {
    DataValueDescriptor[] template = new DataValueDescriptor[2];

    template[0] = new UTF();
    template[1] = new UserType();

    return (template);
  }

  /**
   * Open a scan on the properties conglomerate looking for "key".
   *
   * <p>Open a scan on the properties conglomerate qualified to find the row
   * with value key in column 0. Both column 0 and column 1 are included in the
   * scan list.
   *
   * @return  an open ScanController on the PropertyConglomerate.
   *
   * @param  tc  The transaction to do the Conglomerate work under.
   * @param  key  The "key" of the property that is being requested.
   * @param  open_mode  Whether we are setting or getting the property.
   *
   * @exception  StandardException  Standard exception policy.
   *
  private ScanController openScan(TransactionController tc,
                                  String key,
                                  int open_mode) throws StandardException {
    Qualifier[][] qualifiers = null;

    if (key != null) {

      // Set up qualifier to look for the row with key value in column[0]
      qualifiers = new Qualifier[1][];
      qualifiers[0] = new Qualifier[1];
      qualifiers[0][0] = new UTFQualifier(0, key);
    }

    // open the scan, clients will do the fetches and close.
    ScanController scan = tc.openScan(
                            propertiesConglomId,
                            false, // don't hold over the commit
                            open_mode,
                            TransactionController.MODE_TABLE,
                            TransactionController.ISOLATION_SERIALIZABLE,
                            (FormatableBitSet) null,
                            (DataValueDescriptor[]) null, // start key
                            ScanController.NA,
                            qualifiers,
                            (DataValueDescriptor[]) null, // stop key
                            ScanController.NA);

    return (scan);
  }
  */

  /* Package Methods of This class: */

  /**
   * Set a property in the conglomerate.
   *
   * @param  key  The key used to lookup this property.
   * @param  value  The value to be associated with this key. If null, delete
   *                the property from the properties list.
   */

  /**
   * Set the default for a property.
   *
   * @exception  StandardException  Standard exception policy.
   */
  public void setPropertyDefault(TransactionController tc,
                          String key,
                          Serializable value) throws StandardException {

    //lockProperties(tc);

    Serializable valueToSave = null;

    //
    // If the default is visible we validate apply and map.
    // otherwise we just map.
    if (propertyDefaultIsVisible(key)) {
      valueToSave = validateApplyAndMap(tc, key, value, false);
    }
    else {

      synchronized (this) {
        /*
        Hashtable defaults = new Hashtable();
        getProperties(tc,
                      defaults,
                      false /*!stringsOnly*,
                      true /*defaultsOnly*);
        */
        validate(key, value, this.servicePropertiesDefaults);
        valueToSave = map(key, value, this.servicePropertiesDefaults);
      }
    }

    savePropertyDefault(tc, key, valueToSave);
  }

  /**
   * DOCUMENT ME!
   *
   * @param  key  DOCUMENT ME!
   *
   * @return  DOCUMENT ME!
   *
   * @throws  StandardException  DOCUMENT ME!
   */
  public final boolean propertyDefaultIsVisible(String key)
      throws StandardException {

    return !this.serviceProperties.containsKey(key);
    /*
    lockProperties(tc);

    return (readProperty(tc, key) == null);
    */
  }

  /**
   * DOCUMENT ME!
   *
   * @param  tc  DOCUMENT ME!
   * @param  key  DOCUMENT ME!
   * @param  value  DOCUMENT ME!
   *
   * @throws  StandardException  DOCUMENT ME!
   */
  void saveProperty(TransactionController tc, String key, Serializable value)
  throws StandardException {

    saveServiceProperty(this.serviceProperties, key, value);
    /*
    if (saveServiceProperty(key, value)) {
      return;
    }

    // Do a scan to see if the property already exists in the Conglomerate.
    ScanController scan = this.openScan(
                            tc,
                            key,
                            TransactionController.OPENMODE_FORUPDATE);

    DataValueDescriptor[] dvds = makeNewTemplate();
    
    ExecRow xrow = new ValueRow(dvds.length);
    xrow.setRowArray(dvds);

    if (scan.fetchNext(xrow)) {

      if (value == null) {
        // A null input value means that we should delete the row

        scan.delete();
        /*soubhik: also update the cached instance. underlying
         * conglomerate cannot get updated without this path
         * as its a local region.
         *
        cachedSet.remove(key);
      }
      else {
        // a value already exists, just replace the second columm
        DataValueDescriptor[] row = xrow.getRowArray();
        row[1] = new UserType(value);

        scan.replace(row, (FormatableBitSet) null);
        /*soubhik: also update the cached instance. underlying
         * conglomerate cannot get updated without this path
         * as its a local region.
         *
        cachedSet.put(key, value);
      }

      scan.close();
    }
    else {
      // The value does not exist in the Conglomerate.

      scan.close();
      scan = null;

      if (value != null) {
        // not a delete request, so insert the new property.

        dvds = makeNewTemplate(key, value);

        ConglomerateController cc = tc.openConglomerate(
            propertiesConglomId,
            false,
            TransactionController.OPENMODE_FORUPDATE,
            TransactionController.MODE_TABLE,
            TransactionController.ISOLATION_SERIALIZABLE);

        cc.insert(dvds);
        /*soubhik: also update the cached instance. underlying
         * conglomerate cannot get updated without this path
         * as its a local region.
         *
        cachedSet.put(key, value);

        cc.close();
      }
    }
    */
  }

  /**
   * DOCUMENT ME!
   *
   * @param  key  DOCUMENT ME!
   * @param  value  DOCUMENT ME!
   *
   * @return  DOCUMENT ME!
   */
  private void saveServiceProperty(final Properties props, final String key,
      final Serializable value) {

    if (value != null) {
      props.put(key, value);
    }
    else {
      props.remove(key);
    }

    /*
    if (PropertyUtil.isServiceProperty(key)) {

      if (value != null) {
        serviceProperties.put(key, value);
      }
      else {
        serviceProperties.remove(key);
      }

      return true;
    }
    else {
      return false;
    }
    */
  }

  /**
   * DOCUMENT ME!
   *
   * @param  tc  DOCUMENT ME!
   * @param  key  DOCUMENT ME!
   * @param  value  DOCUMENT ME!
   *
   * @throws  StandardException  DOCUMENT ME!
   */
  void savePropertyDefault(TransactionController tc,
                           String key,
                           Serializable value) throws StandardException {

    saveServiceProperty(this.servicePropertiesDefaults, key, value);
    /*
    if (saveServiceProperty(key, value)) {
      return;
    }

    Dictionary defaults = (Dictionary) readProperty(
                            tc,
                            AccessFactoryGlobals.DEFAULT_PROPERTY_NAME);

    if (defaults == null) {
      defaults = new FormatableHashtable();
    }

    if (value == null) {
      defaults.remove(key);
    }
    else {
      defaults.put(key, value);
    }

    if (defaults.size() == 0) {
      defaults = null;
    }

    saveProperty(tc,
                 AccessFactoryGlobals.DEFAULT_PROPERTY_NAME,
                 (Serializable) defaults);
    */
  }

  /**
   * DOCUMENT ME!
   *
   * @param  tc  DOCUMENT ME!
   * @param  key  DOCUMENT ME!
   * @param  value  DOCUMENT ME!
   * @param  dbOnlyProperty  DOCUMENT ME!
   *
   * @return  DOCUMENT ME!
   *
   * @throws  StandardException  DOCUMENT ME!
   */
  private Serializable validateApplyAndMap(TransactionController tc,
                                           String key,
                                           Serializable value,
                                           boolean dbOnlyProperty)
  throws StandardException {
    /*
    Dictionary d = new Hashtable();
    getProperties(tc, d, false /*!stringsOnly*, false /*!defaultsOnly*);
    */

    Serializable mappedValue = pf.doValidateApplyAndMap(tc,
                                                        key,
                                                        value,
                                                        this.serviceProperties,
                                                        dbOnlyProperty);

    /*
    // RESOLVE: log device cannot be changed on the fly right now
    if (key.equals(Attribute.LOG_DEVICE)) {
      throw StandardException.newException(
        SQLState.RAWSTORE_CANNOT_CHANGE_LOGDEVICE);
    }
    */

    if (mappedValue == null) {
      return value;
    }
    else {
      return mappedValue;
    }
  }

  /**
   * Call the property set callbacks to map a proposed property value to a value
   * to save.
   *
   * <P>The caller must run this in a block synchronized on this to serialize
   * validations with changes to the set of property callbacks
   */
  private Serializable map(String key, Serializable value, Properties set)
  throws StandardException {
    return pf.doMap(key, value, set);
  }

  /**
   * Call the property set callbacks to validate a property change against the
   * property set provided.
   *
   * <P>The caller must run this in a block synchronized on this to serialize
   * validations with changes to the set of property callbacks
   */
  private void validate(String key, Serializable value, Properties set)
  throws StandardException {
    pf.validateSingleProperty(key, value, set);
  }

  /**
   * DOCUMENT ME!
   *
   * @param  tc  DOCUMENT ME!
   * @param  key  DOCUMENT ME!
   * @param  value  DOCUMENT ME!
   *
   * @return  DOCUMENT ME!
   *
   * @throws  StandardException  DOCUMENT ME!
   */
  private boolean bootPasswordChange(TransactionController tc,
                                     String key,
                                     Serializable value)
  throws StandardException {

    // first check for boot password  change - we don't put boot password
    // in the servicePropertyList because if we do, then we expose the
    // boot password in clear text
    if (key.equals(Attribute.BOOT_PASSWORD)) {

      // The user is trying to change the secret key.
      // The secret key is never stored in clear text, but we
      // store the encrypted form in the services.properties
      // file.  Swap the secret key with the encrypted form and
      // put that in the services.properties file.
      AccessFactory af = ((TransactionManager) tc).getAccessManager();

      RawStoreFactory rsf = (RawStoreFactory) Monitor.findServiceModule(
                              af,
                              RawStoreFactory.MODULE);

      // remove secret key from properties list if possible
      serviceProperties.remove(Attribute.BOOT_PASSWORD);

      value = rsf.changeBootPassword(serviceProperties, value);
      serviceProperties.put(RawStoreFactory.ENCRYPTED_KEY, value);

      return true;
    }
    else {
      return false;
    }
  }

  /**
   * Sets the Serializable object associated with a property key.
   *
   * <p>This implementation turns the setProperty into an insert into the
   * PropertyConglomerate conglomerate.
   *
   * <p>See the discussion of getProperty().
   *
   * <p>The value stored may be a Formatable object or a Serializable object
   * whose class name starts with java.*. This stops arbitary objects being
   * stored in the database by class name, which will cause problems in
   * obfuscated/non-obfuscated systems.
   *
   * @param  tc  The transaction to do the Conglomerate work under.
   * @param  key  The key used to lookup this property.
   * @param  value  The value to be associated with this key. If null, delete
   *                the property from the properties list.
   *
   * @exception  StandardException  Standard exception policy.
   */
  public void setProperty(TransactionController tc,
                   String key,
                   Serializable value,
                   boolean dbOnlyProperty) throws StandardException {

    if (SanityManager.DEBUG) {

      if (!((value == null) || (value instanceof Formatable))) {

        if (!(value.getClass().getName().startsWith("java."))) {
          SanityManager.THROWASSERT("Non-formattable, non-java class - " +
                                    value.getClass().getName());
        }
      }
    }

    //lockProperties(tc);

    Serializable valueToValidateAndApply = value;

    // If we remove a property we validate and apply its default.
    if (value == null) {
      valueToValidateAndApply = getPropertyDefault(key);
    }

    Serializable valueToSave = validateApplyAndMap(tc,
                                                   key,
                                                   valueToValidateAndApply,
                                                   dbOnlyProperty);

    // if this is a bootPasswordChange we save it in
    // a special way.
    if (bootPasswordChange(tc, key, value)) {
      return;
    }
    // value==null means we are removing a property.
    // To remove the property we call saveProperty with
    // a null value. Note we avoid saving the mapped
    // DEFAULT value returned by validateAndApply.
    else if (value == null) {
      saveProperty(tc, key, null);
    }
    // value != null means we simply save the property
    else {
      saveProperty(tc, key, valueToSave);
    }
  }

  /*
  /**
   * DOCUMENT ME!
   *
   * @param  tc  DOCUMENT ME!
   * @param  key  DOCUMENT ME!
   *
   * @return  DOCUMENT ME!
   *
   * @throws  StandardException  DOCUMENT ME!
   *
  private Serializable readProperty(TransactionController tc, String key)
  throws StandardException {

    // scan the table for a row with matching "key"
    ScanController scan = openScan(tc, key, 0);

    DataValueDescriptor[] row = makeNewTemplate();

    // did we find at least one row?
    boolean isThere = scan.fetchNext(row);

    scan.close();

    if (!isThere) {
      return null;
    }

    return (Serializable) (((UserType) row[1]).getObject());
  }

  /**
   * DOCUMENT ME!
   *
   * @param  tc  DOCUMENT ME!
   * @param  key  DOCUMENT ME!
   *
   * @return  DOCUMENT ME!
   *
   * @throws  StandardException  DOCUMENT ME!
   *
  private Serializable getCachedProperty(TransactionController tc, String key)
  throws StandardException {

    //
    // Get the cached set of properties.
    Dictionary dbProps = getCachedDbProperties(tc);

    //
    // Return the value if it is defined.
    if (dbProps.get(key) != null) {
      return (Serializable) dbProps.get(key);
    }
    else {
      return getCachedPropertyDefault(tc, key, dbProps);
    }
  }

  /**
   * DOCUMENT ME!
   *
   * @param  tc  DOCUMENT ME!
   * @param  key  DOCUMENT ME!
   * @param  dbProps  DOCUMENT ME!
   *
   * @return  DOCUMENT ME!
   *
   * @throws  StandardException  DOCUMENT ME!
   *
  private Serializable getCachedPropertyDefault(TransactionController tc,
                                                String key,
                                                Dictionary dbProps)
  throws StandardException {

    //
    // Get the cached set of properties.
    if (dbProps == null) {
      dbProps = getCachedDbProperties(tc);
    }

    //
    // return the default for the value if it is defined.
    Dictionary defaults = (Dictionary) dbProps.get(
                            AccessFactoryGlobals.DEFAULT_PROPERTY_NAME);

    if (defaults == null) {
      return null;
    }
    else {
      return (Serializable) defaults.get(key);
    }
  }
  */

  /**
   * Gets the de-serialized object associated with a property key.
   *
   * <p>The Store provides a transaction protected list of database properties.
   * Higher levels of the system can store and retrieve these properties once
   * Recovery has finished. Each property is a serializable object and is
   * stored/retrieved using a String key.
   *
   * <p>In this implementation a lookup is done on the PropertyConglomerate
   * conglomerate, using a scan with "key" as the qualifier.
   *
   * <p>
   *
   * @param  key  The "key" of the property that is being requested.
   *
   * @return  object The object associated with property key. n ull means no
   *          such key-value pair.
   *
   * @exception  StandardException  Standard exception policy.
   */
  public final Serializable getProperty(String key) throws StandardException {

    return (Serializable)this.serviceProperties.get(key);
    /*
    // Try service properties first.
    if (PropertyUtil.isServiceProperty(key)) {
      return serviceProperties.getProperty(key);
    }

    // See if I'm the exclusive owner. If so I cannot populate
    // the cache as it would contain my uncommitted changes.
    if (iHoldTheUpdateLock(tc)) {

      //
      // Return the property value if it is defined.
      Serializable v = readProperty(tc, key);

      if (v != null) {
        return v;
      }

      return getPropertyDefault(tc, key);
    }
    else {
      return getCachedProperty(tc, key);
    }
    */
  }

  /**
   * Get the default for a property.
   *
   * @exception  StandardException  Standard exception policy.
   */
  public final Serializable getPropertyDefault(String key)
      throws StandardException {

    return (Serializable)this.servicePropertiesDefaults.get(key);
    /*
    // See if I'm the exclusive owner. If so I cannot populate
    // the cache as it would contain my uncommitted changes.
    if (iHoldTheUpdateLock(tc)) {

      //
      // Return the property default value (may be null) if
      // defined.
      Dictionary defaults = (Dictionary) readProperty(
                              tc,
                              AccessFactoryGlobals.DEFAULT_PROPERTY_NAME);

      if (defaults == null) {
        return null;
      }
      else {
        return (Serializable) defaults.get(key);
      }
    }
    else {
      return getCachedPropertyDefault(tc, key, null);
    }
    */
  }

  /*
  /**
   * DOCUMENT ME!
   *
   * @param  to  DOCUMENT ME!
   * @param  from  DOCUMENT ME!
   * @param  stringsOnly  DOCUMENT ME!
   *
   * @return  DOCUMENT ME!
   *
  private Dictionary copyValues(Dictionary to,
                                Dictionary from,
                                boolean stringsOnly) {

    if (from == null) {
      return to;
    }

    for (Enumeration keys = from.keys(); keys.hasMoreElements();) {
      String key = (String) keys.nextElement();
      Object value = from.get(key);

      if ((value instanceof String) || !stringsOnly) {
        to.put(key, value);
      }
    }

    return to;
  }
  */

  /**
   * Fetch the set of properties as a Properties object. This means that only
   * keys that have String values will be included.
   */
  public final Properties getProperties() {
    return (Properties)this.serviceProperties.clone();
    /*
    Properties p = new Properties();
    getProperties(tc, p, true /*stringsOnly*, false /*!defaultsOnly*);

    return p;
    */
  }

  /*
  /**
   * DOCUMENT ME!
   *
   * @param  tc  DOCUMENT ME!
   * @param  d  DOCUMENT ME!
   * @param  stringsOnly  DOCUMENT ME!
   * @param  defaultsOnly  DOCUMENT ME!
   *
   * @throws  StandardException  DOCUMENT ME!
   *
  public void getProperties(TransactionController tc,
                            Dictionary d,
                            boolean stringsOnly,
                            boolean defaultsOnly) throws StandardException {

    // See if I'm the exclusive owner. If so I cannot populate
    // the cache as it would contain my uncommitted changes.
    if (iHoldTheUpdateLock(tc)) {
      Dictionary dbProps = readDbProperties(tc);
      Dictionary defaults = (Dictionary) dbProps.get(
                              AccessFactoryGlobals.DEFAULT_PROPERTY_NAME);
      copyValues(d, defaults, stringsOnly);

      if (!defaultsOnly) {
        copyValues(d, dbProps, stringsOnly);
      }
    }
    else {
      Dictionary dbProps = getCachedDbProperties(tc);
      Dictionary defaults = (Dictionary) dbProps.get(
                              AccessFactoryGlobals.DEFAULT_PROPERTY_NAME);
      copyValues(d, defaults, stringsOnly);

      if (!defaultsOnly) {
        copyValues(d, dbProps, stringsOnly);
      }
    }
  }

  /**
   * DOCUMENT ME!
   *
  void resetCache() {
    cachedSet = null;
  }

  /**
   * Read the database properties and add in the service set.
   *
  private Dictionary readDbProperties(TransactionController tc)
  throws StandardException {
    Dictionary set = new Hashtable();

    // scan the table for a row with no matching "key"
    ScanController scan = openScan(tc, (String) null, 0);

    DataValueDescriptor[] dvds = makeNewTemplate();
    
    ExecRow xrow = new ValueRow(dvds.length);
    xrow.setRowArray(dvds);

    while (scan.fetchNext(xrow)) {
      DataValueDescriptor[] row = xrow.getRowArray();
      
      Object key = ((UserType) row[0]).getObject();
      Object value = ((UserType) row[1]).getObject();

      if (SanityManager.DEBUG) {

        if (!(key instanceof String)) {
          SanityManager.THROWASSERT("Key is not a string " +
                                    key.getClass().getName());
        }
      }

      set.put(key, value);
    }

    scan.close();

    // add the known properties from the service properties set
    for (int i = 0; i < PropertyUtil.servicePropertyList.length; i++) {
      String value = serviceProperties.getProperty(PropertyUtil
                                                   .servicePropertyList[i]);

      if (value != null) {
        set.put(PropertyUtil.servicePropertyList[i], value);
      }
    }

    return set;
  }

  /**
   * DOCUMENT ME!
   *
   * @param  tc  DOCUMENT ME!
   *
   * @return  DOCUMENT ME!
   *
   * @throws  StandardException  DOCUMENT ME!
   *
  private Dictionary getCachedDbProperties(TransactionController tc)
  throws StandardException {
    Dictionary dbProps = cachedSet;

    // Get the cached set of properties.
    if (dbProps == null) {
      dbProps = readDbProperties(tc);
      cachedSet = dbProps;
    }

    return dbProps;
  }

  /**
   * Lock the database properties for an update.
   *
  void lockProperties(TransactionController tc) throws StandardException {
    /*
    // lock the property set until the transaction commits.
    // This allows correct operation of the cache. The cache remains
    // valid for all transactions except the one that is modifying
    // it. Thus readers see the old uncommited values. When this
    // thread releases its exclusive lock the cached is cleared
    // and the next reader will re-populate the cache.
    CompatibilitySpace cs = tc.getLockSpace();
    Object csGroup = cs.getOwner();
    lf.lockObject(cs, csGroup, cachedLock, ShExQual.EX,
                  C_LockFactory.TIMED_WAIT);
     *

  }

  /**
   * Return true if the caller holds the exclusive update lock on the property
   * conglomerate.
   *
  private boolean iHoldTheUpdateLock(TransactionController tc)
  throws StandardException {
    return false;
           /*
           CompatibilitySpace cs = tc.getLockSpace();
           Object csGroup = cs.getOwner();
           return lf.isLockHeld(cs, csGroup, cachedLock, ShExQual.EX);
            *
  }
  */
}
