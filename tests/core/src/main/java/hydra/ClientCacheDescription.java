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

package hydra;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.client.*;
import com.gemstone.gemfire.internal.cache.CacheConfig;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.xmlcache.ClientCacheCreation;
import com.gemstone.gemfire.pdx.PdxSerializer;
import java.io.*;
import java.util.*;

/**
 * Encodes information needed to describe and create a client cache.
 */
public class ClientCacheDescription extends AbstractDescription
implements Serializable {

  private static final long serialVersionUID = 1L;

  /** The logical name of this client cache description */
  private String name;

  /** Remaining parameters, in alphabetical order */
  private DiskStoreDescription defaultDiskStoreDescription; // from defaultDiskStoreName
  private String defaultDiskStoreDescriptionName;
  private PoolDescription defaultPoolDescription; // from defaultPoolName
  private String defaultPoolDescriptionName;
  private DiskStoreDescription pdxDiskStoreDescription; // from pdxDiskStoreName
  private String pdxDiskStoreName;
  private Boolean pdxIgnoreUnreadFields;
  private Boolean pdxPersistent;
  private Boolean pdxReadSerialized;
  private String pdxSerializerClass;
  private String pdxSerializerMethod;

  /** Cached fields */
  private transient PdxSerializer pdxSerializerInstance;

//------------------------------------------------------------------------------
// Constructors
//------------------------------------------------------------------------------

  public ClientCacheDescription() {
  }

//------------------------------------------------------------------------------
// Accessors, in aphabetical order after name
//------------------------------------------------------------------------------

  /**
   * Returns the logical name of this client cache description.
   */
  public String getName() {
    return this.name;
  }

  /**
   * Sets the logical name of this client cache description.
   */
  private void setName(String str) {
    this.name = str;
  }

  /**
   * Returns the default disk store description name.
   */
  protected String getDefaultDiskStoreDescriptionName() {
    return this.defaultDiskStoreDescriptionName;
  }

  /**
   * Sets the default disk store description name.
   */
  private void setDefaultDiskStoreDescriptionName(String str) {
    this.defaultDiskStoreDescriptionName = str;
  }

  /**
   * Returns the default disk store description.
   */
  public DiskStoreDescription getDefaultDiskStoreDescription() {
    return this.defaultDiskStoreDescription;
  }

  /**
   * Sets the default disk store description.
   */
  private void setDefaultDiskStoreDescription(DiskStoreDescription dsd) {
    this.defaultDiskStoreDescription = dsd;
  }

  /**
   * Returns the default pool description name.
   */
  private String getDefaultPoolDescriptionName() {
    return this.defaultPoolDescriptionName;
  }

  /**
   * Sets the default pool description name.
   */
  private void setDefaultPoolDescriptionName(String str) {
    this.defaultPoolDescriptionName = str;
  }

  /**
   * Returns the default pool description.
   */
  public PoolDescription getDefaultPoolDescription() {
    return this.defaultPoolDescription;
  }

  /**
   * Sets the default pool description.
   */
  private void setDefaultPoolDescription(PoolDescription pd) {
    this.defaultPoolDescription = pd;
  }

  /**
   * Returns the pdx disk store description name.
   */
  private String getPdxDiskStoreName() {
    return this.pdxDiskStoreName;
  }

  /**
   * Sets the pdx disk store description name.
   */
  private void setPdxDiskStoreName(String str) {
    this.pdxDiskStoreName = str;
  }

  /**
   * Returns the pdx disk store description.
   */
  public DiskStoreDescription getPdxDiskStoreDescription() {
    return this.pdxDiskStoreDescription;
  }

  /**
   * Sets the pdx disk store description.
   */
  private void setPdxDiskStoreDescription(DiskStoreDescription dsd) {
    this.pdxDiskStoreDescription = dsd;
  }

  /**
   * Returns the pdx ignore unread fields.
   */
  private Boolean getPdxIgnoreUnreadFields() {
    return this.pdxIgnoreUnreadFields;
  }

  /**
   * Sets the pdx ignore unread fields.
   */
  private void setPdxIgnoreUnreadFields(Boolean bool) {
    this.pdxIgnoreUnreadFields = bool;
  }

  /**
   * Returns the pdx persistent.
   */
  private Boolean getPdxPersistent() {
    return this.pdxPersistent;
  }

  /**
   * Sets the pdx persistent.
   */
  private void setPdxPersistent(Boolean bool) {
    this.pdxPersistent = bool;
  }

  /**
   * Returns the pdx read serialized.
   */
  private Boolean getPdxReadSerialized() {
    return this.pdxReadSerialized;
  }

  /**
   * Sets the pdx read serialized.
   */
  private void setPdxReadSerialized(Boolean bool) {
    this.pdxReadSerialized = bool;
  }

  /**
   * Returns the pdx serializer class.
   */
  private String getPdxSerializerClass() {
    return this.pdxSerializerClass;
  }

  /**
   * Sets the pdx serializer class.
   */
  private void setPdxSerializerClass(String str) {
    this.pdxSerializerClass = str;
  }

  /**
   * Returns the pdx serializer method.
   */
  private String getPdxSerializerMethod() {
    return this.pdxSerializerMethod;
  }

  /**
   * Sets the pdx serializer method.
   */
  private void setPdxSerializerMethod(String str) {
    this.pdxSerializerMethod = str;
  }

  /**
   * Returns the singleton pdx serializer instance for this description.
   *
   * @throws HydraConfigException if instantiation fails or the class does not
   *                              implement PdxSerializer.
   */
  private synchronized PdxSerializer getPdxSerializerInstance() {
    if (this.pdxSerializerInstance == null) {
      this.pdxSerializerInstance = createPdxSerializerInstance();
    }
    return this.pdxSerializerInstance;
  }

//------------------------------------------------------------------------------
// Cache configuration
//------------------------------------------------------------------------------

  /**
   * Configures the client cache factory using this client cache description.
   * Instantiates and caches the pdx serializer, if any.  Does not configure
   * the default disk store, if any, since that must wait until after the cache
   * is created and can provide a disk store factory.
   */
  public void configure(ClientCacheFactory f) {
    PoolDescription pd = this.getDefaultPoolDescription();
    if (pd != null) {
      pd.configure(f);
    }
    if (this.getPdxDiskStoreDescription() != null) {
      f.setPdxDiskStore(this.getPdxDiskStoreDescription().getName());
    }
    f.setPdxIgnoreUnreadFields(this.getPdxIgnoreUnreadFields());
    f.setPdxPersistent(this.getPdxPersistent());
    f.setPdxReadSerialized(this.getPdxReadSerialized());
    if (this.getPdxSerializerClass() != null) {
      f.setPdxSerializer(this.getPdxSerializerInstance());
    }
  }

  /**
   * Configures the dummy client cache using this client cache description.
   * Instantiates and caches the pdx serializer, if any.
   */
  public void configureDummy(ClientCacheCreation c) {
    if (this.getPdxDiskStoreDescription() != null) {
      c.setPdxDiskStore(this.getPdxDiskStoreDescription().getName());
    }
    c.setPdxIgnoreUnreadFields(this.getPdxIgnoreUnreadFields());
    c.setPdxPersistent(this.getPdxPersistent());
    c.setPdxReadSerialized(this.getPdxReadSerialized());
    if (this.getPdxSerializerClass() != null) {
      c.setPdxSerializer(this.getPdxSerializerInstance());
    }
  }

  /**
   * Returns the client cache as a string.  For use only by {@link
   * ClientCacheHelper#cacheToString(ClientCache)}.
   */
  protected static synchronized String cacheToString(ClientCache c) {
    StringBuffer buf = new StringBuffer();
    Pool p = null;
    if (c instanceof ClientCacheCreation) {
      String pname = ((ClientCacheCreation)c).getDefaultPoolName();
      if (pname != null) {
        p = (Pool)((ClientCacheCreation)c).getPools().get(pname);
      }
    } else {
      p = ((GemFireCacheImpl)c).getDefaultPool();
    }
    if (p == null) {
      buf.append("\n defaultPool: null");
    } else {
      buf.append("\n defaultPool: " + p.getName() + " with attributes:");
      buf.append(PoolDescription.poolToString(p));
      buf.append("\n --------------------");
    }
    buf.append("\n  pdxDiskStoreName: " + c.getPdxDiskStore());
    buf.append("\n  pdxIgnoreUnreadFields: " + c.getPdxIgnoreUnreadFields());
    buf.append("\n  pdxPersistent: " + c.getPdxPersistent());
    buf.append("\n  pdxReadSerialized: " + c.getPdxReadSerialized());
    buf.append("\n  pdxSerializer: " + c.getPdxSerializer());
    return buf.toString();
  }

//------------------------------------------------------------------------------
// Printing
//------------------------------------------------------------------------------

  public SortedMap toSortedMap() {
    SortedMap map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    map.put(header + "defaultDiskStoreName", this.getDefaultDiskStoreDescriptionName());
    map.put(header + "defaultPoolName", this.getDefaultPoolDescriptionName());
    map.put(header + "pdxDiskStoreName", this.getPdxDiskStoreName());
    map.put(header + "pdxIgnoreUnreadFields", this.getPdxIgnoreUnreadFields());
    map.put(header + "pdxPersistent", this.getPdxPersistent());
    map.put(header + "pdxReadSerialized", this.getPdxReadSerialized());
    map.put(header + "pdxSerializer", this.getPdxSerializerClass() + "." +
                                      this.getPdxSerializerMethod());
    return map;
  }

//------------------------------------------------------------------------------
// Configuration
//------------------------------------------------------------------------------

  /**
   * Creates client cache descriptions from the client cache parameters in the
   * test configuration.
   */
  protected static void configure(TestConfig config) {

    ConfigHashtable tab = config.getParameters();

    // create a description for each cache name
    Vector names = tab.vecAt(ClientCachePrms.names, new HydraVector());

    for (int i = 0; i < names.size(); i++) {

      String name = (String)names.elementAt(i);

      // create cache description from test configuration parameters
      ClientCacheDescription ccd = createClientCacheDescription(name, config, i);

      // save configuration
      config.addClientCacheDescription(ccd);
    }
  }

  /**
   * Creates the cache description using test configuration parameters and
   * product defaults.
   */
  private static ClientCacheDescription createClientCacheDescription(String name,
                 TestConfig config, int index) {

    ConfigHashtable tab = config.getParameters();

    ClientCacheDescription ccd = new ClientCacheDescription();
    ccd.setName(name);

    // defaultDiskStoreName (generates DiskStoreDescription)
    {
      Long key = ClientCachePrms.defaultDiskStoreName;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        ccd.setDefaultDiskStoreDescription(getDiskStoreDescription(str, key, config));
        ccd.setDefaultDiskStoreDescriptionName("DiskStoreDescription." + str);
      }
    }
    // defaultPoolName (generates PoolDescription)
    {
      Long key = ClientCachePrms.defaultPoolName;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        ccd.setDefaultPoolDescription(getPoolDescription(str, key, config));
        ccd.setDefaultPoolDescriptionName("PoolDescription." + str);
      }
    }
    // pdxDiskStoreName (generates pdxDiskStoreDescription)
    {
      Long key = CachePrms.pdxDiskStoreName;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        ccd.setPdxDiskStoreDescription(
           getPdxDiskStoreDescription(str, key, config));
        ccd.setPdxDiskStoreName("DiskStoreDescription." + str);
      }
    }
    // pdxIgnoreUnreadFields
    {
      Long key = CachePrms.pdxIgnoreUnreadFields;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        ccd.setPdxIgnoreUnreadFields(CacheConfig.DEFAULT_PDX_IGNORE_UNREAD_FIELDS);
      } else {
        ccd.setPdxIgnoreUnreadFields(bool);
      }
    }
    // pdxPersistent
    {
      Long key = CachePrms.pdxPersistent;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        ccd.setPdxPersistent(CacheConfig.DEFAULT_PDX_PERSISTENT);
      } else {
        ccd.setPdxPersistent(bool);
      }
    }
    // pdxReadSerialized
    {
      Long key = CachePrms.pdxReadSerialized;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        ccd.setPdxReadSerialized(CacheConfig.DEFAULT_PDX_READ_SERIALIZED);
      } else {
        ccd.setPdxReadSerialized(bool);
      }
    }
    // pdxSerializer
    {
      Long key = CachePrms.pdxSerializerInstantiator;
      Vector strs = tab.vecAtWild(key, index, null);
      ccd.setPdxSerializerClass(getPdxSerializerClass(strs, key));
      ccd.setPdxSerializerMethod(getPdxSerializerMethod(strs, key));
    }
    return ccd;
  }

//------------------------------------------------------------------------------
// Default disk store configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the disk store description for the given string.
   * @throws HydraConfigException if the given string is not listed in {@link
   *         DiskStorePrms#names}.
   */
  private static DiskStoreDescription getDiskStoreDescription(String str,
                                      Long key, TestConfig config) {
    DiskStoreDescription dsd = config.getDiskStoreDescription(str);
    if (dsd == null) {
      String s = BasePrms.nameForKey(key) + " not found in "
               + BasePrms.nameForKey(DiskStorePrms.names) + ": " + str;
      throw new HydraConfigException(s);
    } else {
      return dsd;
    }
  }

//------------------------------------------------------------------------------
// Default pool configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the pool description for the given string.
   * @throws HydraConfigException if the given string is not listed in {@link
   *         PoolPrms#names}.
   */
  private static PoolDescription getPoolDescription(String str, Long key,
                                                    TestConfig config) {
    PoolDescription pd = config.getPoolDescription(str);
    if (pd == null) {
      String s = BasePrms.nameForKey(key) + " not found in "
               + BasePrms.nameForKey(PoolPrms.names) + ": " + str;
      throw new HydraConfigException(s);
    } else {
      return pd;
    }
  }

//------------------------------------------------------------------------------
// PDX disk store configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the pdx disk store description for the given string.
   * @throws HydraConfigException if the given string is not listed in {@link
   *         DiskStorePrms#names}.
   */
  private static DiskStoreDescription getPdxDiskStoreDescription(String str,
                                                  Long key, TestConfig config) {
    DiskStoreDescription dsd = config.getDiskStoreDescription(str);
    if (dsd == null) {
      String s = BasePrms.nameForKey(key) + " not found in "
               + BasePrms.nameForKey(DiskStorePrms.names) + ": " + str;
      throw new HydraConfigException(s);
    } else {
      return dsd;
    }
  }

//------------------------------------------------------------------------------
// PDX serializer configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the PdxSerializer class for the given PdxSerializer instantiator.
   * @throws HydraConfigException if the instantiator is malformed.
   */
  private static String getPdxSerializerClass(Vector strs, Long key) {
    if (strs == null || strs.size() == 0) { // default
      return null;
    } else if (strs.size() == 2) {
      return (String)strs.get(0);
    } else {
      String s = BasePrms.nameForKey(key) + " has illegal value: " + strs;
      throw new HydraConfigException(s);
    }
  }

  /**
   * Returns the PdxSerializer method for the given PdxSerializer instantiator.
   * @throws HydraConfigException if the instantiator is malformed.
   */
  private static String getPdxSerializerMethod(Vector strs, Long key) {
    if (strs == null || strs.size() == 0) { // default
      return null;
    } else if (strs.size() == 2) {
      return (String)strs.get(1);
    } else {
      String s = BasePrms.nameForKey(key) + " has illegal value: " + strs;
      throw new HydraConfigException(s);
    }
  }
  /**
   * Creates a pdx serializer instance for this description.
   * @throws HydraConfigException if instantiation fails or the class does not
   *                              implement PdxSerializer.
   */
  private PdxSerializer createPdxSerializerInstance() {
    Long key = CachePrms.pdxSerializerInstantiator;
    String classname = this.getPdxSerializerClass();
    String methodname = this.getPdxSerializerMethod();
    MethExecutorResult result = MethExecutor.execute(classname, methodname);
    if (result.getStackTrace() != null){
      throw new HydraRuntimeException(result.toString());
    }
    Object obj = result.getResult();
    Log.getLogWriter().info("Instantiated  " + obj);
    try {
      return (PdxSerializer)obj;
    } catch (ClassCastException e) {
      String s = BasePrms.nameForKey(key) + "="
               + this.getPdxSerializerClass() + "."
               + this.getPdxSerializerMethod()
               + " returned an instance of " + obj.getClass().getName()
               + " instead of a PdxSerializer";
      throw new HydraConfigException(s);
    }
  }

//------------------------------------------------------------------------------
// Version support
//------------------------------------------------------------------------------

  /**
   * Custom deserialization.
   */
  private void readObject(java.io.ObjectInputStream in)
  throws IOException, ClassNotFoundException {
    this.name = (String)in.readObject();
    this.defaultDiskStoreDescription = (DiskStoreDescription)in.readObject();
    this.defaultDiskStoreDescriptionName = (String)in.readObject();
    this.defaultPoolDescription = (PoolDescription)in.readObject();
    this.defaultPoolDescriptionName = (String)in.readObject();
    this.pdxDiskStoreName = (String)in.readObject();
    this.pdxDiskStoreDescription = (DiskStoreDescription)in.readObject();
    this.pdxIgnoreUnreadFields = (Boolean)in.readObject();
    this.pdxPersistent = (Boolean)in.readObject();
    this.pdxReadSerialized = (Boolean)in.readObject();
    this.pdxSerializerClass = (String)in.readObject();
    this.pdxSerializerMethod = (String)in.readObject();
  }

  /**
   * Custom serialization.
   */
  private void writeObject(java.io.ObjectOutputStream out)
  throws IOException {
    out.writeObject(this.name);
    out.writeObject(this.defaultDiskStoreDescription);
    out.writeObject(this.defaultDiskStoreDescriptionName);
    out.writeObject(this.defaultPoolDescription);
    out.writeObject(this.defaultPoolDescriptionName);
    out.writeObject(this.pdxDiskStoreName);
    out.writeObject(this.pdxDiskStoreDescription);
    out.writeObject(this.pdxIgnoreUnreadFields);
    out.writeObject(this.pdxPersistent);
    out.writeObject(this.pdxReadSerialized);
    out.writeObject(this.pdxSerializerClass);
    out.writeObject(this.pdxSerializerMethod);
  }
}
