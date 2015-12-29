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

package hydra.gemfirexd;

import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

import hydra.FileUtil;
import hydra.Log;
import hydra.HydraRuntimeException;
import hydra.TestConfig;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map;

/**
 * A GfxdTestConfig contains the GemFireXD-specific configuration parsed
 * from a hydra test configuration file.
 * <p>
 * The class maintains this instance as a singleton that can be accessed
 * from any hydra client VM via {@link #getInstance}.
 */
public class GfxdTestConfig implements Serializable {

  private static transient GfxdTestConfig singleton;
  private static final String GFXDConfigFileName = "gfxdConfig.obj";

  private Map<String,FabricServerDescription> FabricServerDescriptions;
  private Map<String,FabricSecurityDescription> FabricSecurityDescriptions;
  private Map<String,GatewayReceiverDescription> GatewayReceiverDescriptions;
  private Map<String,GatewaySenderDescription> GatewaySenderDescriptions;
  private Map<String,LonerDescription> LonerDescriptions;
  private Map<String,NetworkServerDescription> NetworkServerDescriptions;
  private Map<String,ThinClientDescription> ThinClientDescriptions;
  private Map<String,DiskStoreDescription> DiskStoreDescriptions;
  private Map<String,HDFSStoreDescription> HDFSStoreDescriptions;

//------------------------------------------------------------------------------

  /**
   * Gets the singleton instance of the test configuration.  Lazily
   * deserializes the configuration from a file.
   */
  public static synchronized GfxdTestConfig getInstance() {
    if (singleton == null) {
      Log.getLogWriter().info("Deserializing gemfirexd test configuration...");
      String fn = System.getProperty("user.dir") + "/" + GFXDConfigFileName;
      if (FileUtil.exists(fn)) {
        singleton = (GfxdTestConfig)FileUtil.deserialize(fn);
      } else {
        String s = GFXDConfigFileName + " not found";
        throw new HydraRuntimeException(s);
      }
    }
    return singleton;
  }

//------------------------------------------------------------------------------

  public static Properties getGemFireXDProductVersion() {
    String fn = "com/pivotal/gemfirexd/internal/GemFireXDVersion.properties";
    try {
      return FileUtil.getPropertiesFromResource(fn);
    } catch (IOException e) {
      String s = "Unable to find GemFireXD product version";
      throw new HydraRuntimeException(s, e);
    }
  }

  // master only
  public void logBuildSanity() {
    boolean sanity = SanityManager.ASSERT || SanityManager.DEBUG;
    Log.getLogWriter().info("GemFireXD Build Sanity: " + sanity);
  }

  // master only
  public void configure(TestConfig tc) {
    logBuildSanity();
    LonerDescriptions = LonerDescription.configure(tc, this);
    NetworkServerDescriptions = NetworkServerDescription.configure(tc, this);
    FabricSecurityDescriptions = FabricSecurityDescription.configure(tc, this);
    FabricServerDescriptions = FabricServerDescription.configure(tc, this);
    GatewayReceiverDescriptions = GatewayReceiverDescription.configure(tc, this);
    GatewaySenderDescriptions = GatewaySenderDescription.configure(tc, this);
    ThinClientDescriptions = ThinClientDescription.configure(tc, this);
    DiskStoreDescriptions = DiskStoreDescription.configure(tc, this);
    HDFSStoreDescriptions = HDFSStoreDescription.configure(tc, this);
    tc.share(); // first write basic configuration
    share(tc); // then append and write gemfirexd configuration
  }

  private void share(TestConfig tc) {
    String userDir = tc.getMasterDescription().getVmDescription()
                       .getHostDescription().getUserDir();
    String fn = userDir + "/" + GFXDConfigFileName;
    FileUtil.serialize(this, fn);
    String latest = userDir + "/latest.prop";
    String content = this.toString();
    content = content.replace('\\', '/');
    FileUtil.appendToFile(latest, content);
    Log.getLogWriter().info(this.toString());
  }

  private SortedMap<String,Object> toSortedMap() {
    SortedMap<String,Object> map = new TreeMap();
    if (FabricServerDescriptions.size() != 0) {
      for (FabricServerDescription fsd : FabricServerDescriptions.values()) {
        map.putAll(fsd.toSortedMap());
      }
    }
    if (FabricSecurityDescriptions.size() != 0) {
      for (FabricSecurityDescription fsd : FabricSecurityDescriptions.values()) {
        map.putAll(fsd.toSortedMap());
      }
    }
    if (GatewayReceiverDescriptions.size() != 0) {
      for (GatewayReceiverDescription grd : GatewayReceiverDescriptions.values()) {
        map.putAll(grd.toSortedMap());
      }
    }
    if (GatewaySenderDescriptions.size() != 0) {
      for (GatewaySenderDescription gsd : GatewaySenderDescriptions.values()) {
        map.putAll(gsd.toSortedMap());
      }
    }
    if (DiskStoreDescriptions.size() != 0) {
      for (DiskStoreDescription dsd : DiskStoreDescriptions.values()) {
        map.putAll(dsd.toSortedMap());
      }
    }
    if (HDFSStoreDescriptions.size() != 0) {
      for (HDFSStoreDescription hsd : HDFSStoreDescriptions.values()) {
        map.putAll(hsd.toSortedMap());
      }
    }
    if (LonerDescriptions.size() != 0) {
      for (LonerDescription ld : LonerDescriptions.values()) {
        map.putAll(ld.toSortedMap());
      }
    }
    if (NetworkServerDescriptions.size() != 0) {
      for (NetworkServerDescription fsd : NetworkServerDescriptions.values()) {
        map.putAll(fsd.toSortedMap());
      }
    }
    if (ThinClientDescriptions.size() != 0) {
      for (ThinClientDescription tcd : ThinClientDescriptions.values()) {
        map.putAll(tcd.toSortedMap());
      }
    }
    return map;
  }

  public String toString() {
    StringBuffer buf = new StringBuffer();
    SortedMap<String,Object> map = this.toSortedMap();
    for (String key : map.keySet()) {
      Object val = map.get(key);
      buf.append(key + "=" + val + "\n");
    }
    return buf.toString();
  }

//------------------------------------------------------------------------------
// FabricServerDescription
//------------------------------------------------------------------------------

  /**
   * Returns all FabricServerDescriptions in the test configuration.
   */
  public Map<String,FabricServerDescription> getFabricServerDescriptions() {
    return FabricServerDescriptions;
  }

  /**
   * Returns the FabricServerDescription with the specified name. 
   */
  public FabricServerDescription getFabricServerDescription(String name) {
    return FabricServerDescriptions.get(name);
  }

//------------------------------------------------------------------------------
// FabricSecurityDescription
//------------------------------------------------------------------------------

  /**
   * Returns all FabricSecurityDescriptions in the test configuration.
   */
  public Map<String,FabricSecurityDescription> getFabricSecurityDescriptions() {
    return FabricSecurityDescriptions;
  }

  /**
   * Returns the FabricSecurityDescription with the specified name. 
   */
  public FabricSecurityDescription getFabricSecurityDescription(String name) {
    return FabricSecurityDescriptions.get(name);
  }

//------------------------------------------------------------------------------
// GatewayReceiverDescription
//------------------------------------------------------------------------------

  /**
   * Returns all GatewayReceiverDescriptions in the test configuration.
   */
  public Map<String,GatewayReceiverDescription> getGatewayReceiverDescriptions() {
    return GatewayReceiverDescriptions;
  }

  /**
   * Returns the GatewayReceiverDescription with the specified name. 
   */
  public GatewayReceiverDescription getGatewayReceiverDescription(String name) {
    return GatewayReceiverDescriptions.get(name);
  }

//------------------------------------------------------------------------------
// GatewaySenderDescription
//------------------------------------------------------------------------------

  /**
   * Returns all GatewaySenderDescriptions in the test configuration.
   */
  public Map<String,GatewaySenderDescription> getGatewaySenderDescriptions() {
    return GatewaySenderDescriptions;
  }

  /**
   * Returns the GatewaySenderDescription with the specified name. 
   */
  public GatewaySenderDescription getGatewaySenderDescription(String name) {
    return GatewaySenderDescriptions.get(name);
  }

//------------------------------------------------------------------------------
// DiskStoreDescription
//------------------------------------------------------------------------------

  public Map getDiskStoreDescriptions() {
    return DiskStoreDescriptions;
  }

  public DiskStoreDescription getDiskStoreDescription(String name) {
    return (DiskStoreDescription)DiskStoreDescriptions.get(name);
  }

//------------------------------------------------------------------------------
// HDFSStoreDescription
//------------------------------------------------------------------------------

  public Map getHDFSStoreDescriptions() {
    return HDFSStoreDescriptions;
  }

  public HDFSStoreDescription getHDFSStoreDescription(String name) {
    return (HDFSStoreDescription)HDFSStoreDescriptions.get(name);
  }

//------------------------------------------------------------------------------
// LonerDescription
//------------------------------------------------------------------------------

  /**
   * Returns all LonerDescriptions in the test configuration.
   */
  public Map<String,LonerDescription> getLonerDescriptions() {
    return LonerDescriptions;
  }

  /**
   * Returns the LonerDescription with the specified name. 
   */
  public LonerDescription getLonerDescription(String name) {
    return LonerDescriptions.get(name);
  }

//------------------------------------------------------------------------------
// NetworkServerDescription
//------------------------------------------------------------------------------

  /**
   * Returns all NetworkServerDescriptions in the test configuration.
   */
  public Map<String,NetworkServerDescription> getNetworkServerDescriptions() {
    return NetworkServerDescriptions;
  }

  /**
   * Returns the NetworkServerDescription with the specified name. 
   */
  public NetworkServerDescription getNetworkServerDescription(String name) {
    return NetworkServerDescriptions.get(name);
  }

//------------------------------------------------------------------------------
// ThinClientDescription
//------------------------------------------------------------------------------

  /**
   * Returns all ThinClientDescriptions in the test configuration.
   */
  public Map<String,ThinClientDescription> getThinClientDescriptions() {
    return ThinClientDescriptions;
  }

  /**
   * Returns the ThinClientDescription with the specified name. 
   */
  public ThinClientDescription getThinClientDescription(String name) {
    return ThinClientDescriptions.get(name);
  }
}
