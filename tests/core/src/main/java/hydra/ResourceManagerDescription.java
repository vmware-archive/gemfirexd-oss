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

import com.gemstone.gemfire.cache.control.ResourceManager;

import java.io.Serializable;
import java.util.*;

/**
 * Encodes information needed to describe and create a resource manager.
 */
public class ResourceManagerDescription extends AbstractDescription
implements Serializable {

  private static final long serialVersionUID = 1L;

  /** The logical name of this resource manager description */
  private String name;

  /** Remaining parameters, in alphabetical order */
  private Float criticalHeapPercentage;
  private Float criticalOffHeapPercentage;
  private Float evictionHeapPercentage;
  private Float evictionOffHeapPercentage;

//------------------------------------------------------------------------------
// Constructors
//------------------------------------------------------------------------------

  public ResourceManagerDescription() {
  }

//------------------------------------------------------------------------------
// Accessors, in aphabetical order after name
//------------------------------------------------------------------------------

  /**
   * Returns the logical name of this resource manager description.
   */
  public String getName() {
    return this.name;
  }

  /**
   * Sets the logical name of this resource manager description.
   */
  private void setName(String str) {
    this.name = str;
  }

  /**
   * Returns the critical heap percentage.
   */
  private Float getCriticalHeapPercentage() {
    return this.criticalHeapPercentage;
  }
  
  /**
   * Sets the critical heap percentage.
   */
  private void setCriticalHeapPercentage(Float f) {
    this.criticalHeapPercentage = f;
  }
  
  /**
   * Returns the critical off-heap percentage.
   */
  private Float getCriticalOffHeapPercentage() {
    return this.criticalOffHeapPercentage;
  }

  /**
   * Sets the critical off-heap percentage.
   */
  private void setCriticalOffHeapPercentage(Float f) {
    this.criticalOffHeapPercentage = f;
  }

  /**
   * Returns the eviction heap percentage.
   */
  private Float getEvictionHeapPercentage() {
    return this.evictionHeapPercentage;
  }
  
  /**
   * Sets the eviction heap percentage.
   */
  private void setEvictionHeapPercentage(Float f) {
    this.evictionHeapPercentage = f;
  }
  
  /**
   * Returns the eviction off-heap percentage.
   */
  private Float getEvictionOffHeapPercentage() {
    return this.evictionOffHeapPercentage;
  }

  /**
   * Sets the eviction off-heap percentage.
   */
  private void setEvictionOffHeapPercentage(Float f) {
    this.evictionOffHeapPercentage = f;
  }
  
//------------------------------------------------------------------------------
// Resource manager configuration
//------------------------------------------------------------------------------

  /**
   * Configures the resource manager using this description.
   */
  public void configure(ResourceManager rm) {
    if (this.getCriticalHeapPercentage() != null) {
      rm.setCriticalHeapPercentage(this.getCriticalHeapPercentage().floatValue());
    }
    if (this.getCriticalOffHeapPercentage() != null) {
      rm.setCriticalOffHeapPercentage(this.getCriticalOffHeapPercentage().floatValue());
    }
    if (this.getEvictionHeapPercentage() != null) {
      rm.setEvictionHeapPercentage(this.getEvictionHeapPercentage().floatValue());
    }
    if (this.getEvictionOffHeapPercentage() != null) {
      rm.setEvictionOffHeapPercentage(this.getEvictionOffHeapPercentage().floatValue());
    }
  }

//------------------------------------------------------------------------------
// Printing
//------------------------------------------------------------------------------

  public SortedMap toSortedMap() {
    SortedMap map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    if (this.criticalHeapPercentage == null) {
      map.put(header + "criticalHeapPercentage", "computed");
    } else {
      map.put(header + "criticalHeapPercentage", this.getCriticalHeapPercentage());
    }
    if (this.criticalOffHeapPercentage == null) {
      map.put(header + "criticalOffHeapPercentage", "computed");
    } else {
      map.put(header + "criticalOffHeapPercentage", this.getCriticalOffHeapPercentage());
    }
    if (this.evictionHeapPercentage == null) {
      map.put(header + "evictionHeapPercentage", "computed");
    } else {
      map.put(header + "evictionHeapPercentage", this.getEvictionHeapPercentage());
    }
    if (this.evictionOffHeapPercentage == null) {
      map.put(header + "evictionOffHeapPercentage", "computed");
    } else {
      map.put(header + "evictionOffHeapPercentage", this.getEvictionOffHeapPercentage());
    }
    return map;
  }

//------------------------------------------------------------------------------
// Configuration
//------------------------------------------------------------------------------

  /**
   * Creates resource manager descriptions from the resource manager parameters
   * in the test configuration.
   */
  protected static void configure(TestConfig config) {

    ConfigHashtable tab = config.getParameters();

    // create a description for each resource manager name
    Vector names = tab.vecAt(ResourceManagerPrms.names, new HydraVector());

    for (int i = 0; i < names.size(); i++) {

      String name = (String)names.elementAt(i);

      // create resource manager description from test configuration parameters
      // and do hydra-level validation
      ResourceManagerDescription rmd =
        createResourceManagerDescription(name, config, i);

      // save configuration
      config.addResourceManagerDescription(rmd);
    }
  }

  /**
   * Creates the initial resource manager description using test configuration
   * parameters and does hydra-level validation.
   */
  private static ResourceManagerDescription createResourceManagerDescription(
                 String name, TestConfig config, int index) {

    ConfigHashtable tab = config.getParameters();

    ResourceManagerDescription rmd = new ResourceManagerDescription();
    rmd.setName(name);

    // criticalHeapPercentage
    {
      Long key = ResourceManagerPrms.criticalHeapPercentage;
      Double d = tab.getDouble(key, tab.getWild(key, index, null));
      if (d != null) {
        rmd.setCriticalHeapPercentage(Float.valueOf(d.floatValue()));
      }
    }
    // criticalOffHeapPercentage
    {
      Long key = ResourceManagerPrms.criticalOffHeapPercentage;
      Double d = tab.getDouble(key, tab.getWild(key, index, null));
      if (d != null) {
        rmd.setCriticalOffHeapPercentage(Float.valueOf(d.floatValue()));
      }
    }
    // evictionHeapPercentage
    {
      Long key = ResourceManagerPrms.evictionHeapPercentage;
      Double d = tab.getDouble(key, tab.getWild(key, index, null));
      if (d != null) {
        rmd.setEvictionHeapPercentage(Float.valueOf(d.floatValue()));
      }
    }
    // evictionOffHeapPercentage
    {
      Long key = ResourceManagerPrms.evictionOffHeapPercentage;
      Double d = tab.getDouble(key, tab.getWild(key, index, null));
      if (d != null) {
        rmd.setEvictionOffHeapPercentage(Float.valueOf(d.floatValue()));
      }
    }
    return rmd;
  }
}
