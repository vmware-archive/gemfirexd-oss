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

import java.io.Serializable;
import java.util.*;

/**
 * Encodes information needed to describe and create a fixed partition.
 */
public class FixedPartitionDescription extends AbstractDescription
implements Serializable {

  /** The logical name of this fixed partition description */
  private String name;

  /** Remaining parameters, in alphabetical order */
  private Integer datastores;
  private String mappingClass;
  private String mappingMethod;
  private List<String> partitionNames;
  private List<Integer> partitionBuckets;

  /** Cached fields */
  private transient List<FixedPartitionAttributes> fixedPartitionAttributes;

//------------------------------------------------------------------------------
// Constructors
//------------------------------------------------------------------------------

  public FixedPartitionDescription() {
  }

//------------------------------------------------------------------------------
// Accessors, in aphabetical order after name
//------------------------------------------------------------------------------

  /**
   * Returns the logical name of this fixed partition description.
   */
  public String getName() {
    return this.name;
  }

  /**
   * Sets the logical name of this fixed partition description.
   */
  private void setName(String str) {
    this.name = str;
  }

  /**
   * Returns the datastores.
   */
  public Integer getDatastores() {
    return this.datastores;
  }

  /**
   * Sets the datastores.
   */
  private void setDatastores(Integer i) {
    this.datastores = i;
  }

  /**
   * Returns the mapping algorithm signature.
   */
  private String getMappingAlgorithm() {
    return this.getMappingClass() + "."
         + this.getMappingMethod() + "()";
  }

  /**
   * Returns the mapping class.
   */
  private String getMappingClass() {
    return this.mappingClass;
  }

  /**
   * Sets the mapping class.
   */
  private void setMappingClass(String str) {
    this.mappingClass = str;
  }

  /**
   * Returns the mapping method.
   */
  private String getMappingMethod() {
    return this.mappingMethod;
  }

  /**
   * Sets the mapping method.
   */
  private void setMappingMethod(String str) {
    this.mappingMethod = str;
  }

  /**
   * Returns the partition names.
   */
  public List<String> getPartitionNames() {
    return this.partitionNames;
  }

  /**
   * Sets the partition names.
   */
  private void setPartitionNames(List<String> strs) {
    this.partitionNames = strs;
  }

  /**
   * Returns the partition buckets.
   */
  public List<Integer> getPartitionBuckets() {
    return this.partitionBuckets;
  }

  /**
   * Sets the partition buckets.
   */
  private void setPartitionBuckets(List<Integer> ints) {
    this.partitionBuckets = ints;
  }

//------------------------------------------------------------------------------
// Attribute creation
//------------------------------------------------------------------------------

  /**
   * Returns the cached list of fixed partition attributes for this description.
   * Invokes the mapping algorithm the first time through.
   */
  protected synchronized List<FixedPartitionAttributes>
        getFixedPartitionAttributes(String regionName, int redundantCopies) {
    if (this.fixedPartitionAttributes == null) {
      Object[] args = {regionName, this, redundantCopies};
      MethExecutorResult result = MethExecutor.execute(this.getMappingClass(),
                                                       this.getMappingMethod(),
                                                       args);
      if (result.getStackTrace() != null){ 
        throw new HydraRuntimeException(result.toString());
      }
      List<FixedPartitionAttributes> fpas =
                (List<FixedPartitionAttributes>)result.getResult();
      if (fpas != null && fpas.size() != 0) {
        this.fixedPartitionAttributes = fpas;
      }
    }
    return this.fixedPartitionAttributes;
  }

//------------------------------------------------------------------------------
// Printing
//------------------------------------------------------------------------------

  public SortedMap toSortedMap() {
    SortedMap map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    map.put(header + "datastores", this.getDatastores());
    map.put(header + "mappingAlgorithm", this.getMappingAlgorithm());
    map.put(header + "partitionNames", this.getPartitionNames());
    map.put(header + "partitionBuckets", this.getPartitionBuckets());
    return map;
  }

//------------------------------------------------------------------------------
// Configuration
//------------------------------------------------------------------------------

  /**
   * Creates fixed partition descriptions from the fixed partition parameters
   * in the test configuration.
   */
  protected static void configure(TestConfig config) {

    ConfigHashtable tab = config.getParameters();

    // create a description for each fixed partition name
    Vector names = tab.vecAt(FixedPartitionPrms.names, new HydraVector());

    for (int i = 0; i < names.size(); i++) {

      String name = (String)names.elementAt(i);

      // create fixed partition description from test configuration parameters
      FixedPartitionDescription fpd = createFixedPartitionDescription(
                                                          name, config, i);

      // save configuration
      config.addFixedPartitionDescription(fpd);
    }
  }

  /**
   * Creates the fixed partition description using test configuration
   * parameters and product defaults.
   */
  private static FixedPartitionDescription createFixedPartitionDescription(
                               String name, TestConfig config, int index) {

    ConfigHashtable tab = config.getParameters();

    FixedPartitionDescription fpd = new FixedPartitionDescription();
    fpd.setName(name);

    // datastores
    {
      Long key = FixedPartitionPrms.datastores;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        String s = BasePrms.nameForKey(key) + " is a required field";
        throw new HydraConfigException(s);
      }
      fpd.setDatastores(i);
    }
    // mappingAlgorithm
    {
      Long key = FixedPartitionPrms.mappingAlgorithm;
      Vector strs = tab.vecAtWild(key, index, null);
      fpd.setMappingClass(getMappingClass(strs, key));
      fpd.setMappingMethod(getMappingMethod(strs, key));
    }
    // partitionNames
    {
      Long key = FixedPartitionPrms.partitionNames;
      Vector strs = tab.vecAtWild(key, index, null);
      if (strs != null) {
        for (Iterator i = strs.iterator(); i.hasNext();) {
          String str = tab.getString(key, i.next());
          if (str == null || str.equalsIgnoreCase(BasePrms.NONE)) {
            i.remove();
          }
        }
      }
      if (strs == null || strs.size() == 0) {
        String s = BasePrms.nameForKey(key) + " is a required field";
        throw new HydraConfigException(s);
      }
      fpd.setPartitionNames(new ArrayList<String>(strs));
    }
    // partitionBuckets
    {
      List<String> fpnames = fpd.getPartitionNames();
      if (fpnames != null) {
        Long key = FixedPartitionPrms.partitionBuckets;
        Vector<String> strs = tab.vecAtWild(key, index, null);
        List<Integer> fpsizes = new ArrayList();
        Integer defaultBuckets = Integer.valueOf(1);
        for (int i = 0; i < fpnames.size(); i++) {
          if (strs == null || strs.size() == 0) {
            fpsizes.add(defaultBuckets);
          } else if (i < strs.size()) {
            String str = tab.getString(key, strs.get(i));
            if (str == null) {
              fpsizes.add(defaultBuckets);
            } else {
              fpsizes.add(getIntegerFor(str, key));
            }
          } else {
            fpsizes.add(fpsizes.get(i-1));
          }
        }
        fpd.setPartitionBuckets(fpsizes);
      }
    }
    return fpd;
  }

//------------------------------------------------------------------------------
// Mapping algorithm configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the mapping class for the given mapping algorithm.
   * @throws HydraConfigException if the algorithm is malformed.
   */
  private static String getMappingClass(Vector strs, Long key) {
    if (strs == null || strs.size() == 0) { // default
      return "hydra.FixedPartitionHelper";
    } else if (strs.size() == 2) {
      return (String)strs.get(0);
    } else {
      String s = BasePrms.nameForKey(key) + " has illegal value: " + strs;
      throw new HydraConfigException(s);
    }
  }

  /**
   * Returns the mapping method for the given mapping algorithm.
   * @throws HydraConfigException if the algorithm is malformed.
   */
  private static String getMappingMethod(Vector strs, Long key) {
    if (strs == null || strs.size() == 0) { // default
      return "assignRoundRobin";
    } else if (strs.size() == 2) {
      return (String)strs.get(1);
    } else {
      String s = BasePrms.nameForKey(key) + " has illegal value: " + strs;
      throw new HydraConfigException(s);
    }
  }
}
