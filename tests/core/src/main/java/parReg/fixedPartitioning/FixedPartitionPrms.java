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
package parReg.fixedPartitioning;

import getInitialImage.InitImagePrms;
import hydra.BasePrms;

public class FixedPartitionPrms extends BasePrms {

  /** (boolean) True if the region should be created with a cache writer, false otherwise
   */
  public static Long useFixedPartitioning;

  /**
   * (int) RedundantCopies used in the test
   */
  public static Long redundantCopies;

  /**
   * (int) RedundantCopies used in the test
   */
  public static Long totalNumBuckets;
  
  /**
   * (int) Number of regions used in the test
   */
  public static Long numRegions;
  
  /**
   * (boolean) Whether the test should use subregion FPR (only one though)
   */
  public static Long useSubRegion;

  /**
   * (String) Type of the test. Use the following 
   * singleFPR - Test using single FPR. 
   * colocatedFPR - Test using colocatedFPRs 
   * multiFPR - Test with multiple
   * FPRs but not colocated
   */
  public static Long testType;
  
  /**
   * (int) Maximum number of primary partitions used in test
   */
  public static Long maxPrimaryPartitionsOnMember;
  
  /**
   * (Boolean) Whether test uses eviction in the test.
   */
  public static Long useEviction;
  
  /**
   * (Boolean) Whether test uses partition resolver as partition attribute.
   */
  public static Long setResolverAsAttribute;
  
  /**
   * (String) Type of the eviction action used in the test.
   * overFlowToDisk - For overflow to disk
   * localDestroy - For local destroy
   * 
   */
  public static Long evictionAction;
  
  /**
   * (Boolean) Whether test uses persistent partitioning.
   */
  public static Long isWithPRPersistence;

  // ================================================================================
  static {
    BasePrms.setValues(FixedPartitionPrms.class);
  }

}
