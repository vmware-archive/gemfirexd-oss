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
package target;

import hydra.*;



public class TargetPrms extends BasePrms{

  /** (int) Number of entries in the map used for performing a putAll on a region
   *  Entries comprise of an integer and a ByteArray of a specified size;
   */
  public static Long numMapEntries;
  
  /** (int) Size of the ByteArrays associated with map entry 
   * 
   */
  public static Long byteArraySize;
  
  /** (int) The amount of delay to be set in the PutAllCacheWrite , to introduce delay in-between puts
   *   (in milliseconds)
   */
  public static Long delayInBetweenPutsMS;
  
  /** (int) The number of entries after which the cache closed is called
   * 
   */
  public static Long entriesThresholdPercent;
  
  static {
    BasePrms.setValues(TargetPrms.class);
 }
  
}
