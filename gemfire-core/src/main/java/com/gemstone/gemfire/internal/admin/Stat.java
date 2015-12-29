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

package com.gemstone.gemfire.internal.admin;

/**
 * Interface to represent a single statistic of a <code>StatResource</code>
 *
 * @author Darrel Schneider
 * @author Kirk Lund
 */
public interface Stat extends GfObject {
    
  /**
   * @return the value of this stat as a <code>java.lang.Number</code> 
   */
  public Number getValue();
  
  /**
   * @return a display string for the unit of measurement (if any) this stat represents
   */
  public String getUnits();
  
  /**
   * @return true if this stat represents a numeric value which always increases
   */
  public boolean isCounter();
  
}

