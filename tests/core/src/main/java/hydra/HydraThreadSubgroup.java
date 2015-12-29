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

import java.io.*;
import java.util.*;

/** 
*
* A subgroup of a {@link HydraThreadGroup}.  A subgroup contains the total
* number of threads in the subgroup, an optional number of VMs from which
* to choose the threads, and an optional set of logical client names from
* which to choose the VMs.
*
*/

public class HydraThreadSubgroup implements Serializable {

  /** Name of the thread group this subgroup belongs to */
  private String name;

  /** Number of threads in this thread group */
  private int totalThreads = -1;

  /** How many VMs to scatter across */
  private int totalVMs = -1;

  /** Logical clients to choose VMs from */
  private Vector clientNames = null;

  /**
  * Create a hydra thread subgroup.
  */
  public HydraThreadSubgroup( String name ) {
    this.name = name;
  }

  /**
  * Gets the name of the thread group this subgroup belongs to.
  */
  public String getName() {
    return this.name;
  }
  /**
  * Sets the number of threads in this subgroup.
  */
  public void setTotalThreads( int n ) {
    this.totalThreads = n;
  }
  /**
  * Gets the number of threads in this subgroup.
  */
  public int getTotalThreads() {
    return this.totalThreads;
  }
  /**
  * Sets the number of vms the threads in this subgroup will come from.
  */
  public void setTotalVMs( int n ) {
    this.totalVMs = n;
  }
  /**
  * Gets the number of vms the threads in this subgroup will come from.
  */
  public int getTotalVMs() {
    return this.totalVMs;
  }
  /**
  * Sets the logical client names the vms used in this subgroup will come from.
  */
  public void setClientNames( Vector names ) {
    this.clientNames = names;
  }
  /**
  * Gets the logical client names the vms in this subgroup will come from.
  */
  public Vector getClientNames() {
    return this.clientNames;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    PRINTING                                                          ////
  //////////////////////////////////////////////////////////////////////////////

  public String toString() {
    StringBuffer buf = new StringBuffer();
    buf.append( "THREADGROUP " + this.getName() );
    buf.append( " totalThreads:" + this.getTotalThreads() );
    buf.append( " totalVMs:" + this.getTotalVMs() );
    buf.append( " clientNames:" + this.getClientNames() );
    return buf.toString();
  }
}
