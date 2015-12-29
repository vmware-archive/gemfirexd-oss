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
* A hydra thread group is used to carve out a fixed subset of the available
* client thread pool for a task or set of tasks to use.  Each thread belongs
* to exactly one thread group.
* <p>
* A hydra thread group is specified by a logical name and a list of
* {@link HydraThreadSubgroup}, each of which contains the total number of
* threads in the subgroup, an optional number of VMs from which to choose the
* threads, and an optional set of logical client names from which to choose
* the VMs.
* <p>
* Threads not otherwise assigned to groups are assigned to the default group,
* named {@link HydraThreadGroup#DEFAULT_NAME}.
* <p>
* INITTASKs, CLOSETASKs, and TASKs can optionally specify a list of thread
* groups where they will be scheduled to run.  INITTASKs and CLOSETASKs that
* do not specify any thread groups are scheduled on all thread groups.  TASKs
* that do not specify any thread groups are scheduled on the default thread
* group only.  Thread groups not specified by any INITTASK, CLOSETASK, or TASK
* are ignored and not mapped to any threads.
* <p>
* Tasks using the same thread group can run concurrently with themselves and
* each other, unless the global parameter {@link Prms#serialExecution} is true.
* They are also typically be scheduled onto different client threads from one
* execution to the next.
*
* The timing, relative priority, and termination characteristics of tasks
* within the thread group are handled separately using per-task and test-wide
* attributes.
* <p>
* Thread Subgroup Parameters:
* <p>
* -- totalThreads (int)
*    The total number of threads in one subgroup of the thread group.
* <p>
* -- totalVMs (int) (optional)
*    The "totalThreads" threads for this thread subgroup are scattered as
*    evenly as possible across the specified number of VMs.  Omit this to
*    allow the scheduler to use any number of VMs.
* <p>
* -- clientNames (comma-separated list of strings) (optional)
*    The VMs used for this thread subgroup are chosen from VMs with the
*    corresponding logical client name.  Omit this to allow the scheduler
*    to use any logical client types.
* <p>
* Sample usage scenarios:
* <p>
* -- To allow a set of tasks to compete for 5 fixed threads, set
*      totalThreads=5
*    and assign those tasks to the thread group.
* <p>
* -- To run a given task exclusively in 1 thread in 5 VMs, set
*      totalThreads=5  totalVMs=5
*    and assign only that task to the thread group.
* <p>
* -- To run Task A exclusively on 2 single-threaded VMs, Task B and Task C
*    on 4 VMs with 25 threads each, and Task D exclusively on 1 thread in
*    each of those same 4 VMs, define:
*      hydra.ClientPrms-clientNames  single multi;
*      hydra.ClientPrms-vmQuantities 2      4;
*      hydra.ClientPrms-vmThreads    1      26;
*      THREADGROUP ForTaskA  totalThreads=2   totalVMs=2 clientNames=single;
*      THREADGROUP ForTaskBC totalThreads=100 totalVMs=4 clientNames=multi;
*      THREADGROUP ForTaskD  totalThreads=4   totalVMs=4 clientNames=multi;
*    Then assign each task to the appropriate thread group.
* <p>
* -- To include an exclusive thread running Task D on Task A's VMs, define:
*      hydra.ClientPrms-clientNames  single multi;
*      hydra.ClientPrms-vmQuantities 2      4;
*      hydra.ClientPrms-vmThreads    2      26;
*      THREADGROUP ForTaskA  totalThreads=2   totalVMs=2 clientNames=single;
*      THREADGROUP ForTaskBC totalThreads=100 totalVMs=4 clientNames=multi;
*      THREADGROUP ForTaskD  totalThreads=6   totalVMs=6 clientNames=single,multi;
*    Then assign each task to the appropriate thread group.
*/

public class HydraThreadGroup implements Serializable {

  /** Name of the default thread group */
  public static final String DEFAULT_NAME = "default";

  /** Name of this thread group */
  private String name;

  /** Subgroups that make up this thread group */
  private Vector subgroups = new Vector();

  /** Whether the tasks that use this thread group use custom weights */
  private Boolean weighted = null;

  /** Total number of threads in this thread group */
  private int totalThreads = -1;

  /**
  * Create a hydra thread group with the given name.
  */
  public HydraThreadGroup( String s ) {
    this.name = s;
  }

  /**
  * Gets the name of this thread group.
  */
  public String getName() {
    return this.name;
  }
  /**
  * Gets whether the tasks that use this thread group use custom weights.
  */
  public Boolean usesCustomWeights() {
    return this.weighted;
  }
  /**
  * Sets whether the tasks that use this thread group use custom weights.
  */
  public void usesCustomWeights( Boolean b ) {
    this.weighted = b;
  }
  /**
  * Gets the subgroups that make up this thread group.
  */
  public Vector getSubgroups() {
    return this.subgroups;
  }
  /**
  * Gets the i'th subgroup that makes up this thread group.
  */
  public HydraThreadSubgroup getSubgroup( int i ) {
    return (HydraThreadSubgroup) this.subgroups.elementAt(i);
  }
  /**
  * Adds a subgroup to this thread group.
  */
  public void addSubgroup( HydraThreadSubgroup subgroup ) {
    this.subgroups.add( subgroup );
  }
  /**
  * Gets the total number of threads in this thread group (lazily).
  */
  public int getTotalThreads() {
    if ( this.totalThreads == -1 ) {
      this.totalThreads = 0;
      for ( Iterator i = getSubgroups().iterator(); i.hasNext(); ) {
        HydraThreadSubgroup subgroup = (HydraThreadSubgroup) i.next();
        this.totalThreads += subgroup.getTotalThreads();
      }
    }
    return this.totalThreads;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    PRINTING                                                          ////
  //////////////////////////////////////////////////////////////////////////////

  public SortedMap toSortedMap() {
    StringBuffer buf = new StringBuffer();
    for ( Iterator i = this.subgroups.iterator(); i.hasNext(); ) {
      HydraThreadSubgroup sg = (HydraThreadSubgroup) i.next();
      buf.append( sg.toString() );
    }
    SortedMap map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName();
    map.put( header, buf.toString() );
    return map;
  }
  public String toString() {
    StringBuffer buf = new StringBuffer();
    SortedMap map = this.toSortedMap();
    for ( Iterator i = map.keySet().iterator(); i.hasNext(); ) {
      String key = (String) i.next();
      Object val = map.get( key );
      buf.append( key + "=" + val + "\n" );
    }
    return buf.toString();
  }
}
