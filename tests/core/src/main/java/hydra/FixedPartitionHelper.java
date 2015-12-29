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

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.FixedPartitionAttributes;
import hydra.blackboard.SharedLock;
import hydra.blackboard.SharedMap;
import java.util.*;

/**
 * Helps datastores use {@link FixedPartitionDescription}.  Methods are
 * thread-safe.
 */
public class FixedPartitionHelper {

//------------------------------------------------------------------------------
// FixedPartitionAttributes mappings
//------------------------------------------------------------------------------

  /**
   * Returns a list of fixed partition attributes for this JVM for the region
   * with the specified name, or, if that is null, for the region configured
   * in the given region description, or, if that is null, for the default
   * region, {@link RegionPrms#DEFAULT_REGION_NAME}.
   * <p>
   * Primary and secondary partitions are assigned round robin, but in a way
   * that prevents the same partition from being mapped to the same node twice.
   * It constructs a list of FixedPartitionAttributes that includes the
   * primaries and N copies of the secondaries, where N is the number of
   * redundant copies.  It then deals them out to datastores like cards.
   * If a datastore has already been assigned a partition of the same name
   * as the current "card", that card is skipped until an appropriate card is
   * found.  The next datastore then starts with the skipped card.  An
   * exception is thrown if it is impossible to legally deal all cards.
   * <p>
   * For example, if there are 3 datastores and 4 fixed partitions with
   * redundancy 1, the set of assignments would look like this (bucket counts
   * are omitted here for brevity):
   *
   *    datastore 1: P1 primary P4 primary   P3 secondary
   *    datastore 2: P2 primary P1 secondary P4 secondary
   *    datastore 3: P3 primary P2 secondary
   * <p>
   * Or, for 2 datastores and 4 fixed partitions with redundancy 1, the set of
   * assignments would look like this:
   *
   *    datastore 1: P1 primary P3 primary P2 secondary P4 secondary
   *    datastore 2: P2 primary P4 primary P1 secondary P3 secondary
   * <p>
   * Or, for 2 datastores and 2 fixed partitions with redundancy 2, an
   * exception would be thrown since it is impossible to map all redundant
   * copies to nodes that do not already contain either a primary or secondary
   * for the same partition.
   * <p>
   * This is a suitable {@link FixedPartitionPrms#mappingAlgorithm} for most
   * tests, tests that create the same region in multiple WAN sites.  However,
   * if the test creates multiple regions using the same logical region
   * configuration, then it must make sure that the actual region names are
   * passed in by invoking appropriate methods in {@link RegionHelper}.
   *
   * @throws HydraRuntimeException if it is impossible to map the partitions
   *         without mapping the same partition to the same datastore.
   */
  public static synchronized List<FixedPartitionAttributes> assignRoundRobin(
      String regionName, FixedPartitionDescription fpd, int redundantCopies) {

    List<FixedPartitionAttributes> choice = null;

    // look up the blackboard for the given region in this distributed system
    String dsName = DistributedSystemHelper.getDistributedSystemName();
    String bbName = dsName + "." + regionName;
    FixedPartitionBlackboard bb = FixedPartitionBlackboard.getInstance(bbName);

    // look up the choice for this jvm
    int vmid = RemoteTestModule.getMyVmid();
    choice = (List)bb.getSharedMap().get(vmid);

    if (choice == null) { // first time through for this jvm
      try {
        bb.getSharedLock().lock();
        // look up the choices, lazily generating them
        List<List<FixedPartitionAttributes>> choices =
                        (List)bb.getSharedMap().get("choices");
        if (choices == null) {
          choices = generateRoundRobinChoices(fpd, redundantCopies);
          bb.getSharedMap().put("choices", choices);
          if (Log.getLogWriter().fineEnabled()) {
            Log.getLogWriter().fine("assignRoundRobin: put in bb=" + bbName
                                   + " choices=" + choices);
          }
        }

        // choose an assignment for this jvm
        if (choices.size() > 0) {
          choice = choices.remove(0);
          bb.getSharedMap().put("choices", choices);
        } else {
          String s = "Too many JVMs called assignRoundRobin for "
                   + regionName + " " + fpd;
          throw new HydraRuntimeException(s);
        }
      } finally {
        bb.getSharedLock().unlock();
      }

      // update the blackboard
      bb.getSharedMap().put(vmid, choice);
      if (Log.getLogWriter().fineEnabled()) {
        Log.getLogWriter().fine("assignRoundRobin: put in bb=" + bbName
                               + " vmid=" + vmid + " choice=" + choice);
      }
    }
    return choice;
  }

  private static List<List<FixedPartitionAttributes>> generateRoundRobinChoices(
          FixedPartitionDescription fpd, int redundantCopies) {

    // generate all of the primary and secondary fpas
    List<String> partitionNames = fpd.getPartitionNames();
    List<Integer> partitionBuckets = fpd.getPartitionBuckets();
    List<FixedPartitionAttributes> fpas = new ArrayList();
    for (int i = 0; i < partitionNames.size(); i++) {
      FixedPartitionAttributes fpa =
        FixedPartitionAttributes.createFixedPartition(
          partitionNames.get(i), true, partitionBuckets.get(i).intValue());
      fpas.add(fpa);
    }
    for (int i = 0; i < redundantCopies; i++) {
      for (int j = 0; j < partitionNames.size(); j++) {
        FixedPartitionAttributes fpa =
          FixedPartitionAttributes.createFixedPartition(
            partitionNames.get(j), false, partitionBuckets.get(j).intValue());
        fpas.add(fpa);
      }
    }
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("generateRoundRobinChoices: fpas=" + fpas);
    }

    // generate the list of lists
    List<List<FixedPartitionAttributes>> ll = new ArrayList();
    for (int i = 0; i < fpd.getDatastores(); i++) {
      List<FixedPartitionAttributes> l = new ArrayList();
      ll.add(l);
    }

    // make the round robin assignments
    boolean assigned = true;
    while (fpas.size() > 0 && assigned) {
      assigned = false;
      for (int i = 0; i < ll.size(); i++) {
        // assign the next legal fpa to datastore i
        List<FixedPartitionAttributes> l = ll.get(i);
        for (FixedPartitionAttributes fpa : fpas) {
          if (!containsPartitionName(l, fpas.get(0))) {
            l.add(fpas.remove(0));
            assigned = true;
            break;
          }
        }
      }
    }
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("generateRoundRobinChoices: choices for all datastores=" + ll);
    }
    if (fpas.size() > 0) {
      String s = "Unable to assign these FPAs: " + fpas + " to " + ll;
      throw new HydraRuntimeException(s);
    }
    return ll;
  }

  private static boolean containsPartitionName(
        List<FixedPartitionAttributes> fpas, FixedPartitionAttributes cfpa) {
    for (FixedPartitionAttributes fpa : fpas) {
      if (fpa.getPartitionName().equals(cfpa.getPartitionName())) {
        return true;
      }
    }
    return false;
  }
}
