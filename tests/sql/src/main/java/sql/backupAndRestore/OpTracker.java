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
package sql.backupAndRestore;

import java.io.Serializable;

import hydra.GsRandom;
import hydra.Log;
import hydra.TestConfig;

/**
 * The OpTracker class is used to 'track' what keys are used for operations of type update or delete.
 *  The keys are distributed in sequential order for each 'thread' (opTrackerKey) so that no other thread can hand out
 *  the same key. Each thread that uses this has a different sequence of numbers, thus only one thread will
 *  track a row with a given primary key, so each delete or update should be successful.
 *  Example(s)
 *  Thread: 1 2 3 4 1 2 3 4 1  2  3  4  1  2  3  4  1  2  3  4
 *     Key: 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20
 * -- OR --
 *  Thread |----Keys----|
 *     1    1 5  9 13 17
 *     2    2 6 10 14 18
 *     3    3 7 11 15 19
 *     4    4 8 12 16 20
 *
 * The single, 2 argument constructor instantiates an OpTracker for a number of threads to a maximum end key value.
 * When constructed, the key starting points are randomly chosen for each thread to ensure the sequence numbers are
 * scattered across the range of sequence number for all threads.
 *
 * Use the rebaseAt(int) method to increase the maximum end key value.
 *
 * @author mpriest
 * @see ?
 * @since 1.4
 */
public class OpTracker implements Serializable {
  public static final int OP_TYPE_INSERT = -1;// When not tracked, will be an insert
  public static final int OP_TYPE_UPDATE = 1; // for ODD  numbered thread IDs
  public static final int OP_TYPE_DELETE = 2; // for EVEN numbered thread IDs
  private int opTrackerKey;         // Unique ID for the OpTracker (Thread)
  private int opType = -1;          // The type of operation that this tracker is tracking (update or delete)
  private int startKey = -1;        // The Starting Key for the OpTracker
  private int endKey = -1;          // The last Key being tracked by the OpTracker
  private int endValKey = -1;       // The last Key being tracked by the OpTracker for validation

  private int nbrOfThreads = 0;     // the total number of OpTrackers used by the test
  private int maxEndKey = 0;        // the maximum Key to be returned by any OpTracker
  private int nbrAvailKeys = 0;     // the total number of Keys that can be returned by this OpTracker
  private int myMaxKey = 0;         // the highest key that can be returned by this instance of OpTracker
  private int returnedCount = 0;    // the number of keys that have been returned by nextKeyForOp()
  private int returnedValCount = 0; // the number of keys that have been returned by nextKeyForValidation()

  /**
   * Constructor
   *
   * @param maxEndKeyArg an int used to set maxEndKey which is the maximum key that can be returned by any instance of
   *        this class. All instances should be created with the same value.
   * @param nbrOfThreadsArg an int used to set nbrAvailKeys which is the total number of OpTracker instances that will be
   *        used in the test. For example, if multiple threads are using this to coordinate a sequence of primary keys,
   *        this would be the number of threads.
   */
  public OpTracker(int maxEndKeyArg, int nbrOfThreadsArg) {
    opTrackerKey = new Long(BackupAndRestoreBB.getBB().getSharedCounters().incrementAndRead(BackupAndRestoreBB.opTrackerCnt)).intValue();
    // Check the 1st bit of the opTrackerKey, if it is on, then it must be an odd number - set the proper opType
    opType = (opTrackerKey & 1) == 1 ? OP_TYPE_UPDATE : OP_TYPE_DELETE;
    maxEndKey = maxEndKeyArg;
    nbrOfThreads = nbrOfThreadsArg;
    returnedCount = 0;

    // Set the number of available keys
    calcNbrAvailKeys();

    // Set myMaxKey
    calcMyMaxKey();

    // Set a random starting point
    GsRandom rand = TestConfig.tab().getRandGen();
    int reducedNbrAvailKeys = nbrAvailKeys / 2;  // We reduce the number of keys available so we don't start too high
    startKey = (rand.nextInt(0, reducedNbrAvailKeys - 1) * nbrOfThreads) + opTrackerKey;
    Log.getLogWriter().info("OpTracker-Created a new OpTracker:" + this.toString());
  }

  /**
   * Use this method when more records are added and you would like to increase the maximum capacity of the OpTrackers
   *
   * @param maxEndKeyArg an int used to set maxEndKey which is the maximum key that can be returned by any instance of
   *        this class. All instances should call this with the same value.
   */
  public void rebaseAt(int maxEndKeyArg) {
    Log.getLogWriter().fine("OpTracker.rebaseAt-opTrackerKey=" + opTrackerKey +
                            ", maxEndKey=" + maxEndKey +
                            ", maxEndKeyArg=" + maxEndKeyArg);
    maxEndKey = maxEndKeyArg;

    // Set the number of available keys
    calcNbrAvailKeys();

    // calculate the new myMaxKey
    calcMyMaxKey();
  }

  /**
   * Use this to calculate the Number of Available Keys (nbrAvailKeys) field which is the total number of Keys that
   *  can be returned by this OpTracker. It's called from the constructor and rebaseAt(int).
   */
  private void calcNbrAvailKeys() {
    Log.getLogWriter().fine("OpTracker.calcNbrAvailKeys-opTrackerKey=" + opTrackerKey +
                            ", nbrAvailKeys(Before)=" + nbrAvailKeys);
    // Calculate the number of available keys
    int quotient = maxEndKey / nbrOfThreads;
    int remainder = maxEndKey % nbrOfThreads;
    nbrAvailKeys = quotient;
    if (opTrackerKey <= remainder) {
      nbrAvailKeys++;
    }
    Log.getLogWriter().fine("OpTracker.calcNbrAvailKeys-opTrackerKey=" + opTrackerKey +
                            ", nbrAvailKeys(After)=" + nbrAvailKeys);
  }

  /**
   * Use this to calculate the my Max Key (myMaxKey) field which is the highest key that can be returned by this
   *  instance of OpTracker. It's called from the constructor and rebaseAt(int).
   */
  private void calcMyMaxKey() {
    // Calculate myMaxKey
    myMaxKey = ((nbrAvailKeys - 1) * nbrOfThreads) + opTrackerKey;
    if (myMaxKey > maxEndKey) {
      myMaxKey -= nbrOfThreads;
      Log.getLogWriter().info("OpTracker.calcMyMaxKey-myMaxKey '" + myMaxKey +
                              "' is > maxEndKey '" + maxEndKey +
                              "', reducing it by the number of threads (" + nbrOfThreads + ")");
    }
  }

  /**
   * Use this to get the next available key in this OpTracker's sequence of keys. Starts at the startKey. Used when
   *  performing operations.
   *
   * @return An int for the next available key in this OpTracker's sequence. Returns a -1 if all available keys have
   *         been returned. It's up to the caller to determine what to do with this info.
   */
  public int nextKeyForOp() {
    if (endKey == -1) {  // This must be the first call to nextKeyForOp() for a newly created OpTracker
      endKey = startKey;
    } else {
      if (returnedCount == nbrAvailKeys) {  // We already returned the nbrAvailKeys, don't perform the Op
        return -1;
      }
      // Find the next key to be used for an Op
      int nextKey = endKey + nbrOfThreads;
      if (nextKey > myMaxKey) {  // We don't have enough keys available to perform the Op, yet
        return -1;
      } else {
        endKey = nextKey;  // Set the key to return
      }
    }
    returnedCount++;
    Log.getLogWriter().fine("OpTracker.nextKeyForOp-endKey=" + endKey +
                            ", returnedCount=" + returnedCount);
    return endKey;
  }

  /**
   * Use this to get the next available key in this OpTracker's sequence of keys. Starts at the beginning of the
   *  sequence numbers. Used when performing validation.
   *
   * @return An int for the next available key in this OpTracker's sequence. Returns a -1 if all available keys have
   *         been returned. It's up to the caller to determine what to do with this info.
   */
  public int nextKeyForValidation() {
    if (endValKey == -1) {  // This must be the first call to nextKeyForValidation(), let's start at the beginning
      endValKey = opTrackerKey;
    } else {
      if (returnedValCount == nbrAvailKeys) {  // We've already returned the nbrAvailKeys, don't advance
        return -1;
      }
      // Find the next key to be used for validation
      int nextKey = endValKey + nbrOfThreads;
      if (nextKey > myMaxKey) {  // We've already returned all the keys, don't advance
        return -1;
      } else {
        endValKey = nextKey;  // Set the key to return
      }
    }
    returnedValCount++;
    Log.getLogWriter().info("OpTracker.nextKeyForValidation-endValKey=" + endValKey +
                            ", returnedValCount=" + returnedValCount);
    return endValKey;
  }

  /**
   * Used to check if this OpTracker is tracking an operation (update or delete) for a key value.
   *
   * @param key An int value for the key you'd like to check if it's being tracked
   * @return true if the passed in key is being tracked by this OpTracker, otherwise false
   */
  public boolean isKeyTracked(int key) {
    boolean found = false;

    if (endKey == -1) { // can't be here because no keys have been returned yet
      found = false;
    } else if (key == startKey || key == endKey) {  // well, obviously its right here!
      found = true;
    } else if (key > startKey && key < endKey) { // OK, it could be here...
      found = ((key - startKey) % nbrOfThreads) == 0;
    }

    return found;
  }

  /**
   * Used to check the OpTracker's maxEndKey which is the maximum key that can be returned by any OpTracker
   *
   * @return An int value indicating the maximum key value that can be returned by any OpTracker
   */
  public int getMaxEndKey() {
    return maxEndKey;
  }

  /**
   * Used to check the OpTracker's operation type (update or delete)
   *
   * @return An int value for the OpTracker's opType
   */
  public int getOpType() {
    return opType;
  }

  /**
   * Used to check if this OpTracker is tracking deletes
   *
   * @return true if this OpTracker is tracking deletes
   */
  public boolean isDeleteTracker() {
    return opType == OP_TYPE_DELETE;
  }

  /**
   * Used to check if this OpTracker is tracking updates
   *
   * @return true if this OpTracker is tracking updates
   */
  public boolean isUpdateTracker() {
    return opType == OP_TYPE_UPDATE;
  }

  /**
   * Convenient toString method to report the status of the OpTracker
   *
   * @return A String containing the values of the OpTrackers internal fields
   */
  public String toString() {
    return "OpTracker (opTrackerKey=" + opTrackerKey +
           ", opType=" + opType +
           ", nbrAvailKeys=" + nbrAvailKeys +
           ", startKey=" + startKey +
           ", endKey=" + endKey +
           ", endValKey=" + endValKey +
           ", myMaxKey=" + myMaxKey +
           ", maxEndKey=" + maxEndKey +
           ", returnedCount=" + returnedCount +
           ", returnedValCount=" + returnedValCount +
           ", nbrOfThreads=" + nbrOfThreads +
           ")";
  }
}
