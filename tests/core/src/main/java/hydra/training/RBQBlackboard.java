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
package hydra.training;

import hydra.blackboard.Blackboard;
import hydra.blackboard.SharedMap;
import hydra.blackboard.SharedCounters;

/**
 * A Hydra <code>Blackboard</code> that maintains statistics about a
 * {@link RemoteBlockingQueue}.  The blackboard's shared counters are
 * used to store the maximum and minimum duration of certain queue
 * operations.  The blackboard's shared map stores the capacity of the
 * queue and how many <code>poll</code> operations were performed by
 * each client thread (this gives us an idea of the "fairness" of the
 * poll operation among multiple threads).
 *
 * @author Lynn Gallinat
 * @author David Whitlock
 * @since 4.0
 */
public class RBQBlackboard extends Blackboard {
   
  // Blackboard creation variables

  /** The name of the blackboard */
  static final String BB_NAME = "RBQBlackboard";

  /** The type of the blackboard */
  static final String BB_TYPE = "RMI";

  // Define shared counters here

  /** Minimum duration (in nanoseconds) of an offer operation */
  public static int MinOfferTime;

  /** Maximum duration (in nanoseconds) of an offer operation */
  public static int MaxOfferTime;

  /** Minimum duration (in nanoseconds) of a poll operation */
  public static int MinPollTime;

  /** Maximum duration (in nanoseconds) of a poll operation */
  public static int MaxPollTime;

  /** Total number of offer operations performed */
  public static int NumOffers;

  /** Total number of poll operations performed */
  public static int NumPolls;

  /** An example counter that shows how a counter can be given an
   * inital value here; this counter is not used in the example, but
   * is printed out in the start task to show its initial value */
  public static int MyCounter = 11;

  /** A key used for the blackboard's shared map that stores the
   * capacity for the queue used in this test. */
  static final String CapacityKey = "capacity";

  /** The singleton RBQBlackboard */
  public static RBQBlackboard bbInstance = null;

  ///////////////////////  Static Methods  ///////////////////////

  /**
   * Convenience method to get an instance of RBQBlackboard (for
   * efficiency reasons).  This is not required; a test could get a new
   * instance of RBQBlackboard each time it needed access to the
   * blackboard, and each instance would access the same shared counters
   * and shared map with the previous values intact.
   */
  public static synchronized RBQBlackboard getBB() {
    if (bbInstance == null)
      bbInstance = new RBQBlackboard(BB_NAME, BB_TYPE);
    return bbInstance;
  }
   
  //////////////////////  Constructors  //////////////////////

  /**
   * Required zero-arg constructor for remote method invocations.
   */
  public RBQBlackboard() {

  }
   
  /**
   * Constructor to create a blackboard using the specified name and
   * transport type.  This constructor will register this blackboard
   * with the Hydra runtime (master controller).
   */
  private RBQBlackboard(String name, String type) {
    super(name, type, RBQBlackboard.class);
  }

  //////////////////////  Instance Methods  //////////////////////

  /**
   * Reads the {@linkplain RBQPrms#getQueueCapacity() capacity} of the
   * queue to use in this test and stores it in this blackboard.  This
   * ensures that all test VMs agree on the capacity even if it is
   * configured with a <code>ONEOF</code>.
   */
  void initializeCapacity() {
    int capacity = RBQPrms.getQueueCapacity();
    SharedMap map = this.getSharedMap();
    map.put(RBQBlackboard.CapacityKey, new Integer(capacity));
  }
   
  /**
   * Returns the capacity of the queue as store in the blackboard.
   */
  public int getQueueCapacity() {
    SharedMap map = this.getSharedMap();
    return ((Integer)(map.get(RBQBlackboard.CapacityKey))).intValue();
  }

  /**
   * Returns the minimum duration (in nanoseconds) of an offer
   * operation.
   */
  public long getMinOfferTime() {
    return this.getSharedCounters().read(RBQBlackboard.MinOfferTime);
  }

  /**
   * Returns the maximum duration (in nanoseconds) of an offer
   * operation.
   */
  public long getMaxOfferTime() {
    return this.getSharedCounters().read(RBQBlackboard.MaxOfferTime);
  }

  /**
   * Returns the total number of offer operations performed.
   */
  public long getNumOffers() {
    return this.getSharedCounters().read(RBQBlackboard.NumOffers);
  }

  /**
   * Returns the minimum duration (in nanoseconds) of an poll
   * operation.
   */
  public long getMinPollTime() {
    return this.getSharedCounters().read(RBQBlackboard.MinPollTime);
  }

  /**
   * Returns the maximum duration (in nanoseconds) of an poll
   * operation.
   */
  public long getMaxPollTime() {
    return this.getSharedCounters().read(RBQBlackboard.MaxPollTime);
  }

  /**
   * Returns the total number of poll operations performed.
   */
  public long getNumPolls() {
    return this.getSharedCounters().read(RBQBlackboard.NumPolls);
  }

  /**
   * Returns the value of the "MyCounter" shared counter
   */
  public long getMyCounter() {
    return this.getSharedCounters().read(RBQBlackboard.MyCounter);
  }

  /**
   * Notes that an <code>offer</code> operation took place
   *
   * @param duration
   *        The duration of the operation in nanoseconds
   */
  void noteOffer(long duration) {
    SharedCounters sc = this.getSharedCounters();
    sc.setIfLarger(RBQBlackboard.MaxOfferTime, duration);
    sc.setIfSmaller(RBQBlackboard.MinOfferTime, duration);
    sc.increment(RBQBlackboard.NumOffers);
  }
  
  /**
   * Notes that an <code>poll</code> operation took place
   *
   * @param duration
   *        The duration of the operation in nanoseconds
   */
  void notePoll(long duration) {
    SharedCounters sc = this.getSharedCounters();
    sc.setIfLarger(RBQBlackboard.MaxPollTime, duration);
    sc.setIfSmaller(RBQBlackboard.MinPollTime, duration);
    sc.increment(RBQBlackboard.NumPolls);

    // Increment the number of polls performed by this client thread
    String threadName = "Polls for " +
      Thread.currentThread().getName();
    SharedMap map = this.getSharedMap();
    Long threadCount = (Long) map.get(threadName);
    if (threadCount == null) {
      threadCount = new Long(0L);
    }
    map.put(threadName, (new Long(threadCount.longValue() + 1L)));
  }

}


