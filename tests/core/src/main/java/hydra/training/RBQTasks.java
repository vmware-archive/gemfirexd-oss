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

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.internal.NanoTimer;
import java.util.concurrent.TimeUnit;
//import hydra.BasePrms;
import hydra.HydraConfigException;
import hydra.HydraRuntimeException;
import hydra.Log;
import hydra.TestConfig;
import hydra.RmiRegistryHelper;
//import hydra.blackboard.Blackboard;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.*;
import util.TestException;

/**
 * Hydra tasks that test the functionality and scalability of the
 * {@link RemoteBlockingQueue}.
 *
 * @author David Whitlock
 * @since 4.0
 */
public class RBQTasks {

  /** The well-known name of the queue used for this test */
  private static final String QUEUE_NAME = "queue";

  ////////////////////////  Static Methods  ////////////////////////

  /**
   * A START task that reads the capacity for the
   * <code>RemoteBlockingQueue</code> and writes the chosen capacity
   * to the blackboard so that each VM the creates the
   * <code>RemoteBlockingQueue</code> can create it with the same
   * capacity
   */
  public static void initialize() {
    if (!RBQPrms.useBlackboard()) {
      String s = "This test is not configured to use the blackboard";
      throw new HydraConfigException(s);
    }

    RBQBlackboard bb = RBQBlackboard.getBB();
    bb.initializeCapacity();

    // Show the initial values of the blackboard shared counters
    LogWriter log = Log.getLogWriter();
    log.info("Initial value of RBQBlackboard.MinOfferTime: " + 
             bb.getMinOfferTime());
    log.info("Initial value of RBQBlackboard.MaxOfferTime: " + 
             bb.getMaxOfferTime());
    log.info("Initial value of RBQBlackboard.MinPollTime: " + 
             bb.getMinPollTime());
    log.info("Initial value of RBQBlackboard.MaxPollTime: " + 
             bb.getMaxPollTime());
    log.info("Initial value of RBQBlackboard.NumOffers: " + 
             bb.getNumOffers());
    log.info("Initial value of RBQBlackboard.NumPolls: " + 
             bb.getNumPolls());
    log.info("Initial value of RBQBlackboard.MyCounter: " + 
             bb.getMyCounter());
  }

  /**
   * An INIT task that creates a new <code>RemoteBlockingQueue</code>
   * and binds it into Master Controller's RMI Registry.
   */
  public static void bindRBQ() throws RemoteException {
    int capacity;
    if (RBQPrms.useBlackboard()) {
      capacity = RBQBlackboard.getBB().getQueueCapacity();

    } else {
      capacity = RBQPrms.getQueueCapacity();
    }

    Log.getLogWriter().info("Creating RemoteBlockingQueueImpl with capacity " + capacity);
    RemoteBlockingQueue rbq = new RemoteBlockingQueueImpl(capacity);
    try {
      RmiRegistryHelper.bindInMaster(QUEUE_NAME, rbq);

    } catch (HydraRuntimeException ex) {
      if (ex.getCause() instanceof java.rmi.AlreadyBoundException) {
        return;

      } else {
        throw ex;
      }
    }
  }

  /**
   * Returns the <code>RemoteBlockingQueue</code> used by this test.
   */
  private static RemoteBlockingQueue getQueue() {
    return (RemoteBlockingQueue)RmiRegistryHelper.lookupInMaster(QUEUE_NAME);
  }

  /**
   * A hydra TASK that populates the remote queue
   */
  public static void populate()
    throws RemoteException, InterruptedException {

    RemoteBlockingQueue queue = getQueue();
    NanoTimer timer = new NanoTimer();
    for (int i = 0; i < 500; i++) {
      QueueElement element = new QueueElement();
      if (RBQPrms.debug()) {
        Log.getLogWriter().info("Offering " + element);
      }

      timer.reset();
      queue.offer(element, Integer.MAX_VALUE, TimeUnit.SECONDS);
      long duration = timer.reset();

      if (RBQPrms.useBlackboard()) {
        RBQBlackboard.getBB().noteOffer(duration);
      }
    }
  }

  /** Map used by <code>consume</code> task to keep track of the
   * sequence number of the <code>QueueElement</code>s removed from
   * the queue. */
  private static final Map sequenceNumbers = new HashMap();

  /**
   * A hydra TASK that consumes elements from the queue
   */
  public static void consume()
    throws RemoteException, InterruptedException {

    RemoteBlockingQueue queue = getQueue();
    NanoTimer timer = new NanoTimer();
    while (true) {
      // We have to synchronize on a static field because there might
      // be a race between two consumer threads removing elements in
      // FIFO order, but verifying them in non-FIFO order.  Lame.
      synchronized (sequenceNumbers) {
        timer.reset();
        QueueElement element =
          (QueueElement) queue.poll(30, TimeUnit.SECONDS);
        long duration = timer.reset();
        if (RBQPrms.debug()) {
          Log.getLogWriter().info("Removed " + element);
        }
        
        if (element == null) {
          return;
        
        } else {
          if (RBQPrms.useBlackboard()) {
            RBQBlackboard.getBB().notePoll(duration);
          }

          int[] prevSeqNum;
          String threadName = element.getThreadName();
          prevSeqNum = (int[]) sequenceNumbers.get(threadName);
          if (prevSeqNum == null) {
            prevSeqNum = new int[] { -1 };
            sequenceNumbers.put(threadName, prevSeqNum);
          }
              
          int seqNum = element.getSequenceNumber();
          synchronized (prevSeqNum) { // Don't need this
            if (seqNum <= prevSeqNum[0]) {
              String s = "Element " + seqNum + " from client thread " +
                threadName + " should not come after " + prevSeqNum[0];
              throw new TestException(s);

            } else {
              prevSeqNum[0] = seqNum;
            }
          }
        }
      }
    }
  }

  /**
   * A hydra ENDTASK that validates values in the blackboard.
   *
   * @throws TestException
   *         If the number of poll operations does not equal the
   *         number of offer operations.
   */
  public static void validate() throws RemoteException {
    RBQBlackboard bb = RBQBlackboard.getBB();
    bb.print();   // shows the contents of shared counters and the shared map

    // read and log the min and max times
    long minOfferTime = bb.getMinOfferTime();
    long maxOfferTime = bb.getMaxOfferTime();
    long minPollTime = bb.getMinPollTime();
    long maxPollTime = bb.getMaxPollTime();

    LogWriter log = Log.getLogWriter();
    log.info("Minimum time for offer: " + minOfferTime + " nanos");
    log.info("Maximum time for offer: " + maxOfferTime + " nanos");
    log.info("Minimum time for poll : " + minPollTime + " nanos");
    log.info("Maximum time for poll : " + maxPollTime + " nanos");

    // check that all elements put into the queue were actually removed
    long numPolls = bb.getNumPolls();
    long numOffers = bb.getNumOffers();

    if (numPolls != numOffers) {
      String s = "Expected number of poll operations " + numPolls +
        " to equal number of offer operations " + numOffers;
       throw new TestException(s); 
    }
  }

  /**
   * An INIT task that is used to mark the beginning of JProbe
   * measurements.
   */
  public static void jprobeEntry() {
    Log.getLogWriter().info("Begin JProbe measurement");
  }

  /**
   * A CLOSE task that is used to mark the ending of JProbe
   * measurements.
   */
  public static void jprobeExit() {
    Log.getLogWriter().info("End JProbe measurement");
  }

}
