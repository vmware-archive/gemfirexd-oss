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
/**
 * 
 */
package sql.memscale;

import hydra.GsRandom;
import hydra.TestConfig;
import util.TestException;

/**
 * @author lynng
 * 
 * Class to obtain a sequence of numbers which do not intersect with any other thread's
 * sequence of numbers.
 * 
 * This is useful to process a sequence of numbers, perhaps interpreted by the test
 * as primary keys, in a certain order, and have the have the ability to replay that same sequence.
 * 
 * Tests can use this to destroy a certain number of rows using the sequence of numbers, then
 * recreate those rows by replaying the same sequence.
 * 
 * Each thread that uses this has a different sequence of numbers, thus only one thread will
 * delete a row with a given primary key, so each delete should be successful.
 * 
 * Randomly chosen starting points for each thread ensure the sequence numbers are scattered
 * across the range of sequence number for all threads.
 * 
 * 
 */
public class NumberSequencer {

  // a id number for this instance; each instance has an id number of 1..numberInstances
  private int sequencerId = 0;

  // the total number of NumberSequencerInstances used by the test
  private int numSequencerInstances = 0;

  // the maximum sequence number to be returned by any thread
  private int maxSequenceNumber = 0;

  // the starting sequence number, randomly chosen to scatter each thread's sequence throughout the range of 1..maxSequenceNumber
  private int startingSequenceNumber = 0;

  // the highest sequence number that can be returned by this instance of NumberSequence
  private int myMaxSequenceNumber = 0;

  // the total number of sequence numbers that can be returned by this thread
  private int numAvailableSequenceNumbers = 0;

  // the last sequence number returned by next()
  private int lastSequenceNumber = -1;

  // the maximum number of sequence numbers to return by next(), used to limit the sequence to less than all
  private int maxSequenceNumbersToReturn = -1;
  
  // the number of sequence numbers returned by next() since the last reset() or setRandomStart()
  private int returnedCount = 0;

  /** Constructor.
   * 
   * @param maxSequenceNumberArg The maximum sequence number than can be returned by any
   *        instance of this class. All instances should be created with the same value.
   *        For example, if multiple threads are using this to coordinate a sequence of
   *        primary keys valued 1..N, this would be N.
   * @param numSequencerInstancesArg This is the total number of NumberSequencer instances
   *        in the test. For example, if multiple threads are using this to coordinate a
   *        sequence of primary keys, this would be the number of threads.
   */
  public NumberSequencer(int maxSequenceNumberArg, int numSequencerInstancesArg) {
    maxSequenceNumber = maxSequenceNumberArg;
    numSequencerInstances = numSequencerInstancesArg;

    // calculate everything else
    sequencerId = (int) NumberSequencerBB.getBB().getSharedCounters().incrementAndRead(NumberSequencerBB.idNumber);

    // set numOfMyAvailableSequenceNumbers
    int quotient = maxSequenceNumber / numSequencerInstances;
    int remainder = maxSequenceNumber % numSequencerInstances;
    numAvailableSequenceNumbers = quotient;
    if (sequencerId <= remainder) {
      numAvailableSequenceNumbers++;
    }

    // set myMaxSequenceNumber
    myMaxSequenceNumber = sequencerId + ((numAvailableSequenceNumbers - 1) * numSequencerInstances);
    if (myMaxSequenceNumber > maxSequenceNumber) {
      throw new TestException("Test problem: myMaxSequenceNumber " + myMaxSequenceNumber + " is > maxSequenceNumber " + maxSequenceNumber);
    }

    setRandomStart();
  }

  /** Set a new starting point to obtain a different sequence of numbers returned by next()
   * 
   */
  public void setRandomStart() {
    GsRandom rand = TestConfig.tab().getRandGen();
    startingSequenceNumber = sequencerId + (rand.nextInt(0, numAvailableSequenceNumbers - 1) * numSequencerInstances);
    lastSequenceNumber = -1;
    returnedCount = 0;
  }

  /** Reset to obtain the same sequence of numbers returned by next()
   * 
   */
  public void reset() {
    lastSequenceNumber = -1;
    returnedCount = 0;
  }

  /** Return the next number in the sequence. Returns -1 if all sequence numbers
   *  have already been returned since the last reset() call or setRandomStart() call.
   * 
   * @return The next number in the sequence, or -1 if no more. 
   */
  public int next() {
    if (lastSequenceNumber == -1) { // this is the first next() call for a newly created NumberSequencer
      lastSequenceNumber = startingSequenceNumber;
    } else {
      if (returnedCount == maxSequenceNumbersToReturn) { // we already returned the max; don't advance
        return -1;
      }
      // find the next sequence number to be returned
      int nextSequenceNumber = lastSequenceNumber + numSequencerInstances;
      if (nextSequenceNumber > myMaxSequenceNumber) { // wrap around to the beginning
        nextSequenceNumber = sequencerId;
      }
      if (nextSequenceNumber == startingSequenceNumber) { // wrapped all the way around, so we have stopped
        return -1;
      } else {
        lastSequenceNumber = nextSequenceNumber; // set the sequence number to return
      }
    }
    returnedCount++;
    return lastSequenceNumber;
  }

  /** Set a stopping point to return fewer than the full set of sequence numbers.
   * 
   *  
   */
  
  /** Set a stopping point to return fewer than the full set of sequence numbers.
   * 
   * @param percent Set a stopping point to stop the sequence after this percentage
   *                of the entire sequence set has been returned by next().
   */
  public void setStopPercentage(int percent) {
    if ((percent > 100) || (percent < 0)) {
      throw new TestException("Stop percentage " + percent + " must be >= 0 and <= 100");
    }
    maxSequenceNumbersToReturn = (int) (numAvailableSequenceNumbers * ((float)percent/100.0));
  }

  @Override
  public String toString() {
    return "NumberSequencer [sequencerId=" + sequencerId
        + ", numSequencerInstances=" + numSequencerInstances
        + ", maxSequenceNumber=" + maxSequenceNumber
        + ", startingSequenceNumber=" + startingSequenceNumber
        + ", myMaxSequenceNumber=" + myMaxSequenceNumber
        + ", numAvailableSequenceNumbers=" + numAvailableSequenceNumbers
        + ", lastSequenceNumber=" + lastSequenceNumber
        + ", maxSequenceNumbersToReturn=" + maxSequenceNumbersToReturn
        + ", returnedCount=" + returnedCount + "]";
  }

}
