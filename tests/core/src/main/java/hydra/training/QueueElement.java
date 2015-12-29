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

import hydra.HydraThreadLocal;
import java.io.Serializable;

/**
 * An object that is put into the <code>RemoteBlockingQueue</code>
 * it keeps track of which logical Hydra client thread created it.
 * This allows FIFO ordering of the queue to be validated when the
 * <code>QueueElement</code> object is removed.
 *
 * @author David Whitlock
 * @since 4.0
 */
class QueueElement implements Serializable {

  /** A Hydra thread local that maintains the sequence number of
   * this <code>QueueElement</code>. */
  private static final HydraThreadLocal nextSequenceNumber = new
    HydraThreadLocal() {
      protected Object initialValue() {
        return new Integer(0);
      }
    };

  //////////////////////  Instance Fields  //////////////////////

  /** The name of the logical Hydra thread that created this
   * <code>QueueElement</code>. */
  private String threadName;

  /** The sequence number of this <code>QueueElement</code> */
  private int sequenceNumber;

  ///////////////////////  Constructors  ///////////////////////

  /**
   * Creates a new <code>QueueElement</code>
   */
  public QueueElement() {
    this.threadName = Thread.currentThread().getName();
    this.sequenceNumber =
      ((Integer) nextSequenceNumber.get()).intValue();
    nextSequenceNumber.set(new Integer(this.sequenceNumber + 1));
  }

  //////////////////////  Instance Methods  /////////////////////

  /**
   * Returns the name of the Hydra client thread that created this
   * <code>QueueElement</code>.
   */
  public String getThreadName() {
    return this.threadName;
  }

  /**
   * Returns the sequence number of this <code>QueueElement</code>.
   */
  public int getSequenceNumber() {
    return this.sequenceNumber;
  }

  public String toString() {
    return this.getThreadName() + "-" + this.getSequenceNumber();
  }

}
