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
package regions.validate;

import com.gemstone.gemfire.internal.LogWriterImpl;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Represents the "value" of a region entry.  Subclasses represent
 * things like "the entry is invalid" or "the entry is valid with
 * value <I>x</I>".  Instances of this class are placed into the
 * {@link ValidateBlackboard}.
 *
 * @author David Whitlock
 * @since 3.5
 */
public abstract class Value implements Serializable {

  /** Format timestamps to look the same as GemFire log */
  private static final DateFormat format =
    new SimpleDateFormat(LogWriterImpl.FORMAT);

  /** The time at which this object was created */
  private final long timestamp;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>Value</code> and records the current time.
   */
  protected Value() {
    this.timestamp = System.currentTimeMillis();
  }

  ////////////////////  Instance Methods  ////////////////////

  /**
   * Returns the time at which this <code>Value</code> object was
   * created.  We use this timestamp to determine how long it has been
   * since a region operation has taken place.  That is, while
   * validating if we see that the value of an entry only recently
   * changed, we may briefly pause to allow the change to propagate.
   */
  public final long getTimestamp() {
    return this.timestamp;
  }

  /**
   * Returns the timestamp in the same format as those in the GemFire
   * log files.
   */
  protected String formatTimestamp() {
    return format.format(new Date(this.getTimestamp()));
  }

  /**
   * Make sure that subclasses override this method
   */
  public abstract boolean equals(Object o);

  /**
   * Make sure that subclasses override this method
   */
  public abstract String toString();

}
