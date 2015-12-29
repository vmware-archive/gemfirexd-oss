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
package hdfs.mapreduce; 

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import util.TestHelper;

import com.gemstone.gemfire.cache.hdfs.internal.PersistedEventImpl;
import com.gemstone.gemfire.cache.hdfs.internal.SortedHoplogPersistedEvent;
import com.gemstone.gemfire.cache.hdfs.internal.UnsortedHoplogPersistedEvent;

public class PEIWritable implements Writable {

  private PersistedEventImpl event;

  public PEIWritable() {
  }

  public PersistedEventImpl getEvent() {
    return event;
  }

  public PEIWritable(PersistedEventImpl event) {
    this.event = event;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    boolean sorted = event instanceof SortedHoplogPersistedEvent;
    out.writeBoolean(sorted);
    event.toData(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    try {
      boolean sorted  = in.readBoolean();
      if(sorted) {
        event = new SortedHoplogPersistedEvent();
      } else {
        event = new UnsortedHoplogPersistedEvent();
      }
      event.fromData(in);
    } catch (ClassNotFoundException e) {
      System.out.println("PEIWritable.readFields caught " + e + ": " + TestHelper.getStackTrace(e));
    }
  }
}
