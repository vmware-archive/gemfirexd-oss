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
package com.gemstone.gemfire.internal.cache.execute;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.cache.EntryOperation;
import com.gemstone.gemfire.cache.PartitionResolver;
import com.gemstone.gemfire.internal.cache.xmlcache.Declarable2;

/**
 * Example implementation of a Partition Resolver which uses part of the value
 * for custom partitioning.  This example is a simplification of what GemFireXD
 * may do when the DDL specifies "partition by"    

 */
class MonthBasedPartitionResolver implements PartitionResolver, Declarable2 {
  
  private static MonthBasedPartitionResolver mbrResolver = null;
  final static String id = "MonthBasedPartitionResolverid1";
  private Properties properties;
  private String resolverName;
  
  
  public MonthBasedPartitionResolver ()
  { }

  public static MonthBasedPartitionResolver getInstance() {
    if(mbrResolver == null) {
      mbrResolver = new MonthBasedPartitionResolver();
    }
    return mbrResolver;
 }

  public Serializable getRoutingObject(EntryOperation opDetails) {
    Serializable routingObj = (Serializable)opDetails.getKey();
    Calendar cal = Calendar.getInstance();
    cal.setTime((Date)routingObj);        
    return new SerializableMonth(cal.get(Calendar.MONTH));
  }

  public void close() {
    // Close internal state when Region closes
  }

  public void init(Properties props) {
    this.properties = props;
  }

//  public Properties getProperties(){
//return this.properties;
//  }
  
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof MonthBasedPartitionResolver) {
//      MonthBasedPartitionResolver epc = (MonthBasedPartitionResolver) obj;
      return id.equals(MonthBasedPartitionResolver.id);
    } else {
      return false;
    }
  }
  
  public String getName()
  {
    return this.resolverName;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.xmlcache.Declarable2#getConfig()
   */
  public Properties getConfig() {
    return this.properties;
  }

  class SerializableMonth implements DataSerializable {
    private int month;

    public SerializableMonth(int month) {
      this.month = month;
    }

    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {
      this.month = in.readInt();
    }

    public void toData(DataOutput out) throws IOException {
      out.writeInt(this.month);
    }

    public int hashCode() {
      if (this.month < 4)
        return 1;
      else if (this.month >= 4 && this.month < 8)
        return 2;
      else
        return 3;
    }
  }
}
