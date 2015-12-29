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
package com.gemstone.gemfire.internal.cache.partitioned.fixed;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;

import com.gemstone.gemfire.cache.EntryOperation;
import com.gemstone.gemfire.cache.PartitionResolver;

public class MyDate2 extends Date implements PartitionResolver{

  public MyDate2(long time) {
    super(time);
  }
  
  public String getName() {
    return "MyDate2";
  }

  public Serializable getRoutingObject(EntryOperation opDetails) {
    Date date = (Date)opDetails.getKey();
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    int month = cal.get(Calendar.MONTH);
    return month;
  
  }

  public void close() {
    // TODO Auto-generated method stub
  }
  
  public String toString(){
    return "MyDate2";
  }
}
