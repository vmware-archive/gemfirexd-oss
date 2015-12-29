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
package nbsTests;

import hct.EventListener;

import com.gemstone.gemfire.cache.EntryEvent;

public class EventCounterListener extends EventListener {
  
 private long afterCreateEvents = 0;
 private long afterUpdateEvents = 0;
 private long afterDestroyEvents = 0;
 private long afterInvalidateEvents = 0;

  public void afterCreate(EntryEvent event) {
    super.afterCreate(event);
    afterCreateEvents ++;
  }

  public void afterDestroy(EntryEvent event) {
    super.afterDestroy(event);
    afterDestroyEvents ++;
    
  }

  public void afterInvalidate(EntryEvent event) {
    super.afterInvalidate(event);
    afterInvalidateEvents ++;
  }

  public void afterUpdate(EntryEvent event) {
    super.afterUpdate(event);
    afterUpdateEvents ++;
  }
  
  public long getAfterCreateEvents(){
    return afterCreateEvents;
  }
  
  public long getAfterUpdateEvents(){
    return afterUpdateEvents;
  }
  
  public long getAfterDestroyEvents(){
    return afterDestroyEvents;
  }
  
  public long getAfterInvalidateEvents(){
    return afterInvalidateEvents;
  }
  
  public String getEventCountersInfo(){
    return 
        " afterCreate=" + afterCreateEvents+
        ", afterDestroy=" + afterDestroyEvents+
        ", afterInvalidate=" + afterInvalidateEvents +
        ", afterUpdate=" + afterUpdateEvents
        ;
  }

}
