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
package query.remote;

import com.gemstone.gemfire.cache.EntryEvent;

/**
 * Thread which executes Query. The thread is spwaned from
 * afterDestroy()Callback of a CacheListener The event is stored in ThreadLocal
 * of this thread, so that it should be available to the validator
 * 
 * @author Yogesh Mahajan
 * 
 */
public class QueryExecutorThread extends Thread
{

  static ThreadLocal eventLocal = new ThreadLocal();
  
  private EntryEvent event = null ; 
  
  public QueryExecutorThread(EntryEvent e) {
    super("Query Executor Thread");
    this.event = e ;
  }

  public void run()
  {
    //set event to thread local of this thread 
    //so that it should be avilable to validator as well    
    eventLocal.set(this.event);
    //fire query and validate results using validator provided in conf file
    RemoteQueryTest.performQuery();    
  }
}