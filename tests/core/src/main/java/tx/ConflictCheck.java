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
package tx;

import util.TxHelper;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.cache.TXStateInterface;

import hydra.*;
import util.*;

public class ConflictCheck  {
  public static ConflictCheck testInstance = null;
  
  private final String KEY = "key";
  
  private HydraThreadLocal txState = new HydraThreadLocal();

  private Region aRegion;
    
  public synchronized static void HydraTask_initialize(){
    if (testInstance == null)
      testInstance = new ConflictCheck();
    testInstance.initialize();
  }
    
  protected void initialize() {
    Cache aCache = CacheHelper.createCache(ConfigPrms.getCacheConfig());
    CacheUtil.setCache(aCache);     // required by TxUtil and splitBrain/distIntegrityFD.conf
    aRegion = RegionHelper.createRegion(ConfigPrms.getRegionConfig());
    aRegion.put(KEY, new Integer(0));
  }
  
  public static void HydraTask_txOnlyConflictCheck() {
    testInstance.txOnlyConflictCheck();
  }
  

  private boolean hasCommitted = false;
  /**
   * In round 1, one thread  (thread one) begins transaction and getKey and it 
   * waits until round 3 to do a commit;
   * all other threads try to get the same key in their own transactions; 
   * In round 2, all threads try to commit -- only one can commit successfully
   * In round 3, thread one tries to commit and it should detect the write opertation 
   * by another thread and cause commit to fail.
   *
   */
  protected void txOnlyConflictCheck() {
    boolean threadOne = TxUtil.logRoundRobinNumber();
    TxUtil.logExecutionNumber();
    long rrNumber = TxUtil.getRoundRobinNumber();
    long whichRound = rrNumber % 3;
    if (whichRound == 1) { // round to open transactions
       Log.getLogWriter().info("In round " + rrNumber + " to open transactions, threadOne is " + threadOne);
       TxHelper.begin();

       Integer val = (Integer)aRegion.get(KEY);
       Log.getLogWriter().info("The value in the region is " + val);
             
       txState.set(TxHelper.internalSuspend()); 

    } else if (whichRound == 2) { // close transactions
      
       if (threadOne) {
         
         TxHelper.internalResume((TXStateInterface)txState.get()); 

         int val = TestConfig.tab().getRandGen().nextInt(1, 100);         
         aRegion.put(KEY, new Integer(val));
         
         txState.set(TxHelper.internalSuspend()); 
         
         Log.getLogWriter().info("In round " + rrNumber + " threadOne writes, but not commit yet");
       } else {
         TxHelper.internalResume((TXStateInterface)txState.get()); 

         Log.getLogWriter().info("In round " + rrNumber + " to close transactions, threadOne is " 
             + threadOne);
         
         int val = TestConfig.tab().getRandGen().nextInt(1, 100);         

         // workaround for BUG 36436: return value from put can be null (NOT_AVAILABLE)
         // in edgeClients (behavior is actually undefined)
         Integer old = null;
         if (aRegion.getAttributes().getPoolName() != null) {
           old = (Integer)aRegion.get(KEY);
           aRegion.put(KEY, new Integer(val));
         } else {
           old = (Integer)aRegion.put(KEY, new Integer(val));
         }

         Log.getLogWriter().info("put(" + KEY + ", " + val + ") returns oldValue " + old);

         if (old.intValue() != 0) {
           throw new TestException("old was " + old);
         }
         if (hasCommitted) {
           hasCommitted = false;
           Log.getLogWriter().info("original value  before put is " + old
               + ", so commit should fail" );
           TxHelper.commitExpectFailure();
         } else {
           Log.getLogWriter().info("original value  before put is " + old 
               + ", so commit should be successful");
           TxHelper.commitExpectSuccess();
           hasCommitted = true;
         }
       }
    } else { // round 3 to restore key value in the region
        if (threadOne) {
        Log.getLogWriter().info("In round " + rrNumber + " threadOne to verify commit failure and " + 
            "restores the value in the region");

        TxHelper.internalResume((TXStateInterface)txState.get()); 
/*        
        int val = TestConfig.tab().getRandGen().nextInt(1, 100);         
        int old = ((Integer)aRegion.put(KEY, new Integer(val))).intValue();
        
        if (old == 0) {
          throw new TestException("In round " + rrNumber + " put was not successful in round 2");
        }
        else {
          Log.getLogWriter().info("original value  before put is " + old
              + " so commit should fail" );
          TxHelper.commitExpectFailure();
        }
*/

        TxHelper.commitExpectFailure();
        
        aRegion.put(KEY, new Integer(0)); //restore the value
      }
    }
  }

}
