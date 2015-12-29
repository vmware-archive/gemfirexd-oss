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
package sql.tpce;

import java.util.ArrayList;

import hydra.blackboard.Blackboard;
import hydra.blackboard.SharedLock;

public class TPCEBB extends Blackboard {
  static String TPCE_BB_NAME = "TPCEBB_Blackboard";
  static String TPCE_BB_TYPE = "RMI";
  
  public static TPCEBB bbInstance = null;
  
  public static synchronized TPCEBB getBB() {
    if (bbInstance == null) {
        bbInstance = new TPCEBB(TPCE_BB_NAME, TPCE_BB_TYPE);
    }      
    return bbInstance;  
  }
  
  public TPCEBB() {
    
  }
  
  public TPCEBB(String name, String type) {
    super(name, type, TPCEBB.class);
  }
  
  private static SharedLock lock;
  private static String tradeIdsMapId = "tradeIdsMapId";
  
  public static int TradeIdsIndex;
  public static int TradeIdsInsertedInInitTask;
  
  public static int totalServerRecoveryTime;
  public static int importTableTime;
  
  //if no new trade_id available to process, returns -1 to indicate that
  @SuppressWarnings("unchecked")
  public long getNextTradeId() {
    long trade_id = -1;
    if (lock == null) lock = getSharedLock(); 
    
    lock.lock();
    ArrayList<Long> tradeIds = (ArrayList<Long>)getSharedMap().get(tradeIdsMapId);
    if (tradeIds == null) tradeIds = new ArrayList<Long>();
    
    if (tradeIds.size() > 0) {
      trade_id = tradeIds.remove(0);; 
      hydra.Log.getLogWriter().info("tradeIds removes " + trade_id);
      
    }
    
    getSharedMap().put(tradeIdsMapId, tradeIds);
      
    lock.unlock();
    return trade_id;

    
  }
  
  @SuppressWarnings("unchecked")
  public void addTradeId(long trade_id){
    if (lock == null) lock = getSharedLock(); 
    
    lock.lock(); 
    ArrayList<Long> tradeIds = (ArrayList<Long>)getSharedMap().get(tradeIdsMapId);
    if (tradeIds == null) tradeIds = new ArrayList<Long>();
    
    tradeIds.add(trade_id);
    hydra.Log.getLogWriter().info("tradeIds adds " + trade_id);
    getSharedMap().put(tradeIdsMapId, tradeIds);
    lock.unlock();
  }
  
  public static int tradeSentToMarket; //tracks how many trade submitted to MEE
  public static int tradeProcessedByMarket; //tracks how many trade processed
  
  
}
