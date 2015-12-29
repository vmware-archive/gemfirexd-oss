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

import hydra.blackboard.Blackboard;

/**
 * @author lynng
 *
 */
  public class NumberSequencerBB extends Blackboard {

    //Blackboard creation variables
    static String SQL_BB_NAME = "NumberSequencer_Blackboard";
    static String SQL_BB_TYPE = "RMI";
    
    // shared counters
    public static int idNumber;

    public static NumberSequencerBB bbInstance = null;

    public static synchronized NumberSequencerBB getBB() {
      if (bbInstance == null) {
        bbInstance = new NumberSequencerBB(SQL_BB_NAME, SQL_BB_TYPE);
      }      
      return bbInstance;  
    }

    public NumberSequencerBB() {

    }

    public NumberSequencerBB(String name, String type) {
      super(name, type, NumberSequencerBB.class);
    }

}
