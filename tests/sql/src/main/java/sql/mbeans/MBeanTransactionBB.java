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
package sql.mbeans;

import hydra.blackboard.Blackboard;

public class MBeanTransactionBB extends Blackboard {
  // Blackboard creation variables
  static String MBean_BB_NAME = "MBeanTxn_Blackboard";
  static String MBean_BB_TYPE = "RMI";

  public static MBeanTransactionBB bbInstance = null;

  public static synchronized MBeanTransactionBB getBB() {
    if (bbInstance == null) {
      bbInstance = new MBeanTransactionBB(MBean_BB_NAME, MBean_BB_TYPE);
    }
    return bbInstance;
  }

  public MBeanTransactionBB() {

  }

  public MBeanTransactionBB(String name, String type) {
    super(name, type, MBeanTransactionBB.class);
  }
}