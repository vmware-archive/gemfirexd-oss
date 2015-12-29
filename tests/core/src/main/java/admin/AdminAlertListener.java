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

package admin;

import hydra.*;
import hydra.blackboard.*;

import com.gemstone.gemfire.admin.*;

public class AdminAlertListener implements AlertListener {

  public void alert(Alert alert) {

    String message = alert.getMessage();
    Log.getLogWriter().info("ALERT from " +
                            alert.getSystemMember().getId() + ": " +
                            message);

    SharedCounters sc = AdminBB.getInstance().getSharedCounters();
    if ( message.indexOf( "has joined the distributed cache" ) != -1) {
      // Don't count startup 'join' messages
      if (sc.read( AdminBB.recycleRequests ) > 0) {
        sc.increment( AdminBB.joinedNotifications );
      }
    } 

    if ( message.indexOf( "has left the distributed cache" ) != -1) {
      sc.increment( AdminBB.departedNotifications );
    }

    AdminBB.getInstance().printSharedCounters();
    
  }
}
