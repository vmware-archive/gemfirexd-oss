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

package hydra;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.*;

/**
 *  Represents a global round-robin choice from within a set of discrete values. 
 *  Note that this involves a round trip to the hydra master, so use sparingly.
 */
public class RobinG implements Serializable {

  private static int nextIndex = -1;
  private Vector values;

  public RobinG( Vector v ) {
     this.values = v;
  }
  public Object next( Long key ) {
     if ( RemoteTestModule.Master == null ) {
       synchronized( this ) {
         Object val = this.values.elementAt( ++nextIndex % this.values.size() ); 
         return val;
       }
     } else {
       try {
         return RemoteTestModule.Master.getGlobalNext( key );
       } catch( RemoteException e ) {
         throw new HydraRuntimeException( "Could not reach master" );
       }
     }
  }
  public String toString() {
     String str = "ROBING ";
     for ( int i = 0; i < this.values.size(); i++ ) {
        str += values.elementAt(i) + " ";
     }
     return str + "GNIBOR";
  }
}
