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

/**
*
* A thread subclass whose instances contain RemoteTestModules
* representing hydra client threads.  
*
*/
public class HydraThread extends Thread {

  /** Holds the RemoteTestModule on whose behalf this Thread was created */
  private RemoteTestModule mod; 

  /** Used by HydraSubthread to wrap its RemoteTestModule. */
  protected HydraThread(Runnable target, RemoteTestModule mod, String name) {
     super(null, target, name);
     if (mod == null) {
       String s = "Attempt to spawn hydra thread with no RemoteTestModule";
       throw new HydraRuntimeException(s);
     }
     this.mod = mod;
  }

  public HydraThread( Runnable target, String name ) {
     super( null, target, name );
     this.mod = (RemoteTestModule) target;
  }

  public RemoteTestModule getRemoteMod() {
    return mod;
  }
}
