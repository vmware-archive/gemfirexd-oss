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

import java.rmi.*;
//import java.rmi.server.*;
//import java.util.*;

/**
*
* Exported client methods that can be called remotely
* by the master controller.
*
*/
public interface RemoteTestModuleIF extends Remote {

  public void executeTask( int tsid, int type, int index )
  throws RemoteException; 

  public MethExecutorResult executeMethodOnClass( String className,
                                                  String methodName )
  throws RemoteException; 

  public MethExecutorResult executeMethodOnClass( String className,
                                                  String methodName,
                                                  Object[] args )
  throws RemoteException; 

  public MethExecutorResult executeMethodOnObject( Object obj,
                                                   String methodName )
  throws RemoteException; 

  public MethExecutorResult executeMethodOnObject( Object obj,
                                                   String methodName,
                                                   Object[] args )
  throws RemoteException; 

  public void disconnectVM()
  throws RemoteException;
  
  public void shutDownVM( boolean disconnect, boolean runShutdownHook )
  throws RemoteException;
  
  public void runShutdownHook()
  throws RemoteException;

 /**
  *  Notifies a client that dynamic action with the specified id is complete.
  */
  public void notifyDynamicActionComplete( int actionId )
  throws RemoteException;
}
