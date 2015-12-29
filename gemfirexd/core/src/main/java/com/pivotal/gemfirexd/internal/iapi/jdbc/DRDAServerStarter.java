/*

   Derby - Class com.pivotal.gemfirexd.internal.iapi.jdbc.DRDAServerStarter

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

/*
 * Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

package com.pivotal.gemfirexd.internal.iapi.jdbc;

// GemStone changes BEGIN
// GemStone changes END
import com.pivotal.gemfirexd.NetworkInterface.ConnectionListener;
import com.pivotal.gemfirexd.internal.engine.diag.SessionsVTI;
import com.pivotal.gemfirexd.internal.iapi.reference.MessageId;
import com.pivotal.gemfirexd.internal.iapi.services.monitor.ModuleControl;
import com.pivotal.gemfirexd.internal.iapi.services.monitor.Monitor;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;

import java.io.PrintWriter;
import java.lang.Runnable;
import java.lang.Thread;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * Class that starts the network server in its own daemon thread.
 * Works in two situations.
 * <BR>
 * As a module in the engine's Monitor, booted if the
 * property gemfirexd.drda.startNetworkServer is set to true.
 * In this case the boot and shutdown is through the
 * standard ModuleControl methods.
 * <BR>
 * Direct calls from the NetworkServerControlImpl start methods.
 * This is to centralize the creation of the daemon thread in
 * this class in the engine code, since the Monitor provides
 * the thread. This means that NetworkServerControlImpl calls
 * this class to create a thread which in turn calls back
 * to NetworkServerControlImpl.runServer to start the server.
 *
 * @see ModuleControl#boot
 * @see ModuleControl#stop
 */
public final class DRDAServerStarter implements ModuleControl, Runnable
{
    /**
     * The instance of the NetworkServerControlImpl
     * being used to run the server.
     */
    private Object server;
    
    /**
     * Reflect reference to the method to run the server.
     * NetworkServerControlImpl.blockingStart
     */
    private Method runServerMethod;
    
    /**
     * Reflect reference to the method to directly
     * shutdown the server.
     * NetworkServerControlImpl.directShutdown
     */
    private Method serverShutdownMethod;

    private Thread serverThread;
    private static final String serverClassName = "com.pivotal.gemfirexd.internal.impl.drda.NetworkServerControlImpl";
    private Class<?> serverClass;
	
	private InetAddress listenAddress =null;
	private int portNumber = -1;
	private String userArg = null;
	private String passwordArg = null;
	private PrintWriter consoleWriter = null;

    /**
     * Try to start the DRDA server. Log an error in error log and continue if it cannot be started.
     */
//     public static void start()
//     {


	/**
	 * Sets configuration information for the network server to be started.
	 * @param listenAddress InetAddress to listen on
	 * @param portNumber    portNumber to listen on
	 * @param userName      the user name for actions requiring authorization
	 * @param password      the password for actions requiring authorization
     *
	 * @throws Exception on error
	 */
	public void setStartInfo(InetAddress listenAddress, int portNumber,
                             String userName, String password,
                             PrintWriter consoleWriter)
	{
		this.userArg = userName;
		this.passwordArg = password;
        setStartInfo(listenAddress, portNumber, consoleWriter);
    }

	public void setStartInfo(InetAddress listenAddress, int portNumber, PrintWriter
							 consoleWriter)
	{
		this.listenAddress = listenAddress;
		this.portNumber = portNumber;

        // wrap the user-set consoleWriter with autoflush to true.
        // this will ensure that messages to console will be 
        // written out to the consoleWriter on a println.
        // DERBY-1466
        if (consoleWriter != null)
            this.consoleWriter = new PrintWriter(consoleWriter,true);
        else
            this.consoleWriter = consoleWriter;
	}
// GemStone changes BEGIN

	public void setStartInfo(String userName, String password) {
	  this.userArg = userName;
	  this.passwordArg = password;
	}
// GemStone changes END

    /**
     * Find the methods to start and shutdown the server.
     * Perfomed through reflection so that the engine
     * code is not dependent on the network server code.
     * @param serverClass
     * @throws NoSuchMethodException 
     * @throws SecurityException 
     */
    private void findStartStopMethods(final Class serverClass)
        throws SecurityException, NoSuchMethodException
    {
        // Methods are public so no need for privilege blocks.
        runServerMethod = serverClass.getMethod(
                "blockingStart", new Class[] { java.io.PrintWriter.class});
               
// GemStone changes BEGIN
        serverShutdownMethod = serverClass.getMethod("shutdown");
        initOtherPropertiesMethodHandle();
        /* (original code)
        serverShutdownMethod = serverClass.getMethod(
                "directShutdown", null);
        */
// GemStone changes END
    }

    public void boot(boolean create,
                     java.util.Properties properties)
    {
        if( server != null)
        {
            if (SanityManager.DEBUG)
                SanityManager.THROWASSERT( "Network server starter module booted twice.");
            return;
        }
        // Load the server class indirectly so that Derby does not require the network code
        try
        {
            serverClass = Class.forName( serverClassName);
        }
        catch( ClassNotFoundException cnfe)
        {
            Monitor.logTextMessage( MessageId.CONN_NETWORK_SERVER_CLASS_FIND, serverClassName);
            return;
        }
        catch( java.lang.Error e)
        {
            Monitor.logTextMessage( MessageId.CONN_NETWORK_SERVER_CLASS_LOAD,
                                    serverClassName,
                                    e.getMessage());
            return;
        }
        try
        {
            Constructor<?>  serverConstructor;
            try
            {
                serverConstructor = AccessController.doPrivileged(
			      new PrivilegedExceptionAction<Constructor<?>>() {
						  public Constructor<?> run() throws NoSuchMethodException, SecurityException
						  {
							  if (listenAddress == null)
								  return serverClass.getConstructor(
                                      new Class[]{String.class, String.class});
							  else
								  return
									  serverClass.getConstructor(new
										  Class[] {java.net.InetAddress.class,
												   Integer.TYPE,
                                                   String.class,
                                                   String.class});
                          }
					  }
				  );
            }
            catch( PrivilegedActionException e)
            {
                Exception e1 = e.getException();
                Monitor.logTextMessage(
									   MessageId.CONN_NETWORK_SERVER_START_EXCEPTION, e1.getMessage());
				e.printStackTrace(Monitor.getStream().getPrintWriter());
                return;

            }
            
            findStartStopMethods(serverClass);
            
			if (listenAddress == null) {
				server = serverConstructor.newInstance(
                    new Object[]{userArg, passwordArg});
            } else {
				server = serverConstructor.newInstance(new Object[]
// GemStone changes BEGIN
				    // changed to use Integer.valueOf()
				    {listenAddress, Integer.valueOf(portNumber),
				    /* (original code)
					{listenAddress, new Integer(portNumber),
				    */
// GemStone changes END
                     userArg, passwordArg});
            }

            serverThread = Monitor.getMonitor().getDaemonThread( this, "NetworkServerStarter", false);
            serverThread.start();
        }
        catch( Exception e)
        {
			Monitor.logTextMessage( MessageId.CONN_NETWORK_SERVER_START_EXCEPTION, e.getMessage());
			server = null;
			e.printStackTrace(Monitor.getStream().getPrintWriter());
        }
    } // end of boot

// GemStone changes BEGIN
    public void ping() {
      if (this.serverThread == null || !this.serverThread.isAlive()) {
        throw new IllegalStateException("Server thread not available");
      }
      this.invoke(NetworkServerControlProps.ping, null);
    }

    public void setConnectionListener(ConnectionListener connListener) {
      if (this.serverThread == null || !this.serverThread.isAlive()) {
        throw new IllegalStateException("Server thread not available");
      }
      this.invoke(NetworkServerControlProps.setConnectionListener,
          new Object[] { connListener });
    }

    public void collectStatisticsSamples() {
      if (this.serverThread == null || !this.serverThread.isAlive()) {
        //throw new IllegalStateException("Server thread not available");
        return;
      }
      this.invoke(NetworkServerControlProps.collectStatisticsSamples, null);
    }
    
    public void getSessionInfo(SessionsVTI.SessionInfo info) {
      if (this.serverThread == null || !this.serverThread.isAlive()) {
        return;
      }
      this.invoke(NetworkServerControlProps.getSessionInfo, new Object[] { info });
    }
    
// GemStone changes END
    public void run()
    {
        try
        {
            runServerMethod.invoke( server,
                                      new Object[] {consoleWriter });
        }
        catch( InvocationTargetException ite)
        {
            Monitor.logTextMessage(
								   MessageId.CONN_NETWORK_SERVER_START_EXCEPTION, ite.getTargetException().getMessage());
			ite.printStackTrace(Monitor.getStream().getPrintWriter());

            server = null;
        }
        catch( Exception e)
        {
            Monitor.logTextMessage( MessageId.CONN_NETWORK_SERVER_START_EXCEPTION, e.getMessage());
            server = null;
			e.printStackTrace(Monitor.getStream().getPrintWriter());
        }
    }
    
    public void stop()
    {
		try {
			if( serverThread != null && serverThread.isAlive())
			{
				serverShutdownMethod.invoke( server,
											 (Object[])null);
				AccessController.doPrivileged(
							      new PrivilegedAction() {
								  public Object run() {
								      serverThread.interrupt();
								      return null;
								  }
							      });				
				serverThread = null;
			}
		   
		}
		catch( InvocationTargetException ite)
        {
			Monitor.logTextMessage(
								   MessageId.CONN_NETWORK_SERVER_SHUTDOWN_EXCEPTION, ite.getTargetException().getMessage());
			ite.printStackTrace(Monitor.getStream().getPrintWriter());
			
        }
        catch( Exception e)
        {
            Monitor.logTextMessage( MessageId.CONN_NETWORK_SERVER_SHUTDOWN_EXCEPTION, e.getMessage());
			e.printStackTrace(Monitor.getStream().getPrintWriter());
		}
			
		serverThread = null;
		server = null;
		serverClass = null;
		listenAddress = null;
		portNumber = -1;
		consoleWriter = null;
		
    } // end of stop
    //GemStone changes BEGIN
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(listenAddress);
      sb.append(" [ ");
      sb.append(portNumber); 
      sb.append(" ] ");
      sb.append(" user = ");
      sb.append(userArg);
      return sb.toString();
    }
    
    public Object invoke(NetworkServerControlProps method, Object[] args) {
      return method.invoke(this.server, args);
    }

    public enum NetworkServerControlProps {
      ping {
        @Override
        public void initMethodHandle(Class<?> serverClass)
            throws SecurityException, NoSuchMethodException {
          this.methodHandle = serverClass.getMethod("ping");
        }
      },
      traceBoolean {
        @Override
        public void initMethodHandle(Class<?> serverClass)
            throws SecurityException, NoSuchMethodException {
          this.methodHandle = serverClass.getMethod("trace",
              new Class[] { boolean.class });
        }
      },
      traceConnNumBoolean {
        @Override
        public void initMethodHandle(Class<?> serverClass)
            throws SecurityException, NoSuchMethodException {
          this.methodHandle = serverClass.getMethod("trace", new Class[] {
              int.class, boolean.class });
        }
      },
      logConnections {
        @Override
        public void initMethodHandle(Class<?> serverClass)
            throws SecurityException, NoSuchMethodException {
          this.methodHandle = serverClass.getMethod("logConnections",
              new Class[] { boolean.class });
        }
      },
      sendSetTraceDirectory {
        @Override
        public void initMethodHandle(Class<?> serverClass)
            throws SecurityException, NoSuchMethodException {
          this.methodHandle = serverClass.getMethod("sendSetTraceDirectory",
              new Class[] { String.class });
        }
      },
      sysinfo {
        @Override
        public void initMethodHandle(Class<?> serverClass)
            throws SecurityException, NoSuchMethodException {
          this.methodHandle = serverClass.getMethod("sysinfo");
        }
      },
      runtimeInfo {
        @Override
        public void initMethodHandle(Class<?> serverClass)
            throws SecurityException, NoSuchMethodException {
          this.methodHandle = serverClass.getMethod("runtimeInfo");
        }
      },
      netSetMaxThreads {
        @Override
        public void initMethodHandle(Class<?> serverClass)
            throws SecurityException, NoSuchMethodException {
          this.methodHandle = serverClass.getMethod("netSetMaxThreads",
              new Class[] { int.class });
        }
      },
      netSetTimeSlice {
        @Override
        public void initMethodHandle(Class<?> serverClass)
            throws SecurityException, NoSuchMethodException {
          this.methodHandle = serverClass.getMethod("netSetTimeSlice",
              new Class[] { int.class });
        }
      },
      getHostAddressAndPort {
        @Override
        public void initMethodHandle(Class<?> serverClass)
            throws SecurityException, NoSuchMethodException {
          this.methodHandle = serverClass.getMethod("getHostAddressAndPort",
              new Class[] { int[].class });
        }
      },
      getCurrentProperties {
        @Override
        public void initMethodHandle(Class<?> serverClass)
            throws SecurityException, NoSuchMethodException {
          this.methodHandle = serverClass.getMethod("getCurrentProperties");
        }
      },
      setConnectionListener {
        @Override
        public void initMethodHandle(Class<?> serverClass)
            throws SecurityException, NoSuchMethodException {
          this.methodHandle = serverClass.getMethod("setConnectionListener",
              new Class[] { ConnectionListener.class });
        }
      },
      collectStatisticsSamples {

        @Override
        void initMethodHandle(Class<?> serverClass) throws SecurityException,
            NoSuchMethodException {
          this.methodHandle = serverClass.getMethod("collectStatisticsSample");
        }
        
      },
      getSessionInfo {

        @Override
        void initMethodHandle(Class<?> serverClass) throws SecurityException,
            NoSuchMethodException {
          this.methodHandle = serverClass.getMethod("getSessionInfo",
              new Class[] { SessionsVTI.SessionInfo.class });
        }
      };

      protected Method methodHandle = null;

      abstract void initMethodHandle(Class<?> serverClass)
          throws SecurityException, NoSuchMethodException;

      final Object invoke(Object server, Object[] args) {
        try {
          return this.methodHandle.invoke(server, args);
        } catch (IllegalAccessException ie) {
          throw new IllegalStateException(ie);
        } catch (InvocationTargetException ie) {
          throw new IllegalStateException(ie.getTargetException());
        }
      }
    }

    private void initOtherPropertiesMethodHandle() throws SecurityException,
        NoSuchMethodException {
      for (NetworkServerControlProps e : NetworkServerControlProps.values()) {
        e.initMethodHandle(this.serverClass);
      }
    }
    //GemStone changes END
}
