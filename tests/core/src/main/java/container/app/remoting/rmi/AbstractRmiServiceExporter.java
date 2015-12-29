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
package container.app.remoting.rmi;

import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.RMISecurityManager;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import container.app.util.Assert;
import container.app.util.ComposableFilter;
import container.app.util.DefaultFilter;
import container.app.util.Filter;
import container.app.util.StringUtils;
import container.app.util.Sys;

public abstract class AbstractRmiServiceExporter implements Runnable {

  public static final int DEFAULT_REGISTRY_PORT = 1099;
  
  public static final String DEFAULT_REGISTRY_HOST = "localhost";

  private boolean remoteObjectIsBound = false;
  private boolean running = false;

  private volatile Registry registry;

  private volatile Remote remoteObject;

  protected static synchronized void setSecurityManager() {
    if (System.getSecurityManager() == null) {
      Sys.out("Setting the RMISecurityManager on System...");
      System.setSecurityManager(new RMISecurityManager());
    }
  }

  protected Registry getRegistry() throws RemoteException {
    if (this.registry == null) {
      try {
        Sys.out("Locating an RMI Registry running on ({0}) listening to port ({1})...", 
            getRegistryHost(), getRegistryPort());
        // LocateRegistry.getRegistry only returns a stub reference without actually establishing a connection to the Registry
        this.registry = LocateRegistry.getRegistry(getRegistryHost(), getRegistryPort());
        // call a method on Registry to force a connection and verify the Registry actually exists
        this.registry.list();
      } 
      catch (RemoteException e) {
        // One probable cause of this RemoteException is that the RMI Registry does not exist.
        Sys.err("Could not find an RMI Registry running on ({0}) listening to port ({1}): {2}",
            getRegistryHost(), getRegistryPort(), e.getMessage());
        Sys.out("Attempting to create an RMI Registry on localhost listening to port ({0})...", getRegistryPort());
        // Try creating the Registry on localhost using the configured port.
        this.registry = LocateRegistry.createRegistry(getRegistryPort());
      }
    }
    
    return this.registry;
  }

  protected String getRegistryHost() {
    return DEFAULT_REGISTRY_HOST;
  }

  protected int getRegistryPort() {
    return DEFAULT_REGISTRY_PORT;
  }

  protected <T extends Remote> T getRemoteObject() {
    Assert.state(remoteObject != null, "The remote object has not been exported, which occurs only during run!");
    return (T) remoteObject;
  }
  
  private void setRemoteObject(final Remote object) {
    this.remoteObject = object;
  }
  
  protected synchronized boolean isRemoteObjectBound() {
    return remoteObjectIsBound;
  }

  protected synchronized boolean isRunning() {
    return running;
  }

  protected abstract String getServiceNameForBinding();

  protected Filter<Object> getSystemPropertyFilter() {
    return ComposableFilter.composeAnd(new DefaultFilter<Object>(true), null);
  }

  protected abstract Remote exportRemoteObject() throws RemoteException;

  public synchronized void run() {
    Assert.state(!isRunning(), "The remote object service ({0}) is already running!", getServiceNameForBinding());

    try {
      setSecurityManager();
      setRemoteObject(exportRemoteObject());
      getRegistry().rebind(getServiceNameForBinding(), getRemoteObject());
      this.remoteObjectIsBound = true;
      this.running = true;
    }
    catch (Exception e) {
      throw new RuntimeException(MessageFormat.format("Failed to run remote object service ({0})!", 
          getServiceNameForBinding()), e);
    }
  }

  public void showAvailableServices() throws RemoteException {
    Sys.out("Available Remote Object Services: {0}", Arrays.asList(getRegistry().list()));
  }
  
  public void showGemFireState(final Filter<Object> propertyFilter) {
    throw new UnsupportedOperationException(StringUtils.NOT_IMPLEMENTED);
  }

  public void showSystemState(final Filter<Object> systemPropertyFilter) {
    Assert.notNull(systemPropertyFilter, "The object used to filter the System properties cannot be null!");

    for (final Object property : System.getProperties().keySet()) {
      if (systemPropertyFilter.accept(property)) {
        Sys.out("{0} = {1}", property, System.getProperty(property.toString()));
      }
    }
  }

  public synchronized void stop() throws Exception {
    try {
      if (this.running) {
        unbindRemoteObjectFromRegistry();
        unexportRemoteObject();
        setRemoteObject(null);
        stopRegistry();
      }
    }
    finally {
      this.running = false;
    }
  }

  protected void stopRegistry() {
    try {
      if (this.registry != null) {
        final int serviceCount = this.registry.list().length;
        Sys.out("The number of services in the registry is {0}.", serviceCount);
        if (serviceCount == 0) {
          if (UnicastRemoteObject.unexportObject(this.registry, true)) {
            stopNonDaemonRmiThreads();
            this.registry = null;
            Sys.out("The Registry was successfully unexported.");
          }
          else {
            Sys.out("The Registry could not be unexported due to unknown reasons.");
          }
        }
        else {
          Sys.out("Warning, the registry was found with services still bound.");
        }
      }
    }
    catch (NoSuchObjectException e) {
      Sys.err("The RMI Registry is not currently exported: {0}", e.getMessage());
    }
    catch (RemoteException e) {
      Sys.err("Failed to find an RMI Registry on host ({0}) listening to port ({1}): {2}", 
          getRegistryHost(), getRegistryPort(), e.getMessage());
    }
  }

  private void stopNonDaemonRmiThreads() {
    for (final Thread t : Thread.getAllStackTraces().keySet()) {
      if ("RMI Reaper".equals(t.getName())) {
        t.interrupt();
      }
    }
  }

  protected void unbindRemoteObjectFromRegistry() throws RemoteException {
    if (this.remoteObjectIsBound) {
      try {
        Sys.out("Unbinding remote object from service name ({0}) in registry...", getServiceNameForBinding());
        getRegistry().unbind(getServiceNameForBinding());
        this.remoteObjectIsBound = false;
      } 
      catch (NotBoundException e) {
        // ignore the NotBoundException since the remote object binding may have already been removed from the registry
        Sys.err("No remote object was bound to service name ({0}) in registry: {1}", getServiceNameForBinding(), 
            e.getMessage());
        this.remoteObjectIsBound = false;
      }
    }
  }
  
  protected void unexportRemoteObject() throws RemoteException {
    try {
      UnicastRemoteObject.unexportObject(getRemoteObject(), true);
    }
    catch (NoSuchObjectException e) {
      Sys.err("The remote object ({0}) is not currently exported: {1}", getServiceNameForBinding(), e.getMessage());
    }
  }

  protected static class SystemPropertyFilter implements Filter<Object> {

    private static final List<String> systemProperties = new ArrayList<String>();

    public SystemPropertyFilter() {
    }

    public boolean accept(final Object obj) {
      return systemProperties.contains(obj);
    }

    public boolean add(final String systemProperty) {
      return (StringUtils.hasValue(systemProperty) && systemProperties.add(systemProperty));
    }
    
    public boolean remove(final String systemProperty) {
      return systemProperties.remove(systemProperty);
    }
  }

}
