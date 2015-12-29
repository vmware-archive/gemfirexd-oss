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
package container.app.phonedirectory.service;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.EventObject;

import container.app.lang.Visitor;
import container.app.phonedirectory.domain.Address;
import container.app.phonedirectory.domain.Person;
import container.app.phonedirectory.domain.PhoneDirectoryEntry;
import container.app.phonedirectory.domain.PhoneNumber;
import container.app.remoting.rmi.service.Pingable;

public interface PhoneDirectoryService extends Pingable, Remote {

  public void accept(Visitor visitor) throws RemoteException;

  public void add(Person person, Address address) throws RemoteException;

  public void add(Person person, PhoneNumber phoneNumber) throws RemoteException;

  public void add(Person person, Address address, PhoneNumber phoneNumber) throws RemoteException;

  public <E extends EventObject> E consumePersistentEvent() throws RemoteException;

  public Address getAddress(Person person) throws RemoteException;

  public PhoneDirectoryEntry getEntry(Person person) throws RemoteException;

  public Collection<PhoneDirectoryEntry> getEntries() throws RemoteException;

  public PhoneNumber getPhoneNumber(Person person) throws RemoteException;

  public void remove(Person person) throws RemoteException;

}
