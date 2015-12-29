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

import container.app.lang.Visitable;
import container.app.lang.Visitor;
import container.app.phonedirectory.dao.PhoneDirectoryDao;
import container.app.phonedirectory.domain.Address;
import container.app.phonedirectory.domain.Person;
import container.app.phonedirectory.domain.PhoneDirectoryEntry;
import container.app.phonedirectory.domain.PhoneNumber;
import container.app.remoting.rmi.service.AbstractPingableService;
import container.app.util.Assert;
import container.app.util.ObjectUtils;
import container.app.util.StringUtils;

import java.rmi.RemoteException;
import java.util.Collection;
import java.util.EventObject;

public abstract class AbstractPhoneDirectoryService extends AbstractPingableService implements PhoneDirectoryService, Visitable {

  private PhoneDirectoryDao dao;

  protected PhoneDirectoryDao getDao() {
    Assert.state(dao != null, "The Phone Directory DAO for Phone Directory Service ({0}) was not properly initialized!",
        getClass().getName());
    return dao;
  }

  public final void setDao(final PhoneDirectoryDao dao) {
    Assert.notNull(dao, "The Phone Directory DAO for Phone Directory Service ({0}) cannot be null!",
        getClass().getName());
    this.dao = dao;
  }

  public void accept(final Visitor visitor) {
    visitor.visit(this);
    getDao().accept(visitor);
  }

  public void add(final Person person, final Address address) throws RemoteException {
    addInternal(new PhoneDirectoryEntry(person, address));
  }

  public void add(Person person, PhoneNumber phoneNumber) throws RemoteException {
    addInternal(new PhoneDirectoryEntry(person, phoneNumber));
  }

  public void add(Person person, Address address, PhoneNumber phoneNumber) throws RemoteException {
    addInternal(new PhoneDirectoryEntry(person, address, phoneNumber));
  }

  protected abstract void addInternal(PhoneDirectoryEntry entry);

  public <E extends EventObject> E consumePersistentEvent() throws RemoteException {
    throw new UnsupportedOperationException(StringUtils.NOT_IMPLEMENTED);
  }

  public Address getAddress(final Person person) throws RemoteException {
    final PhoneDirectoryEntry entry = getEntry(person);
    return (entry != null ? entry.getAddress() : null);
  }

  public PhoneDirectoryEntry getEntry(final Person person) throws RemoteException {
    for (final PhoneDirectoryEntry entry : getEntries()) {
      if (ObjectUtils.equals(person, entry.getPerson())) {
        return entry;
      }
    }

    return null;
  }

  public Collection<PhoneDirectoryEntry> getEntries() throws RemoteException {
    throw new UnsupportedOperationException(StringUtils.NOT_IMPLEMENTED);
  }

  public PhoneNumber getPhoneNumber(final Person person) throws RemoteException {
    final PhoneDirectoryEntry entry = getEntry(person);
    return (entry != null ? entry.getPhoneNumber() : null);
  }

  public void remove(final Person person) throws RemoteException {
    throw new UnsupportedOperationException(StringUtils.NOT_IMPLEMENTED);
  }

}
