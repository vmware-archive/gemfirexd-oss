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
package container.app.phonedirectory.service.provider;

import java.rmi.RemoteException;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.EventObject;
import java.util.Stack;

import container.app.dao.support.DataAccessEvent;
import container.app.dao.support.DataAccessListenerAdapter;
import container.app.phonedirectory.dao.factory.PhoneDirectoryDaoFactory;
import container.app.phonedirectory.domain.Person;
import container.app.phonedirectory.domain.PhoneDirectoryEntry;
import container.app.phonedirectory.service.AbstractPhoneDirectoryService;
import container.app.util.Assert;

public class PhoneDirectoryServiceImpl extends AbstractPhoneDirectoryService {
  
  private final Stack<DataAccessEvent> eventStack = new Stack<DataAccessEvent>();

  public PhoneDirectoryServiceImpl() {
    // TODO the DAO is a primary candidate for configuration and dependency injection using Spring baby!
    setDao(PhoneDirectoryDaoFactory.getPhoneDirectoryDao());
    getDao().addEventListener(new DataAccessEventTracker());
  }

  /**
   * This method implements custom business rules for a Phone Directory Entry to 
   * ensure that it adheres to the rules before being persisted.
   * 
   * @param entry the PhoneDirectoryEntry being validated against the custom business rules
   * implemented in this service class.   
   */
  protected void assertValidEntry(final PhoneDirectoryEntry entry) {
    Assert.notNull(entry, "The Phone Directory Entry to add or update cannot be null!");
    Assert.notNull(entry.getPerson(), "The Phone Directory Entry must specify a Person with some contact information!");
    
    final boolean addressNotNull = (entry.getAddress() != null);
    final boolean phoneNumberNotNull = (entry.getPhoneNumber() != null);
    
    Assert.state(addressNotNull || phoneNumberNotNull, MessageFormat.format(
        "The Phone Directory Entry must specify either an Address or Phone Number with Person ({0})!", 
        entry.getPerson()));
  }

  @Override
  protected void addInternal(final PhoneDirectoryEntry entry) {
    assertValidEntry(entry);
    getDao().save(entry);
  }
  
  @Override
  // TODO fix implementation; this is a hack.
  public <E extends EventObject> E consumePersistentEvent() throws RemoteException {
    return (E) (eventStack.isEmpty() ? null : eventStack.pop());
  }

  /**
   * Obtains an entry from the data store for the specified person. This is an optimized version 
   * of the getEntry method compared with the AbstractPhoneDirectoryService.getEntry method.
   * 
   * @param person is the Person used to lookup a Phone Directory Entry.
   * @return PhoneDirectoryEntry for the specified Person.
   * @see AbstractPhoneDirectoryService#getEntry(Person)
   */
  @Override
  public PhoneDirectoryEntry getEntry(final Person person) throws RemoteException {
    return getDao().load(person);
  }

  public Collection<PhoneDirectoryEntry> getEntries() throws RemoteException {
    return getDao().loadAll();
  }

  public void remove(final Person person) throws RemoteException {
    getDao().remove(person);
  }

  @Override
  public String toString() {
    return "Phone Directory Service";
  }

  protected class DataAccessEventTracker extends DataAccessListenerAdapter {

    @Override
    public void afterCreate(final DataAccessEvent event) {
      eventStack.push(event);
    }

    @Override
    public void afterRetrieve(final DataAccessEvent event) {
      eventStack.push(event);
    }

    @Override
    public void afterUpdate(final DataAccessEvent event) {
      eventStack.push(event);
    }

    @Override
    public void afterDelete(final DataAccessEvent event) {
      eventStack.push(event);
    }
  }

}
