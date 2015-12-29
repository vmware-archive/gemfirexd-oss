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
package container.app.phonedirectory.dao;

import java.text.MessageFormat;
import java.util.Collection;

import container.app.dao.support.DataAccessEvent;
import container.app.dao.support.DataAccessListener;
import container.app.lang.AbstractEventSource;
import container.app.lang.Visitor;
import container.app.phonedirectory.domain.Person;
import container.app.phonedirectory.domain.PhoneDirectoryEntry;
import container.app.phonedirectory.support.PhoneDirectoryEntryNotFoundException;
import container.app.util.Assert;
import container.app.util.StringUtils;

public abstract class AbstractPhoneDirectoryDao extends AbstractEventSource<DataAccessListener> implements PhoneDirectoryDao {

  public void accept(final Visitor visitor) {
    visitor.visit(this);
  }

  public PhoneDirectoryEntry find(final Person person) {
    throw new UnsupportedOperationException(StringUtils.NOT_IMPLEMENTED);
  }

  public PhoneDirectoryEntry load(final Person person) {
    final PhoneDirectoryEntry entry = find(person);
    Assert.notNull(entry, new PhoneDirectoryEntryNotFoundException(MessageFormat.format(
        "Failed to find Phone Directory Entry for Person ({0})!", person)));
    return entry;
  }

  public Collection<PhoneDirectoryEntry> loadAll() {
    throw new UnsupportedOperationException(StringUtils.NOT_IMPLEMENTED);
  }

  public PhoneDirectoryEntry remove(final Person person) {
    throw new UnsupportedOperationException(StringUtils.NOT_IMPLEMENTED);
  }

  public PhoneDirectoryEntry save(final PhoneDirectoryEntry entry) {
    throw new UnsupportedOperationException(StringUtils.NOT_IMPLEMENTED);
  }

  protected void fireBeforeCreate(final DataAccessEvent event) {
    for (final DataAccessListener listener : this) {
      listener.beforeCreate(event);
    }
  }

  protected void fireAfterCreate(final DataAccessEvent event) {
    for (final DataAccessListener listener : this) {
      listener.afterCreate(event);
    }
  }

  protected void fireBeforeRetrieve(final DataAccessEvent event) {
    for (final DataAccessListener listener : this) {
      listener.beforeRetrieve(event);
    }
  }

  protected void fireAfterRetrieve(final DataAccessEvent event) {
    for (final DataAccessListener listener : this) {
      listener.afterRetrieve(event);
    }
  }

  protected void fireBeforeUpdate(final DataAccessEvent event) {
    for (final DataAccessListener listener : this) {
      listener.beforeUpdate(event);
    }
  }

  protected void fireAfterUpdate(final DataAccessEvent event) {
    for (final DataAccessListener listener : this) {
      listener.afterUpdate(event);
    }
  }

  protected void fireBeforeDelete(final DataAccessEvent event) {
    for (final DataAccessListener listener : this) {
      listener.beforeDelete(event);
    }
  }

  protected void fireAfterDelete(final DataAccessEvent event) {
    for (final DataAccessListener listener : this) {
      listener.afterDelete(event);
    }
  }

}
