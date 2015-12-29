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

import java.util.Collection;

import container.app.dao.support.DataAccessListener;
import container.app.lang.EventSource;
import container.app.lang.Visitable;
import container.app.phonedirectory.domain.Person;
import container.app.phonedirectory.domain.PhoneDirectoryEntry;

public interface PhoneDirectoryDao extends EventSource<DataAccessListener>, Visitable {

  public PhoneDirectoryEntry find(Person person);
  
  public PhoneDirectoryEntry load(Person person);
  
  public Collection<PhoneDirectoryEntry> loadAll();
  
  public PhoneDirectoryEntry remove(Person person);

  public PhoneDirectoryEntry save(PhoneDirectoryEntry entry);

}
