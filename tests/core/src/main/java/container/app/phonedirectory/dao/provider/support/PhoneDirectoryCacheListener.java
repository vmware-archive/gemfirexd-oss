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
package container.app.phonedirectory.dao.provider.support;

import java.util.Properties;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;

import container.app.dao.DataAccessType;
import container.app.phonedirectory.domain.PhoneDirectoryEntry;
import container.app.util.SystemUtils;

public class PhoneDirectoryCacheListener extends CacheListenerAdapter<String, PhoneDirectoryEntry> implements Declarable {

  @Override
  public void afterCreate(final EntryEvent<String, PhoneDirectoryEntry> event) {
    SystemUtils.printToStandardOut("Created a new cache entry with key ({0}) and value ({1}).", 
        event.getKey(), event.getNewValue());
  }

  @Override
  public void afterUpdate(final EntryEvent<String, PhoneDirectoryEntry> event) {
    SystemUtils.printToStandardOut("Updated cache entry with key ({0}) from ({1}) to ({2}).", 
        event.getKey(), event.getOldValue(), event.getNewValue());
  }

  @Override
  public void afterDestroy(EntryEvent<String, PhoneDirectoryEntry> event) {
    SystemUtils.printToStandardOut("Deleted existing cache entry with key ({0}) and value ({1}).", 
        event.getKey(), event.getNewValue());
  }

  protected CacheDataAccessEvent createCacheDataAccessEvent(final Object source,
      final DataAccessType op, 
      final String key, 
      final PhoneDirectoryEntry value) 
  {
    return new CacheDataAccessEvent(source, op, key, value);
  }

  public void init(final Properties props) {
    SystemUtils.printToStandardOut("Initialized CacheListener ({0})", this);
  }
  
  @Override
  public String toString() {
    return getClass().getName();
  }

}
