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
package management.operations.events;

public interface EntryOperationEvents {
  
  /* Entry Operations */

  public void entryAdded(String regionName, String key, Object value);

  public void entryInvalidated(String regionName, String key);

  public void entryDestroyed(String regionName, String key);

  public void entryUpdated(String regionName, String key, Object value);

  public void entryRead(String regionName, String key);

  public void entryInvalidatedLocal(String regionName, String key);

  public void entryDestroyedLocal(String regionName, String key);

  public void entryPutIfAbsent(String regionName, String key, Object value);

  public void entryRemoved(String regionName, String key);

  public void entryReplaced(String regionName, String key, Object value);

  public static String EVENT_ENTRY_ADDED = "EVENT_ENTRY_ADDED";
  public static String EVENT_ENTRY_INVALIDATED = "EVENT_ENTRY_INVALIDATED";
  public static String EVENT_ENTRY_DESTROYED = "EVENT_ENTRY_DESTROYED";
  public static String EVENT_ENTRY_UPDATED = "EVENT_ENTRY_UPDATED";
  public static String EVENT_ENTRY_READ = "EVENT_ENTRY_READ";
  public static String EVENT_ENTRY_INVALIDATEDLOCAL = "EVENT_ENTRY_INVALIDATEDLOCAL";
  public static String EVENT_ENTRY_DESTROYEDLOCAL = "EVENT_ENTRY_DESTROYEDLOCAL";
  public static String EVENT_ENTRY_PUTIFABSENT = "EVENT_ENTRY_PUTIFABSENT";
  public static String EVENT_ENTRY_REMOVED = "EVENT_ENTRY_REMOVED";
  public static String EVENT_ENTRY_REPLACED = "EVENT_ENTRY_REPLACED";

}
