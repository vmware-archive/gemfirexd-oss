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
package com.gemstone.gemfire.admin;

/**
 * Configuration for a GemFire cache server that is managed by the
 * administration API.  The cache server may or may not be running.
 *
 * @see AdminDistributedSystem#addCacheServer()
 *
 * @author David Whitlock
 * @since 4.0
 * @deprecated as of 5.7 use {@link CacheVmConfig} instead.
 */
@Deprecated
public interface CacheServerConfig extends CacheVmConfig {
  /**
   * Returns the <code>cache.xml</code> declarative caching
   * initialization file used to configure this cache server VM.  By
   * default, a cache server VM is started without an XML file.
   */
  public String getCacheXMLFile();

  /**
   * Sets the <code>cache.xml</code> declarative caching
   * initialization file used to configure this cache server VM.
   */
  public void setCacheXMLFile(String cacheXml);

  /**
   * Returns the location(s) of user classes (such as cache loaders)
   * required by the cache server VM.
   */
  public String getClassPath();

  /**
   * Sets the location(s) of user classes (such as cache loaders)
   * required by the cache server VM.
   */
  public void setClassPath(String classpath);
}
