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
package com.gemstone.gemfire.cache30;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;

/**
 * Tests the declarative caching functionality introduced in the GemFire
 * 5.0 (i.e. congo1). Don't be confused by the 45 in my name :-)
 * 
 * @author Mitch Thomas
 * @since 5.0
 */

public class CacheXml55Test extends CacheXml51Test
{

  // ////// Constructors

  public CacheXml55Test(String name) {
    super(name);
  }

  // ////// Helper methods

  protected String getGemFireVersion()
  {
    return CacheXml.VERSION_5_5;
  }

  /**
   * Tests that a region created with a named attributes has the correct
   * attributes.
   */
  public void testEmpty() throws CacheException
  {}

}
