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
package com.company.data;

import com.gemstone.gemfire.cache.*;

/**
 * A <code>CacheLoader</code> that is <code>Declarable</code>
 *
 * @author David Whitlock
 * @since 3.2.1
 */
public class DatabaseLoader implements CacheLoader, Declarable {

  public Object load(LoaderHelper helper)
    throws CacheLoaderException {

    throw new UnsupportedOperationException("I do NOTHING");
  }

  public void init(java.util.Properties props) {

  }

  public void close() {

  }

}
