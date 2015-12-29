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
package com.pivotal.gemfirexd.internal.engine.distributed.metadata;

import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.RegionKey;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;

/**
 * 
 * Represents the dynamic Primary or CompositeKey of the system. To obtain the
 * actual key , this class uses the Parameter Data present in Activation object
 * to evalaute the Key
 * 
 * @author Asif
 * @since Sql Fabric
 * @see PrimaryDynamicKey
 * @see CompositeDynamicKey
 * 
 */
public interface DynamicKey {

  public RegionKey getEvaluatedPrimaryKey(Activation activation,
      GemFireContainer container, boolean doClone) throws StandardException;

  public Object getEvaluatedIndexKey(Activation activation)
      throws StandardException;
}
