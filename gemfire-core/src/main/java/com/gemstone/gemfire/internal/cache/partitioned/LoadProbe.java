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
package com.gemstone.gemfire.internal.cache.partitioned;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

/**
 * A probe which calculates the load of a PR in a given member. This class
 * is designed to be created in the member that is doing a rebalance operation
 * and sent to all of the data stores to gather their load. In the future, this
 * class or something like it may be exposed to customers to allow them to 
 * provide different methods for determining load.
 * @author dsmith
 *
 */
public interface LoadProbe extends DataSerializable {
  PRLoad getLoad(PartitionedRegion pr);
}
