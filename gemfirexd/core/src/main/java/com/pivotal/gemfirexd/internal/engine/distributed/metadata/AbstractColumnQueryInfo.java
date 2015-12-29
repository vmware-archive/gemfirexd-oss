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

import com.gemstone.gemfire.internal.cache.AbstractRegion;
import com.pivotal.gemfirexd.internal.engine.ddl.resolver.GfxdPartitionResolver;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TableDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;

/**
 * 
 * @author Asif
 *
 */
public interface AbstractColumnQueryInfo 
{  
  
  public abstract DataTypeDescriptor getType() ;  
  public GfxdPartitionResolver getResolverIfSingleColumnPartition();  
  boolean isExpression();
  public int getActualColumnPosition() ; 
  public String getActualColumnName() ;
  public TableDescriptor getTableDescriptor();
  
  
}

