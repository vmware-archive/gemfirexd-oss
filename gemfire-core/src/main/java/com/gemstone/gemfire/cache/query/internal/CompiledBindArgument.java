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

package com.gemstone.gemfire.cache.query.internal;

import java.util.*;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.pdx.internal.PdxString;
import com.gemstone.gemfire.cache.query.AmbiguousNameException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.TypeMismatchException;

/**
 * Class Description
 *
 * @author      ezoerner
 */


public class CompiledBindArgument extends AbstractCompiledValue {
  private int index; // one-based

    public CompiledBindArgument(int index) {
      this.index = index;
    }

  public int getType() {
    return QUERY_PARAM;
  }

  @Override
  public void generateCanonicalizedExpression(StringBuffer clauseBuffer,
      ExecutionContext context) throws AmbiguousNameException,
      TypeMismatchException, NameResolutionException {
    Object rgn;
    if ((rgn = context.getBindArgument(this.index)) instanceof Region) {
      clauseBuffer.insert(0, ((Region)rgn).getFullPath());
    }
    else {
      super.generateCanonicalizedExpression(clauseBuffer, context);
    }
  }
    
    public Object evaluate(ExecutionContext context) {
      Object obj = context.getBindArgument(this.index);
      // check for BucketRegion substitution
      if (obj instanceof Region) {
        PartitionedRegion pr = context.getPartitionedRegion();
        if (pr != null) {
          if (pr.getFullPath().equals(((Region)obj).getFullPath())) {
            obj = context.getBucketRegion();
          }
        }
      }
      return obj;
    }
    
    /*
     * provided just the bind parameters, we can evaluate if the expected
     * parameter is all that is needed.  For example a bound limit variable
     */
    public Object evaluate(Object[] bindArguments) {
        if (index > bindArguments.length) {
            throw new IllegalArgumentException(LocalizedStrings.ExecutionContext_TOO_FEW_QUERY_PARAMETERS.toLocalizedString());
        }
        return bindArguments[index - 1];
    }
    
    @Override
    public void getRegionsInQuery(Set regionsInQuery, Object[] parameters) {
      Object v = parameters[this.index - 1];
      if (v instanceof Region) {
        regionsInQuery.add(((Region)v).getFullPath());
      }
    }
    
    public PdxString getSavedPdxString(ExecutionContext context){
      return context.getSavedPdxString(this.index);
    }
}
