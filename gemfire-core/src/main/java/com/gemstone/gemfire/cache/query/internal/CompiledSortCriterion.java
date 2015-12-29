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
import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.TypeMismatchException;

/**
 * This class represents a compiled form of sort criterian present in order by clause
 * @author Yogesh Mahajan
 */
public class CompiledSortCriterion extends AbstractCompiledValue  {
	//Asif: criterion true indicates descending order
	boolean criterion = false;	
	CompiledValue expr = null;
  
  @Override
  public List getChildren() {
    return Collections.singletonList(this.expr);
  }
  
	/**
	 * @return int
	 */
	public int getType()
    {
        return SORT_CRITERION;
    }
	/** evaluates sort criteria in order by clause 
	 * @param context
	 * @return Object
	 * @throws FunctionDomainException
	 * @throws TypeMismatchException
	 * @throws NameResolutionException
	 * @throws QueryInvocationTargetException
	 */
	public Object evaluate(ExecutionContext context)throws FunctionDomainException, TypeMismatchException,
    NameResolutionException, QueryInvocationTargetException 
	{
		return this.expr.evaluate(context);  
		 
	}
	/**
	 * concstructor
	 * @param criterion
	 * @param cv
	 */
	CompiledSortCriterion(boolean criterion, CompiledValue cv) {
	  this.expr = cv;
	  this.criterion = criterion;
	}
	
  public boolean getCriterion() {
    return criterion;
  }

  public CompiledValue getExpr() {
    return expr;
  }
	
}
