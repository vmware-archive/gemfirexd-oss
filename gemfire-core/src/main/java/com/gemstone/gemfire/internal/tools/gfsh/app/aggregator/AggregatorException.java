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
package com.gemstone.gemfire.internal.tools.gfsh.app.aggregator;

/**
 * AggregatorException is thrown by Aggregator if there is
 * any error related to the aggregator.
 * 
 * @author dpark
 *
 */
public class AggregatorException extends Exception
{
	private static final long serialVersionUID = 1L;
	
	private Throwable functionExceptions[];
	
    public AggregatorException()
    {
        super();
    }
    public AggregatorException(String message)
    {
        super(message);
    }

    public AggregatorException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public AggregatorException(Throwable cause)
    {
        super(cause);
    }
    
    public AggregatorException(String message, Throwable functionExceptions[])
    {
    	super(message);
    	this.functionExceptions = functionExceptions;
    }
    
    /**
     * The exception caught in AggregateFunction.run().
     * @return exception caught in AggregateFunction.run()
     */
    public Throwable[] getFunctionExceptions()
    {
    	return functionExceptions;
    }
}

