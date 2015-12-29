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
package com.gemstone.gemfire.internal.tools.gfsh.aggregator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

/**
 * AggregateResults contains a data object set by AggregateFunction.run().
 * 
 * @author dpark
 */
public class AggregateResults implements DataSerializable
{

	private static final long serialVersionUID = 1L;
	
	public static final byte CODE_NORMAL = 0;
	public final static byte CODE_ERROR = -1;

	private byte code = CODE_NORMAL;
	private Object dataObject;
	private String codeMessage;
	private Throwable exception;
	
	/**
	 * Creates an empty AggregateResults object.
	 */
	public AggregateResults()
	{
	}
	
	/**
     * Creates a new AggregatedResults object with the data object
     * contains results.
     * @param dataObject The data object that contains the task results.
     */
    public AggregateResults(Object dataObject)
    {
    	this.dataObject = dataObject;
    }
    
    /**
     * Returns the data object set by AggregateFunction.
     */
    public Object getDataObject() {
		return dataObject;
	}
    
    /**
     * Sets the data object.
     * @param dataObject The data object representing the aggregated
     *                   results from one partition.
     */
    public void setDataObject(Object dataObject) 
    {
		this.dataObject = dataObject;
	}
	
	/**
     * Returns the code set by the AggregateFunction. It is typically
     * used for sending error code. The default value is 0.
     * @return code
     */
    public byte getCode() {
		return code;
	}

    /**
     * Sets code. 
     * @param code
     */
	public void setCode(byte code) {
		this.code = code;
	}

	/**
	 * Returns the message associated with the code. The default
	 * value is null.
	 */
	public String getCodeMessage() {
		return codeMessage;
	}

	/**
	 * Sets the code message.
	 * @param codeMessage
	 */
	public void setCodeMessage(String codeMessage) {
		this.codeMessage = codeMessage;
	}
	
	/**
	 * Returns the partition exception if any.
	 */
	public Throwable getException() {
		return exception;
	}

	/**
	 * Sets the partition exception.
	 * @param exception The exception caught in AggregateFunction.run().
	 */
	public void setException(Throwable exception) {
		this.exception = exception;
	}
	
	public void toData(DataOutput out) throws IOException
    {
    	out.writeByte(code);
    	DataSerializer.writeString(codeMessage, out);
        DataSerializer.writeObject(exception, out);
        DataSerializer.writeObject(dataObject, out);
    }

    public void fromData(DataInput in) throws IOException, ClassNotFoundException
    {
    	code = in.readByte();
    	codeMessage = DataSerializer.readString(in);
        exception = (Throwable)DataSerializer.readObject(in);
        dataObject = DataSerializer.readObject(in);
    }

}
