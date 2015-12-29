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
package com.gemstone.gemfire.internal.tools.gfsh.app.command.task.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.cache.PartitionAttributes;

/**
 * A data class that contains partition region attribute information.
 * @author dpark
 *
 */
public class PartitionAttributeInfo implements DataSerializable
{
	private static final long serialVersionUID = 1L;
	
	private long versionId = serialVersionUID;

	private String regionPath;
	
	private int redundantCopies;
	private int totalNumBuckets;
    
    private List partitionList = new ArrayList();
    
    public PartitionAttributeInfo() {}
    
    public PartitionAttributeInfo(PartitionAttributes attr)
    {
    	
    }
    
    public void addPartition(Partition partition)
    {
    	partitionList.add(partition);
    }
    
    public List getPartitionList()
    {
    	return partitionList;
    }

	public String getRegionPath()
	{
		return regionPath;
	}

	public int getRedundantCopies()
	{
		return redundantCopies;
	}

	public int getTotalNumBuckets()
	{
		return totalNumBuckets;
	}

	public void fromData(DataInput in) throws IOException, ClassNotFoundException
	{
		versionId = in.readLong();
		
		regionPath = in.readUTF();
		redundantCopies = in.readInt();
		totalNumBuckets = in.readInt();
		
		partitionList = new ArrayList();
		int size = in.readInt();
		for (int i = 0; i < size; i++) {
			Partition part = new Partition();
			part.memberName = in.readUTF();
			part.localMaxMemory = in.readInt();
			part.toalMaxMemory = in.readLong();
			partitionList.add(part);
		}
	}

	public void toData(DataOutput out) throws IOException
	{
		out.writeLong(versionId);
		
		out.writeUTF(regionPath);
		out.writeInt(redundantCopies);
		out.writeInt(totalNumBuckets);
		
		int size = partitionList.size();
		out.writeInt(size);
		for (int i = 0; i < size; i++) {
			Partition part = (Partition)partitionList.get(i);
			out.writeUTF(part.memberName);
			out.writeInt(part.localMaxMemory);
			out.writeLong(part.toalMaxMemory);
		}
		
	}
	
	public static class Partition
    {
		public Partition() {}
		
    	private String memberName;
    	private int localMaxMemory ;
    	private long toalMaxMemory;
    	
		public String getMemberName()
		{
			return memberName;
		}
		
		public int getLocalMaxMemory()
		{
			return localMaxMemory;
		}
		
		public long getToalMaxMemory()
		{
			return toalMaxMemory;
		}
    }

}