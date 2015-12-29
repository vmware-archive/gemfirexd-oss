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
package com.gemstone.gemfire.internal.tools.gfsh.app.cache.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.tools.gfsh.app.misc.util.ReflectionUtil;


public class IndexInfo implements DataSerializable
{
	private static final long serialVersionUID = 1L;

	public int indexListSize;

	public int indexMapSize;

	public Object minSetQueryKey;
	public Object maxSetQueryKey;

	public int minSetSize = Integer.MAX_VALUE;
	public int maxSetSize = 0;

	public String toString()
	{
		return ReflectionUtil.toStringPublicMembers(this);
	}

	public void fromData(DataInput in) throws IOException,
			ClassNotFoundException
	{
		indexListSize = in.readInt();
		indexMapSize = in.readInt();
		minSetSize = in.readInt();
		maxSetSize = in.readInt();
		minSetQueryKey = DataSerializer.readObject(in);
		maxSetQueryKey = DataSerializer.readObject(in);
	}

	public void toData(DataOutput out) throws IOException
	{
		out.writeInt(indexListSize);
		out.writeInt(indexMapSize);
		out.writeInt(minSetSize);
		out.writeInt(maxSetSize);
		DataSerializer.writeObject(minSetQueryKey, out);
		DataSerializer.writeObject(maxSetQueryKey, out);
	}

}
