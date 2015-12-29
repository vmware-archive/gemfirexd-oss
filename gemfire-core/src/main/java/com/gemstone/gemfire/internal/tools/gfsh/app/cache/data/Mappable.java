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
package com.gemstone.gemfire.internal.tools.gfsh.app.cache.data;

import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.DataSerializable;

public interface Mappable extends DataSerializable
{
	public void put(String key, Mappable mappable);
	public void put(String key, Listable listable);
	public void put(String key, String value);
	public void put(String key, boolean value);
	public void put(String key, byte value);
	public void put(String key, short value);
	public void put(String key, int value);
	public void put(String key, long value);
	public void put(String key, float value);
	public void put(String key, double value);
	public Object getValue(String key);
	public boolean getBoolean(String key) throws NoSuchFieldException, InvalidTypeException;
	public byte getByte(String name) throws NoSuchFieldException, InvalidTypeException;
	public char getChar(String key) throws NoSuchFieldException, InvalidTypeException;
	public short getShort(String key) throws NoSuchFieldException, InvalidTypeException;
	public int getInt(String key) throws NoSuchFieldException, InvalidTypeException;
	public long getLong(String key) throws NoSuchFieldException, InvalidTypeException;
	public float getFloat(String key) throws NoSuchFieldException, InvalidTypeException;
	public double getDouble(String key) throws NoSuchFieldException, InvalidTypeException;
	public String getString(String key) throws NoSuchFieldException, InvalidTypeException;
	public boolean hasMappable();
	public boolean hasListable();
	public Object remove(String key);
	public int size();
	public Collection getValues();
	public Set getKeys();
	public Set getEntries();
	public Map.Entry[] getAllEntries();
	public Map.Entry[] getAllPrimitives();
	public int getPrimitiveCount();
	public Map.Entry[] getAllMappables();
	public Map.Entry[] getAllListables();
	public int getMappableCount();
	public int getListableCount();
	public void clear();
	public void dump(OutputStream out);
	public Object clone();
	
}
