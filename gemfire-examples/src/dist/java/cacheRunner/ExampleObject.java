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
/*
 *  Code Generation by gfgen 
 *
 */

package cacheRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.Instantiator;
import com.gemstone.gemfire.cache.Declarable;

/** Example Object For Java/C Caching */
public class ExampleObject implements DataSerializable, Declarable {

    private double doubleField;

    private long longField;

    private float floatField;

    private int intField;

    private short shortField;

    private java.lang.String stringField;
    
    private List<String> stringListField;

    static {
		Instantiator.register(new Instantiator(ExampleObject.class, (byte) 46) {
 		        @Override
			public DataSerializable newInstance() {
				return new ExampleObject();
			}
		});
	}

    public ExampleObject( ) {
        this.doubleField = 0.0D;
        this.longField = 0L;
        this.floatField = 0.0F;
        this.intField = 0;
        this.shortField = 0;
        this.stringField = null;
        this.stringListField = null;
    }

    public ExampleObject(int id) {
    	this.intField = id;
    	this.stringField = String.valueOf(id);
    	this.shortField = Short.parseShort(stringField);
    	this.doubleField = Double.parseDouble(stringField);
    	this.floatField = Float.parseFloat(stringField);
		this.longField = Long.parseLong(stringField);
		this.stringListField = new ArrayList<String>(3);
		for (int i=0; i<3; i++) {
			this.stringListField.add(stringField);
		}
    }

    public ExampleObject(String id_str) {
    	this.intField = Integer.parseInt(id_str);
    	this.stringField = id_str;
    	this.shortField = Short.parseShort(stringField);
    	this.doubleField = Double.parseDouble(stringField);
    	this.floatField = Float.parseFloat(stringField);
		this.longField = Long.parseLong(stringField);
		this.stringListField = new ArrayList<String>(3);
		for (int i=0; i<3; i++) {
			this.stringListField.add(stringField);
		}
    }

    public Integer getKey() {
        return new Integer(intField);
    } 

    public double getDoubleField( ) {
        return this.doubleField;
    }

    public void setDoubleField( double doubleField ) {
        this.doubleField = doubleField;
    }

    public long getLongField( ) {
        return this.longField;
    }

    public void setLongField( long longField ) {
        this.longField = longField;
    }

    public float getFloatField( ) {
        return this.floatField;
    }

    public void setFloatField( float floatField ) {
        this.floatField = floatField;
    }

    public int getIntField( ) {
        return this.intField;
    }

    public void setIntField( int intField ) {
        this.intField = intField;
    }

    public short getShortField( ) {
        return this.shortField;
    }

    public void setShortField( short shortField ) {
        this.shortField = shortField;
    }

    public java.lang.String getStringField( ) {
        return this.stringField;
    }

    public void setStringField( java.lang.String stringField ) {
        this.stringField = stringField;
    }

    public List<String> getStringListField( ) {
        return this.stringListField;
    }

    public void setStringListField( Vector<String> stringListField ) {
        this.stringListField = stringListField;
    }

	public void toData(DataOutput out) throws IOException {
		out.writeDouble(doubleField);
		out.writeFloat(floatField);
		out.writeLong(longField);
		out.writeInt(intField);
		out.writeShort(shortField);
		out.writeUTF(stringField);
		out.writeInt(stringListField.size());
		for (int i=0; i<stringListField.size(); i++) {
			out.writeUTF(stringListField.get(i));
		}
	}

	public void fromData(DataInput in) throws IOException, ClassNotFoundException {
		this.doubleField = in.readDouble();
		this.floatField = in.readFloat();
		this.longField = in.readLong();
		this.intField = in.readInt();
		this.shortField = in.readShort();
		this.stringField = in.readUTF();
		this.stringListField = new Vector<String>();
		int size = in.readInt();
		for (int i=0; i<size; i++) {
			String s = in.readUTF();
			stringListField.add(i, s);
		}
	}
	
	@Override
	public int hashCode() {
		return this.intField;
	}
	
	@Override
	public boolean equals(Object eo) {
		if (!(eo instanceof ExampleObject)) return false;
		ExampleObject o = (ExampleObject)eo;
		if (this.doubleField != o.doubleField) return false;
		if (this.floatField != o.floatField) return false;
		if (this.longField != o.longField) return false;
		if (this.intField != o.intField) return false;
		if (this.shortField != o.shortField) return false;
		if (!this.stringField.equals(o.stringField)) return false;
		if (!this.stringListField.equals(o.stringListField)) return false;
		return true;
	}

	public void init(Properties pros) {
    	this.stringField = pros.getProperty("id");
    	this.intField = Integer.parseInt(stringField);
    	this.shortField = Short.parseShort(stringField);
    	this.doubleField = Double.parseDouble(stringField);
    	this.floatField = Float.parseFloat(stringField);
		this.longField = Long.parseLong(stringField);
		this.stringListField = new ArrayList(3);
		for (int i=0; i<3; i++) {
			this.stringListField.add(stringField);
		}
    }

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(this.getClass().getName());
		sb.append(": \"");
		sb.append(this.getDoubleField());
		sb.append("\"(double)");
		sb.append(" \"");
		sb.append(this.getLongField());
		sb.append("\"(long)");
		sb.append(" \"");
		sb.append(this.getFloatField());
		sb.append("\"(float)");
		sb.append(" \"");
		sb.append(this.getIntField());
		sb.append("\"(int)");
		sb.append(" \"");
		sb.append(this.getShortField());
		sb.append("\"(short)");
		sb.append(" \"");
		sb.append(this.getStringField());
		sb.append("\"(string)");
		sb.append(" \"");
		List<String> v = this.getStringListField();
		sb.append("\"");
		sb.append(v.toString());
		sb.append("\"(String Vector)");
		return sb.toString();
	}
}
