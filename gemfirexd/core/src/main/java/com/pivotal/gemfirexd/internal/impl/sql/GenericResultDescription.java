/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.GenericResultDescription

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

/*
 * Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

package com.pivotal.gemfirexd.internal.impl.sql;



import com.gemstone.gnu.trove.TObjectIntHashMap;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatIdUtil;
import com.pivotal.gemfirexd.internal.iapi.services.io.Formatable;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultColumnDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultDescription;
import com.pivotal.gemfirexd.internal.iapi.util.ReuseFactory;
import com.pivotal.gemfirexd.internal.iapi.util.StringUtil;
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * GenericResultDescription: basic implementation of result
 * description, used in conjunction with the other 
 * implementations in this package.  This implementation 
 * of ResultDescription may be used by anyone.
 *
 */
public final class GenericResultDescription
	implements ResultDescription, Formatable
{

	/********************************************************
	**
	**	This class implements Formatable. That means that it
	**	can write itself to and from a formatted stream. If
	**	you add more fields to this class, make sure that you
	**	also write/read them with the writeExternal()/readExternal()
	**	methods.
	**
	**	If, inbetween releases, you add more fields to this class,
	**	then you should bump the version number emitted by the getTypeFormatId()
	**	method.
	**
	********************************************************/

	private ResultColumnDescriptor[] columns;
	private String statementType;
    
    /**
     * Saved JDBC ResultSetMetaData object.
     * @see ResultDescription#setMetaData(java.sql.ResultSetMetaData)
     */
    private transient ResultSetMetaData metaData;
    
    /**
     * A map which maps a column name to a column number.
     * Entries only added when accessing columns with the name.
     * It is OK not to sycnhronize this Map during get  even though it is being populated 
     * via setColumnDescriptor because  the activation instance itself gets created in 
     * a synchronized manner from the activation class & there are synchronization points
     * in the GenericStatement class also which will ensure that data populated in the Map
     * by a thread will be subsequently visible to other threads. 
     */
    private transient  final TObjectIntHashMap columnNameMap;
    private transient boolean qualifiedNamesPopulated;
	
	/**
	 * Niladic constructor for Formatable
	 */
	public GenericResultDescription()
	{
	  columnNameMap =  null;
	}

	/**
	 * Build a GenericResultDescription from columns and type
	 *
	 * @param columns an array of col descriptors
	 * @param statementType the type
	 */
	public GenericResultDescription(ResultColumnDescriptor[] columns, 
					String statementType) 
	{
		this.columns = columns;
		this.statementType = statementType;
//                GemStone changes BEGIN
		if(columns != null && columns.length > 0) {
                  this.columnNameMap = new TObjectIntHashMap(columns.length);
                  // populate the map here
                  for (int index = 0; index < this.columns.length; ++index) {
                    setColumnDescriptor(index, columns[index]);
                  }
		}else {
                  this.columnNameMap = null;
		}
//                GemStone changes END
    
	}

	//
	// ResultDescription interface
	//
	/**
	 * @see ResultDescription#getStatementType
	 */
	public String	getStatementType() {
		return statementType;
	}

	/**
	 * @see ResultDescription#getColumnCount
	 */
	public int	getColumnCount() 
	{
		return (columns == null) ? 0 : columns.length;
	}

	public ResultColumnDescriptor[] getColumnInfo() {
		return columns;
	}

	/**
	 * position is 1-based.
	 * @see ResultDescription#getColumnDescriptor
	 */
	public ResultColumnDescriptor getColumnDescriptor(int position) {
		return columns[position-1];
	}


	//////////////////////////////////////////////
	//
	// FORMATABLE
	//
	//////////////////////////////////////////////
	/**
	 * Write this object out
	 *
	 * @param out write bytes here
	 *
 	 * @exception IOException thrown on error
	 */
	public void writeExternal(ObjectOutput out) throws IOException
	{
		int len = (columns == null) ? 0 : columns.length;

		out.writeObject(statementType);
		out.writeInt(len);
		while(len-- > 0)
		{
			/*
			** If we don't have a GenericColumnsDescriptor, 
			** create one now and use that to write out.
			** Do this to avoid writing out query tree
			** implementations of ResultColumnDescriptor
			*/
			if (!(columns[len] instanceof 
						GenericColumnDescriptor))
			{
				columns[len] = new GenericColumnDescriptor(columns[len]);
			}
			out.writeObject(columns[len]);
		}
	}

	/**
	 * Read this object from a stream of stored objects.
	 *
	 * @param in read this.
	 *
	 * @exception IOException					thrown on error
	 * @exception ClassNotFoundException		thrown on error
	 */
	public void readExternal(ObjectInput in)
		throws IOException, ClassNotFoundException
	{
		int len;

		columns = null;
		statementType = (String)in.readObject();
		len = in.readInt();
		if (len > 0)
		{
			columns = new GenericColumnDescriptor[len];
			while(len-- > 0)
			{
				columns[len] = (ResultColumnDescriptor)in.readObject();
			}
		}
	}
	
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int getTypeFormatId()	{ return StoredFormatIds.GENERIC_RESULT_DESCRIPTION_V01_ID; }


	
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			StringBuilder colStr = new StringBuilder();
			for (int i = 0; i < columns.length; i++)
			{
				colStr.append("column["+i+"]\n");
				colStr.append(columns[i].toString());
			}	
			return "GenericResultDescription\n" +
					"\tStatementType = "+statementType+"\n" +
					"\tCOLUMNS\n" + colStr.toString();
		}
		else
		{
			return "";
		}
	}

    /**
     * Set the meta data if it has not already been set.
     */
    public synchronized void setMetaData(ResultSetMetaData rsmd) {
        if (metaData == null)
            metaData = rsmd;
    }

    /**
     * Get the saved meta data.
     */
    public synchronized ResultSetMetaData getMetaData() {
        return metaData;
    }

    /**
     * Find a column name based upon the JDBC rules for
     * getXXX and setXXX. Name matching is case-insensitive,
     * matching the first name (1-based) if there are multiple
     * columns that map to the same name.
     */
    public synchronized int findColumnInsensitive(String columnName) {
//    GemStone changes BEGIN
        /*final Map workMap; 
        
        synchronized (this) {
            if (columnNameMap==null) {
                // updateXXX and getXXX methods are case insensitive and the 
                // first column should be returned. The loop goes backward to 
                // create a map which preserves this property.
                Map map = new HashMap();
                for (int i = getColumnCount(); i>=1; i--) {
                    
                    final String key = StringUtil.
                        SQLToUpperCase(
                            getColumnDescriptor(i).getName());
                    
                    final Integer value = ReuseFactory.getInteger(i);
                    
                    map.put(key, value);
                }
                
                // Ensure this map can never change.
                columnNameMap = Collections.unmodifiableMap(map);
            }
            workMap = columnNameMap;
        }*/

        int val = this.columnNameMap.get(columnName);
        if (val > 0) {
          return val;
        }
        String ucolumnName = StringUtil.SQLToUpperCase(columnName);
        val = this.columnNameMap.get(ucolumnName);
        if (val > 0) {
          return val;
        }
        // check qualified names too
        if (populateQualifiedNames()) {
          val = this.columnNameMap.get(columnName);
          if (val > 0) {
            return val;
          }
          val = this.columnNameMap.get(ucolumnName);
          if (val > 0) {
            return val;
          }
        }
        return -1;
    }

  public void setColumnDescriptor(int index, ResultColumnDescriptor rcd) {
    // TODO: see when rcd.getName() returns null -- should not happen in
    // SingleTablePredicatesDUnit
    if (rcd != null && rcd.getName() != null) {
      this.columns[index] = rcd;
      final String columnName = rcd.getName();
      if (columnName != null) {
        this.columnNameMap.putIfAbsent(columnName, index + 1, 0);
      }
    }
  }

  private boolean populateQualifiedNames() {
    if (this.columns != null && !this.qualifiedNamesPopulated) {
      int columnPos = 1;
      String columnName;
      for (ResultColumnDescriptor rcd : this.columns) {
        columnName = rcd.getName();
        if (columnName != null) {
          // put prefixed with tableName and schemaName (#41272)
          String tableName = rcd.getSourceTableName();
          if (tableName != null && tableName.length() > 0) {
            this.columnNameMap.putIfAbsent(tableName + '.' + columnName,
                columnPos, 0);
            String schemaName = rcd.getSourceSchemaName();
            if (schemaName != null && schemaName.length() > 0) {
              this.columnNameMap.putIfAbsent(schemaName + '.' + tableName + '.'
                  + columnName, columnPos, 0);
            }
          }
        }
        columnPos++;
      }
      this.qualifiedNamesPopulated = true;
      return true;
    }
    else {
      return false;
    }
  }
//  GemStone changes END
}

