/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
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
package gfxdperf.ycsb.core;

import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * A layer for accessing a database to be benchmarked. Each thread in the client
 * will be given its own instance of whatever DB class is to be used in the test.
 * This class should be constructed using a no-argument constructor, so we can
 * load it dynamically. Any argument-based initialization should be
 * done by init().
 * 
 * Note that YCSB does not make any use of the return codes returned by this class.
 * Instead, it keeps a count of the return values and presents them to the user.
 * 
 * The semantics of methods such as insert, update and delete vary from database
 * to database.  In particular, operations may or may not be durable once these
 * methods commit, and some systems may return 'success' regardless of whether
 * or not a tuple with a matching key existed before the call.  Rather than dictate
 * the exact semantics of these methods, we recommend you either implement them
 * to match the database's default semantics, or the semantics of your 
 * target application.  For the sake of comparison between experiments we also 
 * recommend you explain the semantics you chose when presenting performance results.
 */
public class DB
{
	/**
	 * Properties for configuring this DB.
	 */
	Properties _p=new Properties();

	/**
	 * Set the properties for this DB.
	 */
	public void setProperties(Properties p)
	{
		_p=p;

	}

	/**
	 * Get the set of properties for this DB.
	 */
	public Properties getProperties()
	{
		return _p; 
	}

	/**
	 * Initialize any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	public void init() throws DBException
	{
	}

	/**
	 * Cleanup any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	public void cleanup() throws DBException
	{
	}

	/**
	 * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to read.
	 * @param fields The list of fields to read, or null for all of them
	 * @param result A HashMap of field/value pairs for the result
	 * @return Zero on success, a non-zero error code on error or "not found".
	 */
	public int read(String table, String key, Set<String> fields, HashMap<String,ByteIterator> result) {
          throw new UnsupportedOperationException();
        }

	/**
	 * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
	 *
	 * @param table The name of the table
	 * @param startkey The record key of the first record to read.
	 * @param recordcount The number of records to read
	 * @param fields The list of fields to read, or null for all of them
	 * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String,ByteIterator>> result) {
          throw new UnsupportedOperationException();
        }
	
	/**
	 * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
	 * record key, overwriting any existing values with the same field name.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to write.
	 * @param values A HashMap of field/value pairs to update in the record
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public int update(String table, String key, HashMap<String,ByteIterator> values) {
          throw new UnsupportedOperationException();
        }

	/**
	 * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
	 * record key.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to insert.
	 * @param values A HashMap of field/value pairs to insert in the record
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public int insert(String table, String key, HashMap<String,ByteIterator> values) {
          throw new UnsupportedOperationException();
        }

	/**
	 * Delete a record from the database. 
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to delete.
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public int delete(String table, String key) {
          throw new UnsupportedOperationException();
        }
	
	/**
         * Queries the database for records that match:
	 * <ul>
	 *  <li>field0 = field0value
	 *  <li>field2 > field2start
	 *  <li>field2 < field2end
	 * </ul>
	 * 
	 * @param table The name of the table
	 * @param field0 the name of the first field
	 * @param field2 the name of the second field
	 * @param field0value the field1 value
	 * @param field2start the field2 start value
	 * @param field2end the field2 end value
	 * @param recordcount the number of records to read
	 * @param result the map of field/values for each record
         * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public int queryWithFilter(String table, String field0, String field2, 
            String field0value, long field2start, long field2end, int recordcount, 
            Vector<HashMap<String,ByteIterator>> result) {
          throw new UnsupportedOperationException();
	}
	
	/**
         * Queries the database for records that match 
         * <ul>
         *  <li>field0 = field0value
         *  <li>field2 > field2start
         *  <li>field2 < field2end
         *  <li>
         * </ul>
         * 
         * while grouping and ordering the results by field1 and summing on field3.
         * 
         * @param table The name of the table
         * @param field0 the name of the first field
         * @param field1 the name of the second field
         * @param field0value the field0 value
         * @param field2start the field1 start value
         * @param field2end the field1 end value
         * @param recordcount the number of records to read
         * @param result the map of field/values for each record
         * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
         */
        public int queryWithAggregate(String table, String field0, String field1, 
            String field2, String field3, String field0value, long field2start, 
            long field2end, int recordcount, Vector<HashMap<String,ByteIterator>> result) {
          throw new UnsupportedOperationException();
        }

        /**
         * Queries the database for records that match 
         * <ul>
         *  <li>field2 > field2start
         *  <li>field2 < field2end
         * </ul>
         * 
         * while joining the table to itself on field1.
         * 
         * @param table The name of the table
         * @param table2 the name of the 2nd table
         * @param field0value the field0 value
         * @param field2start the field2 start value
         * @param field2end the field2 end value
         * @param recordcount the number of records to read
         * @param result the map of field/values for each record
         * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
         */
        public int queryWithJoin(String table, String table2, String field0value, 
            long field2start, long field2end, int recordcount, 
            Vector<HashMap<String,ByteIterator>> result) {
          throw new UnsupportedOperationException();
        }
}
