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

package com.gemstone.gemfire.cache.query.types;


/**
 * Describes the field names and types for each field in a {@link
 * com.gemstone.gemfire.cache.query.Struct}.
 *
 * @author Eric Zoerner
 * @since 4.0
 */
public interface StructType extends ObjectType {
  
  /**
   * The the types of the fields for this struct
   * @return the array of Class for the fields
   */
  ObjectType[] getFieldTypes();

  /**
   * Get the names of the fields for this struct
   * @return the array of field names
   */
  String[] getFieldNames();

  /**
   * Returns the index of the field with the given name in this
   * <code>StructType</code>. 
   *
   * @throws IllegalArgumentException
   *         If this <code>StructType</code> does not contain a field
   *         named <code>fieldName</code>.
   */
  public int getFieldIndex(String fieldName);
  
}
