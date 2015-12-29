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

package com.gemstone.gemfire.cache.query.internal.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.types.*;

/**
 * Implementation of CollectionType
 * @author Eric Zoerner
 * @since 4.0
 */
public class CollectionTypeImpl extends ObjectTypeImpl implements CollectionType {
  private static final long serialVersionUID = 892402666471396897L;
  private ObjectType elementType;
  /**
   * Empty constructor to satisfy <code>DataSerializer</code> requirements
   */
  public CollectionTypeImpl() {
  }
  
  /** Creates a new instance of ObjectTypeImpl */
  public CollectionTypeImpl(Class clazz, ObjectType elementType) {
    super(clazz);
    this.elementType = elementType;
  }
  
  public CollectionTypeImpl(String className, ObjectType elementType)
  throws ClassNotFoundException {
    super(className);
    this.elementType = elementType;
  }
  
  @Override
  public boolean equals(Object obj) {
    return super.equals(obj) &&
            (obj instanceof CollectionTypeImpl) &&
            this.elementType.equals(((CollectionTypeImpl)obj).elementType);
  }
  
  @Override
  public int hashCode() {
    return super.hashCode() ^ this.elementType.hashCode();
  }
  
  @Override
  public String toString(){
    return resolveClass().getName() +
            "<" + this.elementType.resolveClass().getName() + ">";
  }
  
  public boolean allowsDuplicates() {
    Class cls = resolveClass();
    return !Set.class.isAssignableFrom(cls) &&
            !Map.class.isAssignableFrom(cls) &&
            !Region.class.isAssignableFrom(cls);
  }
  
  public ObjectType getElementType() {
    return this.elementType;
  }
  
  public boolean isOrdered() {
    Class cls = resolveClass();
    return List.class.isAssignableFrom(cls) ||
            SortedSet.class.isAssignableFrom(cls) ||
            cls.isArray() || LinkedHashSet.class.isAssignableFrom(cls);
  }

  @Override
  public boolean isCollectionType() { return true; }
  @Override
  public boolean isMapType() { return false; }
  @Override
  public boolean isStructType() { return false; }
  
  @Override
  public int getDSFID() {
    return COLLECTION_TYPE_IMPL;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.elementType = (ObjectType)DataSerializer.readObject(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
     super.toData(out);
    DataSerializer.writeObject(this.elementType, out);
  }
}
