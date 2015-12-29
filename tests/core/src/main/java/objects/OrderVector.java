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

package objects;

import java.util.*;
import com.gemstone.gemfire.internal.cache.lru.Sizeable;

/**
 *  A vector of orders of configurable size, which implements Sizeable
 *  for better use with a memory evictor.
 * 
 */

public class OrderVector extends Vector implements ConfigurableObject, Sizeable {

  int orderID;
  int bytes = 4 /* bytes */ 
    + 4 /* orderId */
    + 4 + 4 + 48 /* vector overhead */;

  public OrderVector() {
    super();
  }
  public void init( int index ) {
    this.orderID = index;
    int size = OrderVectorPrms.getSize();
    for ( int i = 0; i < size; i++ ) {
      Order order = new Order();
      order.init( index );
      this.add( order );
    }
  }

  public boolean add(Object o) {
    if (o instanceof Sizeable) {
      this.bytes += ((Sizeable) o).getSizeInBytes();
    }
    return super.add(o);
  }

  public boolean remove(Object o) {
    if (o instanceof Sizeable) {
      this.bytes -= ((Sizeable) o).getSizeInBytes();
    }
    return super.remove(o);
  }

  public int getSizeInBytes() {
    return this.bytes + (elementData.length * 4) /* vector storage overhead */;
  }

  public int getIndex() {
    return this.orderID;
  }
  public void validate( int index ) {
    int encodedIndex = this.getIndex();
    if ( encodedIndex != index ) {
      throw new ObjectValidationException( "Expected index " + index + ", got " + encodedIndex );
    }
    for ( Iterator i = this.iterator(); i.hasNext(); ) {
      Object obj = i.next();
      if ( obj instanceof Order ) {
        ObjectHelper.validate( index, obj );
      } else {
        throw new ObjectValidationException( "Unexpected type: " + obj.getClass().getName() );
      }
    }
  }
}
