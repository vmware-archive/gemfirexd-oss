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
 *  A nested vector of orders of configurable width and depth, which
 *  implements Sizeable for better use with a memory evictor
 *  (requires that <code>Order<code>s also implement Sizeable)
 */

public class NestedOrderVector extends Vector implements ConfigurableObject, Sizeable {

  int orderID;
  int bytes = 4 /* bytes */ 
    + 4 /* orderid */
    +  4 + 4 + 48 /* vector overhead */;

  public NestedOrderVector() {
    super();
  }
  private NestedOrderVector( int index ) {
    super();
    this.orderID = index;
  }
  public void init( int index ) {
    this.orderID = index;
    int width = NestedOrderVectorPrms.getWidth();
    int depth = NestedOrderVectorPrms.getDepth();
    for ( int i = 0; i < width; i++ ) {
      this.add( getNode( index, width, depth - 1 ) );
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

  private static Object getNode( int index, int width, int depth ) {
    if ( depth == 0 ) {
      Order order = new Order();
      order.init( index );
      return order;
    } else {
      NestedOrderVector orders = new NestedOrderVector( index );
      for ( int i = 0; i < width; i++ ) {
        orders.add( getNode( index, width, depth - 1 ) );
      }
      return orders;
    }
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
      if ( obj instanceof NestedOrderVector ) {
        ObjectHelper.validate( index, obj );
      } else if ( obj instanceof Order ) {
        ObjectHelper.validate( index, obj );
      } else {
        throw new ObjectValidationException( "Unexpected type: " + obj.getClass().getName() );
      }
    }
  }
}
