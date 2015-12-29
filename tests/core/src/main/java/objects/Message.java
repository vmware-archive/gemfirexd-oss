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

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
//import com.gemstone.gemfire.Instantiator;
import com.gemstone.gemfire.cache.util.ObjectSizer;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.util.Sizeof;
import hydra.*;
import java.io.*;

/**
 *  A message containing a timestamp and a string of configurable size.
 */

public class Message implements ConfigurableObject, TimestampedObject, ObjectSizer, DataSerializable
{
  private long timestamp;
  private String content;

    // INSTANTIATORS DISABLED due to bug 35646
    //
  //static {
  //  Instantiator.register(new Instantiator(Message.class, (byte)17) {
  //    public DataSerializable newInstance() {
  //      return new Message();
  //    }
  //  });
  //}

  public Message() {
  }
  public void init( int index ) {
    int size = MessagePrms.getSize();
    if ( size == 0 ) {
      this.content = null;
    } else {
      StringBuffer buf = new StringBuffer( size );
      buf.insert( 0, (double) index );
      int padding = size - buf.length();
      if ( padding < 0 ) {
        throw new ObjectCreationException( "Unable to encode index " + index + " into string of size " + size );
      }
      char[] c = new char[ padding ];
      for ( int i = 0; i < padding; i++ ) {
        c[i] = '0';
      }
      buf.append( c );
      this.content = buf.toString();
    }
    this.timestamp = NanoTimer.getTime() - RemoteTestModule.getClockSkew();
  }
  public int getIndex() {
    if ( this.content == null ) {
      throw new ObjectAccessException( "No index is encoded when " + BasePrms.nameForKey( MessagePrms.size ) + " is 0" );
    } else {
      int marker = this.content.indexOf( "." );
      String index = this.content.substring( 0, marker );
      try {
        return ( new Integer( index ) ).intValue();
      } catch( NumberFormatException e ) {
        throw new ObjectAccessException( this.content + " does not contain an encoded integer index" );
      }
    }
  }
  public void validate( int index ) {
    if ( this.content == null ) {
      Log.getLogWriter().info( "Cannot validate encoded index of object " + index + ", it has no content" );
    } else {
      int encodedIndex = this.getIndex();
      if ( encodedIndex != index ) {
        throw new ObjectValidationException( "Expected index " + index + ", got " + encodedIndex );
      }
    }
  }
  public long getTimestamp() {
    return this.timestamp;
  }
  public void resetTimestamp() {
    this.timestamp = NanoTimer.getTime() - RemoteTestModule.getClockSkew();
  }
  public String toString() {
    if ( this.content == null ) {
      return "Message@" + this.timestamp;
    } else {
      return "Message(" + this.getIndex() + ")@" + this.timestamp;
    }
  }

  /**
   * Two <code>Message</code>s are considered to be equal if they have
   * the same content ({@linkplain #getIndex index}) and timestamp.
   * This provides stronger validation than the {@link #validate}
   * method that only considers the index.
   *
   * @since 3.5
   */
  public boolean equals(Object o) {
    if (o instanceof Message) {
      Message other = (Message) o;
      if (this.timestamp == other.timestamp) {
        if (this.content != null) {
          return this.content.equals(other.content);

        } else {
          return other.content == null;
        }
      }
    }

    return false;
  }

  // ObjectSizer
  public int sizeof(Object o) {
    if (o instanceof Message) {
      Message obj = (Message)o;
      return Sizeof.sizeof(obj.timestamp)
                   + Sizeof.sizeof(obj.content);
    } else {
      return Sizeof.sizeof(o);
    }
  }

  // DataSerializable

  public void toData( DataOutput out )
  throws IOException {
    out.writeLong( this.timestamp );
    DataSerializer.writeString( this.content, out );
  }
  public void fromData( DataInput in )
  throws IOException, ClassNotFoundException {
    this.timestamp = in.readLong();
    this.content = DataSerializer.readString( in );
  }
}
