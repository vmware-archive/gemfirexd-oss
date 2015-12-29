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

import hydra.BasePrms;

/**
 *  A string of configurable size.
 */

public class SizedString {

  private static final String STRDATA = "abcdefghij";

  public static String init( int index ) {
    boolean constant = SizedStringPrms.getConstant();
    if ( constant ) {
      return new String( STRDATA );
    }
    int size = SizedStringPrms.getSize();
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
    return buf.toString();
  }
  public static int getIndex( String str ) {
    int marker = str.indexOf( "." );
    if ( marker == -1 ) {
      throw new ObjectAccessException( "No index is encoded when " + BasePrms.nameForKey( SizedStringPrms.constant ) + " is true" );
    }
    String index = str.substring( 0, marker );
    try {
      return ( new Integer( index ) ).intValue();
    } catch( NumberFormatException e ) {
      throw new ObjectAccessException( str + " does not contain an encoded integer index" );
    }
  }
  public static void validate( int index, String str ) {
    if ( SizedStringPrms.getConstant() ) {
      if ( ! str.equals( STRDATA ) ) {
        throw new ObjectValidationException( "Expected " + STRDATA + ", got " + str );
      }
    } else {
      int encodedIndex = getIndex( str );
      if ( encodedIndex != index ) {
        throw new ObjectValidationException( "Expected index " + index + ", got " + encodedIndex );
      }
    }
  }
}
