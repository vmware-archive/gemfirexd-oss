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

package hydra;

import java.lang.reflect.*;
import java.util.*;
import java.util.zip.*;

/**
*
* A class used to support keys for hydra configuration settings.
* <p>
* To add test- or package-specific parameters, create a subclass with
* a <B>public</B> field for each parameter and a static initializer,
* like so:
* <pre>
*    package sampletest;
*
*    import hydra.BasePrms;
*
*    public class SamplePrms extends BasePrms {
*
*      public static Long sample1;
*      public static Long sample2;
*
*      static {
*          BasePrms.setValues( SamplePrms.class );
*      }
*    }
* </pre>
*
* <P>At runtime, the values of config settings can be accessed through
* the {@link hydra.ConfigHashtable} available from {@link
* hydra.TestConfig#getParameters()}.</P>
*
* @see hydra.ConfigHashtable#stringAt(Long)
*/

public class BasePrms {

  public static final String NONE = "none";
  public static final String DEFAULT = "default";

  private static Hashtable keymap = new Hashtable();
  private static Hashtable namemap = new Hashtable();

  protected static String DASH = "-";
  protected static void setValues( Class cls ) {
    String classname = cls.getName();
    Field[] fields = cls.getDeclaredFields();
    for ( int i = 0; i < fields.length; i++ ) {
      String fieldname = fields[i].getName();
      int m = fields[i].getModifiers();
      if ( Modifier.isPublic( m ) && ! Modifier.isFinal( m ) ) {
        try {
          String fullname = classname + DASH + fieldname;
          CRC32 checksum = new CRC32();
          checksum.update( fullname.getBytes() );
          Long val = new Long( checksum.getValue() );
          fields[i].setAccessible(true);
          fields[i].set( val, val );
          keymap.put( val, fullname );
          namemap.put( fullname, val );
          //Log.getLogWriter().finest( "assigned\t" + fullname + "\t" + val );
        } catch( IllegalArgumentException e ) {
          e.printStackTrace();
          throw new HydraRuntimeException( "Error accessing field: " + fields[i], e );
        } catch( IllegalAccessException e ) {
          e.printStackTrace();
          throw new HydraRuntimeException( "Error accessing field", e );
        }
      }
    }
  }

  public static String nameForKey( Long key ) {
    String name = (String) keymap.get( key );
    if ( name == null )
      throw new HydraInternalException( "No map entry for key: " + key );
      return name;
    }

  public static Long keyForName( String name ) {
    Long key = (Long) namemap.get( name );
    if ( key == null ) {
      // try reflecting it in
      StringTokenizer st = new StringTokenizer( name, DASH, false );
      // @todo lises check for proper name structure
      String classname = st.nextToken();
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      try {
        Class cls = Class.forName( classname, true, cl );
      } catch( ClassNotFoundException e ) {
        throw new HydraConfigException( "No map entry for name: " + name, e );
      }
      key = (Long) namemap.get( name );
      if ( key == null ) {
        throw new HydraConfigException( "No map entry for name: " + name );
      }
    }
    return key;
  }

  public static void dumpKeys() {
    Log.getLogWriter().info( "Dumping parameter map by request..." );
    Enumeration keys = keymap.keys();
    while ( keys.hasMoreElements() ) {
      Long key = (Long) keys.nextElement();
      String name = (String) keymap.get( key );
      Log.getLogWriter().info( "KEY:" + keyForName( name ) + "\tNAME:" + nameForKey( key ) );
    }
    Log.getLogWriter().info( "...end parameter map" );
  }

  public static ConfigHashtable tab() {
    return TestConfig.tab();
  }

  public static ConfigHashtable tasktab() {
    return TestConfig.tasktab();
  }
}
