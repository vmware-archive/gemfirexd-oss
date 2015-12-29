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

package batterytest;

import hydra.*;

import java.io.*;
import java.util.*;

/**
 *  Contains a batterytest test configuration, consisting of a test name
 *  and optional system properties to use when running the test.
 */
public class BatteryTestConfig {

  // the test name
  private String name;

  // the test properties; must use ordered type to preserve parsing order
  private Vector<Property> properties;

  // whether the test has already been through the postprocessing step
  private boolean postprocessed = false;

  public BatteryTestConfig( String name ) {
    this.name = name;
  }
  public String getName() {
    return this.name;
  }
  protected Vector getProperties() {
    return this.properties;
  }
  public SortedMap getSortedProperties() {
    if ( this.properties == null ) {
      return null;
    }
    SortedMap map = new TreeMap();
    for ( Iterator i = this.properties.iterator(); i.hasNext(); ) {
      Property p = (Property) i.next();
      map.put( p.getKey(), p.getVal() );
    }
    return map;
  }
  protected void setProperties( Vector properties ) {
    this.properties = properties;
  }
  protected String getPropertyKeyString(String val) {
    if (val.substring(9).length() == 0) {
      if (this.properties == null) {
        return "none";
      } else {
        return getPropertyKeyString();
      }
    } else {
      if (this.properties == null) {
        String s = "No properties available to compute " + val;
        throw new HydraConfigException(s);
      } else {
        return getPropertyKeyStringFrom(val);
      }
    }
  }
  private String getPropertyKeyString() {
    StringBuffer buf = new StringBuffer();
    for (Property p : this.properties) {
      if (!p.getKey().equals("perffmwk.comparisonKey")) {
        if (buf.length() > 0) {
          buf.append("_");
        }
        buf.append(p.getVal());
      }
    }
    return buf.toString();
  }
  private String getPropertyKeyStringFrom(String propspec) {
    StringBuffer buf = new StringBuffer();
    StringTokenizer st = new StringTokenizer(propspec.substring(9), ":", false);
    boolean skipNextUnderscore = false;
    while (st.hasMoreTokens()) {
      String key = st.nextToken();
      String val = (String)getPropertyVal(key);
      if (val == null) { // treat as a literal
        //String s = "No property found for " + key + " in " + propspec
        //         + ", available properties are " + this.properties;
        //throw new HydraConfigException(s);
        val = key;
      }
      if (val.equals("x")) {
        skipNextUnderscore = true;
      } else if (skipNextUnderscore) {
        skipNextUnderscore = false;
      } else if (buf.length() > 0) {
        buf.append("_");
      }
      buf.append(val);
    }
    return buf.toString();
  }
  protected String getPropertyString() {
    StringBuffer buf = new StringBuffer();
    buf.append( " " );
    if ( this.properties != null ) {
      for ( Iterator i = this.properties.iterator(); i.hasNext(); ) {
        Property property = (Property) i.next();
	buf.append( "-D" + property.getKey() + "=" + property.getVal() + " " );
      }
    }
    return buf.toString();
  }
  protected Property getProperty( String key ) {
    if ( this.properties != null ) {
      for ( Iterator i = this.properties.iterator(); i.hasNext(); ) {
        Property p = (Property) i.next();
        if ( p.getKey().equals( key ) ) {
          return p;
        }
      }
    }
    return null;
  }
  protected Object getPropertyVal( String key ) {
    if ( this.properties == null ) {
      return null;
    }
    Property property = getProperty( key );
    if ( property == null ) {
      return null;
    }
    return property.getVal();
  }
  /**
   *  Reset a property to a new value, or add the property if not already there.
   */
  protected void resetProperty( String key, Object val ) {
    boolean reset = false;
    for ( Iterator i = this.properties.iterator(); i.hasNext(); ) {
      Property p = (Property) i.next();
      if ( p.getKey().equals( key ) ) {
        int index = this.properties.indexOf( p );
        p.setVal( val );
        reset = true;
      }
    }
    if ( ! reset ) { // it's new, so just add it at the end
      this.properties.add( new Property( key, val ) );
    }
  }
  protected void addProperty( String key, Object val ) {
    if ( this.properties == null ) {
      this.properties = new Vector();
    }
    Property p = getProperty( key );
    if ( p != null ) {
      throw new HydraConfigException( "Property " + p.getKey() +
                                     " has already been set to " + p.getVal() +
				     ", cannot be reset to " + val );
    }
    Property property = new Property( key, val );
    this.properties.add( property );
  }
  /**
   *  Answers whether this config has been postprocessed.
   */
  protected boolean postprocessed() {
    return this.postprocessed;
  }
  /**
   *  Sets this config to postprocessed.
   */
  protected void postprocess() {
    this.postprocessed = true;
  }
  /**
   *  Adds default values for properties that are not already defined.
   *  Does nothing if the defaults file does not exist.
   */
  public void fillInWithDefaultsFrom( String fn ) {
    FileInputStream fis;
    try {
      fis = new FileInputStream( fn ); 
    } catch( FileNotFoundException e ) {
      return; // no defaults available
    }
    Properties p = new Properties();
    try {
      p.load( fis );
    } catch( IOException e) {
      throw new HydraConfigException( "Unable to load test properties from " + fn );
    }
    for ( Enumeration names = p.propertyNames(); names.hasMoreElements(); ) {
      String name = (String) names.nextElement();
      if ( getProperty( name ) == null ) {
        addProperty( name, p.getProperty( name ) );
      }
    }
  }
  protected void writePropertiesToFile( String fn ) {
    StringBuffer buf = new StringBuffer();
    SortedMap map = getSortedProperties();
    if ( map != null ) {
      buf.append( "testName" + "=" + this.getName() + "\n" );
      for ( Iterator i = map.keySet().iterator(); i.hasNext(); ) {
        String key = (String) i.next();
	buf.append( key + "=" + map.get( key ) + "\n" );
      }
      FileUtil.writeToFile( fn, buf.toString() );
    }
  }
  protected String toTestString() {
    return toTestSpec() + "\n";
  }
  protected String toTestSpec() {
    if (this.properties == null) {
      return this.name;
    } else {
      return this.name + " " + toTestProps();
    }
  }
  protected String toTestProps() {
    StringBuffer buf = new StringBuffer();
    if ( this.properties != null ) {
      for ( Iterator i = this.properties.iterator(); i.hasNext(); ) {
        Property property = (Property) i.next();
	buf.append( " " + property.getKey() + "=" + property.getVal() );
      }
    }
    return buf.toString().trim();
  }
  public String toString() {
    String s = this.name;
    if ( this.properties != null ) {
      s += " " + this.properties;
    }
    return s;
  }
  public static class Property {
    String key;
    Object val;
    public Property( String key, Object val ) {
      this.key = key;
      this.val = val;
    }
    public String getKey() {
      return this.key;
    }
    public Object getVal() {
      return this.val;
    }
    protected void setVal( Object val ) {
      this.val = val;
    }
    public String toString() {
      return this.key + "=" + this.val;
    }
    public Property copy() { // shallow copy
      return new Property( this.key, this.val );
    }
  }
}
