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

import java.io.*;
import java.util.*;

/**
*
* Manages a HashMap used for storing hydra configuration settings.
* This class provides routines for retrieving string, int, long, and
* double representations of parameter values.
*
* <P>
*
* Note that if one of the "at" methods is invoked, but no value has
* been specified for the configuration parameter, a {@link
* HydraConfigException} is thrown.
*
* @see hydra.TestConfig#tab()
*/

public class ConfigHashtable implements Serializable {

  private static GsRandom randGen = null;

  private Map _mytab;

  public ConfigHashtable() {
    _mytab = new HashMap();
  }
  public int size() {
    return _mytab.size();
  }

  /**
  *
  * Returns the int value of key.
  *
  */
  public int intAt( Long key ) {
    Object val = this.get( key );
    if ( val == null )
      throw new HydraConfigException("No config param found for key \"" + BasePrms.nameForKey( key ) + "\"");
    return intFor( val, getRandGen(), key );
  }
  public int intAt( Long key, int defaultVal ) {
    Object val = this.get( key );
    if ( val == null )
      return defaultVal;
    else
      return intFor( val, getRandGen(), key );
  }
  public static int intFor( Object val, GsRandom rand, Long key ) {

    if ( val instanceof Integer ) {
        return ( (Integer) val ).intValue();
    }
    else if ( val instanceof Double ) {
        return ( (Double) val ).intValue();
    }
    else if ( val instanceof String ) {
        double dval;
        try {
          dval = Double.parseDouble((String)val);
        } catch (NumberFormatException e) {
          String s =  "Cannot use intAt on " + cause(key,val);
          throw new HydraConfigException(s, e);
        }
        int ival = (int)dval;
        if ((double)ival != dval) {
          String s =  "Cannot use intAt on " + cause(key,val);
          throw new HydraConfigException(s);
        }
        return ival;
    }
    else if ( val instanceof Range ) {
        return ( (Range) val ).nextInt( rand );
    }
    else if ( val instanceof OneOf ) {
        return intFor( ( (OneOf) val ).next( rand ), rand, key );
    }
    else if ( val instanceof RobinG ) {
        return intFor( ( (RobinG) val ).next( key ), rand, key );
    }
    else {
      throw new HydraTypeException("Cannot use intAt on " + cause(key,val));
    }
  }

  /**
  *
  * Return the ith int of a vector parameter.  If none is
  * specified, try previous elements, from i-1 down to 0, using
  * the first that isn't null.  If they are all null, use the
  * provided default value.
  *
  */
  public int intAtWild( Long key, int i, int defaultVal ) {

    Object val = getWild( key, i, new Integer( defaultVal ) );
    if ( val == null )
      throw new HydraConfigException("No config param found for key \"" + BasePrms.nameForKey( key ) + "\" at index " + i );
    else
      return intFor( val, getRandGen(), key );
  }

  /**
  *
  * Returns the long value of the Double at key.
  *
  */
  public long longAt( Long key ) {
    Object val = this.get( key );
    if ( val == null )
      throw new HydraConfigException("No config param found for key \"" + BasePrms.nameForKey( key ) + "\"");
    return longFor( val, getRandGen(), key );
  }
  public long longAt( Long key, long defaultVal ) {
    Object val = this.get( key );
    if ( val == null )
      return defaultVal;
    else
      return longFor( val, getRandGen(), key );
  }
  public static long longFor( Object val, GsRandom rand, Long key ) {

    if ( val instanceof Long ) {
        return ( (Long) val ).longValue();
    }
    else if ( val instanceof Integer ) {
        return  ( (Integer) val ).intValue();
    }
    else if ( val instanceof Double ) {
        return ( (Double) val ).longValue();
    }
    else if ( val instanceof String ) {
        double dval;
        try {
          dval = Double.parseDouble((String)val);
        } catch (NumberFormatException e) {
          String s =  "Cannot use longAt on " + cause(key,val);
          throw new HydraConfigException(s, e);
        }
        long lval = (long)dval;
        if ((double)lval != dval) {
          String s =  "Cannot use longAt on " + cause(key,val);
          throw new HydraConfigException(s);
        }
        return lval;
    }
    else if ( val instanceof Range ) {
        return ( (Range) val ).nextLong( rand );
    }
    else if ( val instanceof OneOf ) {
        return longFor( ( (OneOf) val ).next( rand ), rand, key );
    }
    else if ( val instanceof RobinG ) {
        return longFor( ( (RobinG) val ).next( key ), rand, key );
    }
    else {
      throw new HydraTypeException("Cannot use longAt on " + cause(key,val));
    }
  }

  /**
  *
  * Return the ith long of a vector parameter.  If none is
  * specified, try previous elements, from i-1 down to 0, using
  * the first that isn't null.  If they are all null, use the
  * provided default value.
  *
  */
  public long longAtWild( Long key, int i, long defaultVal ) {

    Object val = getWild( key, i, new Long( defaultVal ) );
    if ( val == null )
      throw new HydraConfigException("No config param found for key \"" + BasePrms.nameForKey( key ) + "\" at index " + i );
    else
      return longFor( val, getRandGen(), key );
  }

  /**
  *
  * Returns the double value of the Double at key.
  *
  */
  public double doubleAt( Long key ) {
    Object val = this.get( key );
    if ( val == null )
      throw new HydraConfigException("No config param found for key \"" + BasePrms.nameForKey( key ) + "\"");
    return doubleFor( val, getRandGen(), key );
  }
  public double doubleAt( Long key, double defaultVal ) {
    Object val = this.get( key );
    if ( val == null )
      return defaultVal;
    else
      return doubleFor( val, getRandGen(), key );
  }
  public static double doubleFor( Object val, GsRandom rand, Long key ) {

    if ( val instanceof Integer ) {
        return ( (Integer) val ).intValue();
    }
    else if ( val instanceof Double ) {
        return ( (Double) val ).doubleValue();
    }
    else if ( val instanceof String ) {
        double dval;
        try {
          dval = Double.parseDouble((String)val);
        } catch (NumberFormatException e) {
          String s =  "Cannot use doubleAt on " + cause(key,val);
          throw new HydraConfigException(s, e);
        }
        return dval;
    }
    else if ( val instanceof Range ) {
        return ( (Range) val ).nextDouble( rand );
    }
    else if ( val instanceof OneOf ) {
        return doubleFor( ( (OneOf) val ).next( rand ), rand, key );
    }
    else if ( val instanceof RobinG ) {
        return doubleFor( ( (RobinG) val ).next( key ), rand, key );
    }
    else {
      throw new HydraTypeException("Cannot use doubleAt on " + cause(key,val));
    }
  }

  /**
  *
  * Return the ith double of a vector parameter.  If none is
  * specified, try previous elements, from i-1 down to 0, using
  * the first that isn't null.  If they are all null, use the
  * provided default value.
  *
  */
  public double doubleAtWild( Long key, int i, double defaultVal ) {

    Object val = getWild( key, i, new Double( defaultVal ) );
    if ( val == null )
      throw new HydraConfigException("No config param found for key \"" + BasePrms.nameForKey( key ) + "\" at index " + i );
    else
      return doubleFor( val, getRandGen(), key );
  }

  /**
  *
  * Returns the string at key as a boolean.
  *
  */
  public boolean booleanAt( Long key ) {
    Object val = this.get( key );
    if ( val == null )
      throw new HydraConfigException("No config param found for key \"" + BasePrms.nameForKey( key ) + "\"");
    return booleanFor( val, getRandGen(), key );
  }
  public boolean booleanAt( Long key, boolean defaultVal ) {
    Object val = this.get( key );
    if ( val == null )
      return defaultVal;
    else
      return booleanFor( val, getRandGen(), key );
  }
  public static boolean booleanFor( Object val, GsRandom rand, Long key ) {

    if ( val instanceof Boolean ) {
        return ( (Boolean) val ).booleanValue();
    }
    else if ( val instanceof String ) {
        if ( ( (String) val ).equalsIgnoreCase( "true" ) )
          return true;
        else if ( ( (String) val ).equalsIgnoreCase( "false" ) )
          return false;
        else
          throw new HydraTypeException("Cannot use booleanAt on " + cause(key,val));
    }
    else if ( val instanceof OneOf ) {
        return booleanFor( ( (OneOf) val ).next( rand ), rand, key );
    }
    else if ( val instanceof RobinG ) {
        return booleanFor( ( (RobinG) val ).next( key ), rand, key );
    }
    else {
      throw new HydraTypeException("Cannot use booleanAt on " + cause(key,val));
    }
  }

  /**
  *
  * Return the ith boolean of a vector parameter.  If none is
  * specified, try previous elements, from i-1 down to 0, using
  * the first that isn't null.  If they are all null, use the
  * provided default value.
  *
  */
  public boolean booleanAtWild( Long key, int i, Boolean defaultVal ) {
    Object val = getWild( key, i, defaultVal );
    if ( val == null )
      throw new HydraConfigException("No config param found for key \"" + BasePrms.nameForKey( key ) + "\" at index " + i );
    else
      return booleanFor( val, getRandGen(), key );
  }

  /**
  *
  * Returns the String stored at key.
  *
  */
  public String stringAt( Long key ) {
    Object val = this.get( key );
    if ( val == null )
      throw new HydraConfigException("No config param found for key \"" + BasePrms.nameForKey( key ) + "\"");
    return stringFor( val, getRandGen(), key );
  }
  public String stringAt( Long key, String defaultVal ) {
    Object val = this.get( key );
    if ( val == null )
      return defaultVal;
    else
      return stringFor( val, getRandGen(), key );
  }
  public static String stringFor( Object val, GsRandom rand, Long key ) {

    if ( val instanceof Integer ) {
        return val.toString();
    }
    else if ( val instanceof Double ) {
        return val.toString();
    }
    else if ( val instanceof String ) {
        return (String) val;
    }
    else if ( val instanceof OneOf ) {
        return stringFor( ( (OneOf) val ).next( rand ), rand, key );
    }
    else if ( val instanceof RobinG ) {
        return stringFor( ( (RobinG) val ).next( key ), rand, key );
    }
    else {
      throw new HydraTypeException("Cannot use stringAt on " + cause(key,val));
    }
  }

  /**
  *
  * Return the ith element of a vector parameter as a String,
  * or default if null.
  *
  */
  public String stringAt( Long key, int i, String defaultVal ) {
    Object val = this.get( key, i, defaultVal );
    if ( val == null )
      throw new HydraConfigException("No config param found for key \"" + BasePrms.nameForKey( key ) + "\" at index " + i );
    else
      return stringFor( val, getRandGen(), key );
  }

  /**
  *
  * Return the ith string of a vector parameter.  If none is
  * specified, try previous elements, from i-1 down to 0, using
  * the first that isn't null.  If they are all null, use the
  * provided default value (which can be null).
  *
  */
  public String stringAtWild( Long key, int i, String defaultVal ) {
    Object val = getWild( key, i, defaultVal );
    if ( val == null ) {
      if ( defaultVal == null ) // we asked for it
        return null;
      else
        throw new HydraConfigException
        (
          "No config param found for key \"" + BasePrms.nameForKey( key ) +
          "\" at index " + i );
    }
    else
      return stringFor( val, getRandGen(), key );
  }

  /**
  *
  * Returns the path stored at key.
  *
  */
  public String pathAt( Long key ) {
    Object val = this.get( key );
    if ( val == null )
      throw new HydraConfigException("No config param found for key \"" + BasePrms.nameForKey( key ) + "\"");
    return pathFor( val, getRandGen(), key );
  }
  public String pathAt( Long key, String defaultVal ) {
    Object val = this.get( key );
    if ( val == null )
      return defaultVal;
    else
      return pathFor( val, getRandGen(), key );
  }
  public static String pathFor( Object val, GsRandom rand, Long key ) {

    if ( val instanceof String ) {
        String clientName = System.getProperty( ClientPrms.CLIENT_NAME_PROPERTY );
        HostDescription hd = null;
        if(clientName==null) {
          hd = TestConfig.getInstance().getMasterDescription().getVmDescription().getHostDescription();
        } else {
                hd = TestConfig.getInstance().getClientDescription( clientName ).getVmDescription().getHostDescription();
        }
        return EnvHelper.expandEnvVars( (String) val, hd );
    }
    else if ( val instanceof OneOf ) {
            return pathFor( ( (OneOf) val ).next( rand ), rand, key );
    }
    else if ( val instanceof RobinG ) {
            return pathFor( ( (RobinG) val ).next( key ), rand, key );
    }
    else {
      throw new HydraTypeException("Cannot use pathAt on " + cause(key,val));
    }
  }

  /**
  *
  * Return the ith element of a vector parameter as a path,
  * or default if null.
  *
  */
  public String pathAt( Long key, int i, String defaultVal ) {
    Object val = this.get( key, i, defaultVal );
    if ( val == null )
      throw new HydraConfigException("No config param found for key \"" + BasePrms.nameForKey( key ) + "\" at index " + i );
    else
      return pathFor( val, getRandGen(), key );
  }

  /**
  *
  * Return the ith path of a vector parameter.  If none is
  * specified, try previous elements, from i-1 down to 0, using
  * the first that isn't null.  If they are all null, use the
  * provided default value (which can be null).
  *
  */
  public String pathAtWild( Long key, int i, String defaultVal ) {
    Object val = getWild( key, i, defaultVal );
    if ( val == null ) {
      if ( defaultVal == null ) // we asked for it
        return null;
      else
        throw new HydraConfigException
          (
          "No config param found for key \"" + BasePrms.nameForKey( key ) +
          "\" at index " + i );
    }
    else {
      return pathFor( val, getRandGen(), key );
    }
  }

  /**
  *
  * Returns the HydraVector stored at key.  If the value is not a HydraVector,
  * it creates one.
  *
  */
  public HydraVector vecAt( Long key ) {
    Object val = this.get( key );
    if ( val == null )
      throw new HydraConfigException("No config param found for key \"" + BasePrms.nameForKey( key ) + "\"");
    return vecFor( val );
  }
  public HydraVector vecAt( Long key, HydraVector defaultVal ) {
    Object val = this.get( key );
    if ( val == null )
      return defaultVal;
    else
      return vecFor( val );
  }
  /**
  *
  * Return the ith element of a vector of vectors parameter as a HydraVector,
  * or default if null.  If the ith value is not a HydraVector, it
  * creates one.
  *
  */
  public HydraVector vecAt( Long key, int i, HydraVector defaultVal ) {
    // get the element, if any
    Object val = this.get( key );
    if ( val == null ) {
      // key holds nothing
      return defaultVal;
    } else if ( val instanceof HydraVector ) {
      // key holds vector type [? ?], get the i'th element, if any
      Object subval = this.get( key, i, null );
      if ( subval == null ) {
        // there is no i'th element
        return defaultVal;
      } else if ( subval instanceof HydraVector ) {
        // i'th element is vector type [A A], so key holds vector of vector type [[A A][A A]]
        return (HydraVector) subval;
      } else {
        // i'th element is non-vector type A, so key holds simple vector type [A A]
        if ( i == 0 ) {
          // and that is the vector we seek
          return (HydraVector) val;
        } else {
          // there is no i'th vector
          return defaultVal;
        }
      }
    } else {
      // key holds simple non-vector type A
      if ( i == 0 ) {
        // and this is the element we seek
        return vecFor( val );
      } else {
        // there is no i'th element
        return defaultVal;
      }
    }
  }
  /**
  *
  * Return the ith vector of a vector of vectors parameter.  If none
  * is specified, try previous elements, from i-1 down to 0, using
  * the first that isn't null.  If they are all null, use the
  * provided default value (which can be null).
  *
  */
  public HydraVector vecAtWild( Long key, int i, HydraVector defaultVal ) {
    Object val = get( key );
    if ( val == null ) {
      return defaultVal;
    } else if ( val instanceof HydraVector ) {
      boolean vecvec = false;
      for ( Iterator it = ((HydraVector)val).iterator(); it.hasNext(); ) {
        Object vecval = it.next();
        if ( vecval instanceof HydraVector ) {
          vecvec = true;
          break;
        }
      }
      if ( vecvec ) { // vector of vectors, return i'th one (wild) as a vector
        val = getWild( key, i, defaultVal );
        if ( val instanceof HydraVector ) {
          return (HydraVector) val;
        } else {
          return vecFor( val );
        }
      } else { // single vector, return whole enchilada
        return (HydraVector) val;
      }
    } else { // simple value, return as vector
      return vecFor( val );
    }
  }
  public static HydraVector vecFor( Object val ) {

    if ( val instanceof HydraVector ) {
      return (HydraVector) val;
    } else {
      return new HydraVector( val );
    }
  }

  /**
  *
  * Return the parameter with the given key or null if there isn't one.
  *
  */
  public Object get( Long key ) {
    return _mytab.get( key );
  }

  /**
  *
  * Return the parameter with the given key or the default if there isn't one.
  *
  */
  public Object get( Long key, Object defaultVal ) {
    Object obj = _mytab.get( key );
    if ( obj == null )
      obj = defaultVal;
    return obj;
  }

  /**
  *
  * Return the ith element of a vector parameter,
  * or the default if it's not there.
  *
  */
  public Object get( Long key, int i, Object defaultVal ) {
    Object result = defaultVal;
    Vector v = (Vector) _mytab.get( key );
    if ( ( v != null ) && ( i < v.size() ) )
      result = v.elementAt(i);
    return result;
  }

  /**
  *
  * Return the ith element of a hydra vector parameter.  If none
  * is specified, try previous elements, from i-1 down to 0, using
  * the first that isn't null.  If they are all null, use the
  * provided default value.
  *
  * If the parameter is not a vector, just return the value, if it exists.
  *
  */
  public Object getWild( Long key, int i, Object defaultVal ) {

    Object o = _mytab.get( key );
    if ( o == null ) return defaultVal;
    if ( ! ( o instanceof Vector ) ) return o;
//    Vector v = (Vector) o;
    return getWild( (Vector) o, i, defaultVal );
  }
  public static Object getWild( Vector v, int i, Object defaultVal ) {
    int sz = v.size();
    if ( sz == 0 )   return defaultVal;
    if ( i < sz )    return v.elementAt(i);
    return v.elementAt( sz - 1 );
  }

//------------------------------------------------------------------------------
// Support for returning object types that could have been specified "default"
//------------------------------------------------------------------------------

  /**
   * Returns a Boolean from the object, null if it is the default string.
   * Handles OneOf, Range, RobinG, etc.
   * @throws HydraTypeException if the object is an illegal type.
   */
  public Boolean getBoolean(Long key, Object val) {
    if (val == null) {
      return null;

    } else if (val instanceof Boolean) {
      return (Boolean)val;

    } else if (val instanceof String) {
      if (((String)val).equalsIgnoreCase(BasePrms.DEFAULT)) {
        return null;
      } else if (((String)val).equalsIgnoreCase("true")) {
        return Boolean.TRUE;
      } else if (((String)val).equalsIgnoreCase("false")) {
        return Boolean.FALSE;
      } else {
        throw new HydraTypeException("Cannot use getBoolean on " + cause(key,val));
      }
    } else if (val instanceof OneOf) {
      return getBoolean(key, ((OneOf)val).next(getRandGen()));

    } else if (val instanceof RobinG) {
      return getBoolean(key, ((RobinG)val).next(key));

    } else {
      throw new HydraTypeException("Cannot use getBoolean on " + cause(key,val));
    }
  }

  /**
   * Returns a Double from the object, null if it is the default string.
   * Handles OneOf, Range, RobinG, etc.
   * @throws HydraTypeException if the object is an illegal type.
   */
  public Double getDouble(Long key, Object val) {
    if (val == null) {
      return null;

    } else if (val instanceof Integer) {
      return new Double(((Integer)val).intValue());

    } else if (val instanceof Double) {
      return (Double)val;

    } else if (val instanceof String) {
      if (((String)val).equalsIgnoreCase(BasePrms.DEFAULT)) {
        return null;
      } else {
        double dval;
        try {
          dval = Double.parseDouble((String)val);
        } catch (NumberFormatException e) {
          String s =  "Cannot use getDouble on " + cause(key,val);
          throw new HydraConfigException(s, e);
        }
        return new Double(dval);
      }
    } else if (val instanceof Range) {
      return new Double(((Range)val).nextDouble(getRandGen()));

    } else if (val instanceof OneOf) {
      return getDouble(key, ((OneOf)val).next(getRandGen()));

    } else if (val instanceof RobinG) {
      return getDouble(key, ((RobinG)val).next(key));

    } else {
      throw new HydraTypeException("Cannot use getDouble on " + cause(key,val));
    }
  }

  /**
   * Returns an Integer from the object, null if it is the default string.
   * Handles OneOf, Range, RobinG, etc.
   * @throws HydraTypeException if the object is an illegal type.
   */
  public Integer getInteger(Long key, Object val) {
    if (val == null) {
      return null;

    } else if (val instanceof Integer) {
      return (Integer)val;

    } else if (val instanceof Double) {
      return new Integer(((Double)val).intValue());

    } else if (val instanceof String) {
      if (((String)val).equalsIgnoreCase(BasePrms.DEFAULT)) {
        return null;
      } else {
        double dval;
        try {
          dval = Double.parseDouble((String)val);
        } catch (NumberFormatException e) {
          String s =  "Cannot use getInteger on " + cause(key,val);
          throw new HydraConfigException(s, e);
        }
        int ival = (int)dval;
        if ((double)ival != dval) {
          String s =  "Cannot use getInteger on " + cause(key,val);
          throw new HydraConfigException(s);
        }
        return new Integer(ival);
      }
    } else if (val instanceof Range) {
      return new Integer(((Range)val).nextInt(getRandGen()));

    } else if (val instanceof OneOf) {
      return getInteger(key, ((OneOf)val).next(getRandGen()));

    } else if (val instanceof RobinG) {
      return getInteger(key, ((RobinG)val).next(key));

    } else {
      throw new HydraTypeException("Cannot use getInteger on " + cause(key,val));
    }
  }

  /**
   * Returns a Long from the object, null if it is the default string.
   * Handles OneOf, Range, RobinG, etc.
   * @throws HydraTypeException if the object is an illegal type.
   */
  public Long getLong(Long key, Object val) {
    if (val == null) {
      return null;

    } else if (val instanceof Long) {
      return (Long)val;

    } else if (val instanceof Integer) {
      return new Long(((Integer)val).intValue());

    } else if (val instanceof Double) {
      return new Long(((Double)val).longValue());

    } else if (val instanceof String) {
      if (((String)val).equalsIgnoreCase(BasePrms.DEFAULT)) {
        return null;
      } else {
        double dval;
        try {
          dval = Double.parseDouble((String)val);
        } catch (NumberFormatException e) {
          String s =  "Cannot use getLong on " + cause(key,val);
          throw new HydraConfigException(s, e);
        }
        long lval = (long)dval;
        if ((double)lval != dval) {
          String s =  "Cannot use getLong on " + cause(key,val);
          throw new HydraConfigException(s);
        }
        return new Long(lval);
      }
    } else if (val instanceof Range) {
      return new Long(((Range)val).nextLong(getRandGen()));

    } else if (val instanceof OneOf) {
      return getLong(key, ((OneOf)val).next(getRandGen()));

    } else if (val instanceof RobinG) {
      return getLong(key, ((RobinG)val).next(key));

    } else {
      throw new HydraTypeException("Cannot use getLong on " + cause(key,val));
    }
  }

  /**
   * Returns a String from the object, null if it is the default string.
   * Handles OneOf, Range, RobinG, etc.
   * @throws HydraTypeException if the object is an illegal type.
   */
  public String getString(Long key, Object val) {
    if (val == null) {
      return null;

    } else if (val instanceof Integer) {
      return val.toString();

    } else if (val instanceof Double) {
      return val.toString();

    } else if (val instanceof String) {
      if (((String)val).equalsIgnoreCase(BasePrms.DEFAULT)) {
        return null;
      } else {
        return (String)val;
      }
    } else if (val instanceof OneOf) {
      return getString(key, ((OneOf)val).next(getRandGen()));

    } else if (val instanceof RobinG) {
      return getString(key, ((RobinG)val).next(key));

    } else {
      throw new HydraTypeException("Cannot use getString on " + cause(key,val));
    }
  }

  /**
   * Returns a Vector from the object, null if it is the default string.
   * @throws HydraTypeException if the object is an illegal type.
   */
  public Vector getVector(Long key, Object val) {
    if (val == null) {
      return null;
    }
    else if (val instanceof Vector) {
      return (Vector)val;
    }
    else if (val instanceof Integer) {
      return vecFor(val.toString());
    }
    else if (val instanceof Double) {
      return vecFor(val.toString());
    }
    else if (val instanceof String) {
      if (((String)val).equalsIgnoreCase(BasePrms.DEFAULT)) {
        return null;
      }
      else {
        return vecFor(val);
      }
    }
    else {
      throw new HydraTypeException("Cannot use getString on " + cause(key,val));
    }
  }

  /**
   * Returns a path from the object, null if it is the default string.
   * Handles OneOf, RobinG, etc.
   * @throws HydraTypeException if the object is an illegal type.
   */
  protected String getPath(Long key, Object val) {
    if (val == null) {
      return null;

    } else if (val instanceof String) {
      if (((String)val).equalsIgnoreCase(BasePrms.DEFAULT)) {
        return null;
      } else {
        String clientName = System.getProperty(ClientPrms.CLIENT_NAME_PROPERTY);
        HostDescription hd = null;
        if (clientName == null) { // running in master
          hd = TestConfig.getInstance().getMasterDescription()
                         .getVmDescription().getHostDescription();
        } else { // running in client
          hd = TestConfig.getInstance().getClientDescription(clientName)
                         .getVmDescription().getHostDescription();
        }
        return EnvHelper.expandEnvVars((String) val, hd);
      }
    } else if (val instanceof OneOf) {
      return getPath(key, ((OneOf)val).next(getRandGen()));

    } else if (val instanceof RobinG) {
      return getPath(key, ((RobinG)val).next(key));

    } else {
      throw new HydraTypeException("Cannot use getPath on " + cause(key,val));
    }
  }

  /**
  *
  * Put an object in the table if it isn't null.
  *
  */
  public void put( Long key, Object val ) {
    if ( val != null )
      _mytab.put( key, val );
  }

  /**
  *
  * Returns the random number generator associated with this instance of
  * ConfigHashtable.  It is also used by instances of HydraVector stored
  * in this table.  It is used to compute values for "range" and "oneof".
  *
  * The seed is obtained from {@link Prms#randomSeed} if it exists.
  * Otherwise the seed is <code>System.currentTimeMillis()</code>.
  *
  */
  public GsRandom getRandGen() {
    if ( randGen == null ) {
      long seed = -1;
      Object val = this.get( Prms.randomSeed );
      if ( val instanceof Long )
        seed = ( (Long) val ).longValue();
      else if ( val instanceof String )
        seed = Long.parseLong( (String) val );
      else
        throw new HydraTypeException( "Cannot getRandGen seed from " + val.getClass().getName() );

      // perturb the seed in a predictable way so each client vm has a different one
      String clientName = System.getProperty(ClientPrms.CLIENT_NAME_PROPERTY);
      if (clientName != null) { // running in client
        seed += RemoteTestModule.getMyVmid();
      } // else do nothing

      randGen = new GsRandom( seed );
    }
    return randGen;
  }

  public String toString() {
    StringBuffer buf = new StringBuffer();
    SortedMap map = this.toSortedMap();
    for ( Iterator i = map.keySet().iterator(); i.hasNext(); ) {
      String key = (String) i.next();
      Object val = map.get( key );
      buf.append( key + "=" + val + "\n" );
    }
    return buf.toString();
  }
  public SortedMap toSortedMap() {
    return toSortedMap(false);
  }
  protected SortedMap toSortedMap(boolean forLatestConf) {
    Hashtable stringers = new Hashtable();
    for ( Iterator i = _mytab.keySet().iterator(); i.hasNext(); ) {
      Long key = (Long) i.next();
      stringers.put( BasePrms.nameForKey( key ), _mytab.get( key ) );
    }
    SortedMap map = new TreeMap( stringers );
    for ( Iterator i = map.keySet().iterator(); i.hasNext(); ) {
      String key = (String) i.next();
      Object obj = map.get( key );
      if ( obj != null ) {
        StringBuffer buf = new StringBuffer();
        appendStrVal( obj, buf, forLatestConf );
        map.put( key, buf.toString() );
      }
    }
    return map;
  }

  private void appendStrVal(Object obj, StringBuffer buf, boolean forLatestConf) {
    if (obj instanceof HydraVector) {
      if (!forLatestConf) buf.append(" [ HYDRAVECTOR ");
      HydraVector vecVal = (HydraVector) obj;
      if (vecVal.size() > 0) {
        if (vecVal.get(0) instanceof HydraVector) { // list of lists
          for (int i = 0; i < vecVal.size(); i++) {
            appendStrVal(vecVal.get(i), buf, forLatestConf); 
            if (i < (vecVal.size() - 1)) {
              buf.append(", ");
            }
          }
        } else { // list
          for (int i = 0; i < vecVal.size(); i++) {
            appendStrVal(vecVal.get(i), buf, forLatestConf); 
            if (i < (vecVal.size() - 1)) {
              if (!forLatestConf) buf.append(",");
              buf.append(" ");
            }
          }
        }
      }
      if (!forLatestConf) buf.append(" ] ");

    } else if (obj instanceof OneOf || obj instanceof Range || obj instanceof RobinG) {
      if (!forLatestConf) buf.append(" [ ");
      buf.append(obj.toString());
      if (!forLatestConf) buf.append(" ] ");

    } else if (obj instanceof Boolean) {
      buf.append(((Boolean)obj).toString().toLowerCase());

    } else if (obj instanceof Double || obj instanceof Integer || obj instanceof Long) {
      buf.append(obj.toString());

    } else if (obj instanceof String) {
      buf.append(obj);

    } else {
      String s = "Unexpected object type: " + obj.getClass().getName();
      throw new HydraInternalException(s);
    }
  }

  private static String cause(Long key, Object val) {
    return val.getClass().getName()
       + " at key \"" + BasePrms.nameForKey( key ) + "\""
       + " (a common cause is using a FCN that returns a different type)";
  }
}
