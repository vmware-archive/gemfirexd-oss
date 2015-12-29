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

import hydra.*;

/**
 *
 *  Object creation helper used to create configurable objects encoded with
 *  an index.  Supported objects must either implement {@link ConfigurableObject}
 *  or be special-cased.  They can optionally rely on a parameter class for
 *  configuration.  See the various datatypes in this directory for examples.
 *  <p>
 *  Usage example:
 *  <blockquote>
 *  <pre>
 *     // In a hydra configuration file...
 *     objects.SizedStringPrms-size = 1024; // create strings of size 1K
 *
 *     // In one vm...
 *     Object name = ObjectHelper.createName( i );
 *     Object value = ObjectHelper.createObject( "objects.SizedString", i );
 *     cache.put( name, value );
 *
 *     // In a different vm...
 *     Object name = ObjectHelper.createName( i );
 *     Object value = cache.get( name );
 *     ObjectHelper.validate( i, value );
 *  </pre>
 *  </blockquote>
 */

public class ObjectHelper {

  // @todo lises
  // add support to create the "next" key (within a thread, within a vm, across vms)
  // base on hydra thread id to make unique via knowing total number of threads

  //----------------------------------------------------------------------------
  // Helper methods
  //----------------------------------------------------------------------------

  /**
   * Generates an object name of type java.lang.String encoding the supplied
   * index.
   */
  public static Object createName( int index ) {
    return String.valueOf( index );
  }

  /**
   * Generates an object key of the specified type encoding the supplied index.
   * The type must be a fully specified classname.
   *
   * @author lises
   * @since 5.0
   *
   * @throws HydraConfigException
   *         The class could not be found.
   * @throws ObjectCreationException
   *         An error occured when creating the object.  See the error
   *         message for more details.
   */
  public static Object createName(String classname, int index) {
    // skip reflection for common and special types
    if (classname.equals("java.lang.Long")) {
      return Long.valueOf(index);
    } else if (classname.equals("java.lang.Integer")) {
      return Integer.valueOf(index);
    } else if (classname.equals("java.lang.String")) {
      return String.valueOf( index );
    } else if (classname.equals("objects.BatchString")) {
      return BatchString.init(index);
    } else if ( classname.equals( "objects.PosNegInteger" ) ) {
      return PosNegInteger.init( index );
    } else {
      try {
	Class cls = Class.forName(classname);
	return cls.newInstance();
      } catch (ClassNotFoundException e ) {
	throw new HydraConfigException("Unable to find class for type " + classname, e);
      } catch (IllegalAccessException e) {
	throw new ObjectCreationException("Unable to instantiate object of type " + classname, e);
      } catch (InstantiationException e ) {
	throw new ObjectCreationException("Unable to instantiate object of type " + classname, e);
      }
    }
  }


  /**
   *  Generates an object of the specified type encoding the specified index,
   *  using the settings in the corresponding parameter class for the type, if
   *  any.  Invokes {@link ConfigurableObject#init} on the object, if it
   *  applies, otherwise handles specially supported types.
   *
   *  @throws HydraConfigException
   *          The class is not a supported type or could not be found.
   *  @throws ObjectCreationException
   *          An error occured when creating the object.  See the error
   *          message for more details.
   */
  public static Object createObject( String classname, int index ) {
    if ( classname.equals( "objects.FlatObject" ) ) {
      try {
        FlatObject[] obj = FlatObject.genObjects(1);
        FlatObject result = obj[0];
        result.setId(index);
        return result;
      } catch (java.io.IOException e) {
        throw new HydraRuntimeException("Creating FlatObject", e);
      }
    } else if ( classname.equals( "objects.ArrayOfByte" ) ) {
      return ArrayOfByte.init( index );
    } else if ( classname.equals( "objects.SizedString" ) ) {
      return SizedString.init( index );
    } else if ( classname.equals( "objects.BatchString" ) ) {
      return BatchString.init( index );
    } else if ( classname.equals( "objects.TestInteger" ) ) {
      return TestInteger.init( index );

    } else {
      try {
	Class cls = Class.forName( classname, true, Thread.currentThread().getContextClassLoader());
	ConfigurableObject obj = (ConfigurableObject) cls.newInstance();
	obj.init( index );
	return obj;
      } catch( ClassCastException e ) {
	throw new HydraConfigException( classname + " is neither a specially supported type nor a ConfigurableObject", e );
      } catch( ClassNotFoundException e ) {
	throw new HydraConfigException( "Unable to find class for type " + classname, e );
      } catch( IllegalAccessException e ) {
	throw new ObjectCreationException( "Unable to instantiate object of type " + classname, e );
      } catch( InstantiationException e ) {
	throw new ObjectCreationException( "Unable to instantiate object of type " + classname, e );
      }
    }
  }

  /**
   *  Returns the index encoded in the object.
   *  Invokes {@link ConfigurableObject#getIndex()} on the object, if it
   *  applies, otherwise handles it as a specially supported type.
   *
   *  @throws HydraConfigException
   *          The class is not a supported type.
   *  @throws ObjectAccessException
   *          An error occured when accessing the object.  See the error
   *          message for more details.
   */
  public static int getIndex( Object obj ) {
    try {
      if ( obj instanceof byte[] ) {
	return ArrayOfByte.getIndex( (byte[]) obj );

      } else if ( obj instanceof java.lang.String ) {
        String s = (String)obj;
        if (s.startsWith(BatchString.PREFIXER)) {
          return BatchString.getIndex( (String)obj );
        } else {
          return SizedString.getIndex( (String)obj );
        }

      } else if ( obj instanceof java.lang.Integer ) {
        return TestInteger.getIndex( (Integer) obj );

      } else {
	return ( (ConfigurableObject) obj ).getIndex();
      }
    } catch( ClassCastException e ) {
      throw new HydraConfigException( obj.getClass().getName() + " is neither a specially supported type nor a ConfigurableObject", e );
    }
  }

  /**
   *  Validates whether the index is encoded in the object, if this
   *  applies, and performs other validation checks as needed.
   *  Invokes {@link ConfigurableObject#validate(int)} on the object, if it
   *  applies, otherwise handles it as a specially supported type.
   *
   *  @throws HydraConfigException
   *          The class is not a supported type.
   *  @throws ObjectAccessException
   *          An error occured when accessing the object.  See the error
   *          message for more details.
   *  @throws ObjectValidationException
   *          The object failed validation.  See the error message for more
   *          details.
   */
  public static void validate( int index, Object obj ) {
    try {
      if ( obj == null ) {
        throw new ObjectValidationException( "The object with index " + index + " is null" );
      } else if ( obj instanceof byte[] ) {
	ArrayOfByte.validate( index, (byte[]) obj );

      } else if ( obj instanceof java.lang.String ) {
          String s = (String)obj;
          if (s.startsWith(BatchString.PREFIXER)) {
            BatchString.validate( index, (String)obj );
          } else {
            SizedString.validate( index, (String)obj );
          }

      } else if ( obj instanceof java.lang.Integer ) {
        TestInteger.validate( index, (Integer)obj );

      } else {
	( (ConfigurableObject) obj ).validate( index );
      }
    } catch( ClassCastException e ) {
      throw new HydraConfigException( obj.getClass().getName() + " is neither a specially supported type nor a ConfigurableObject", e );
    }
  }

  /**
   *  Returns the timestamp encoded in the object, if any.
   *  Invokes {@link TimestampedObject#getTimestamp()} on the object, if it
   *  applies, otherwise tries to handle it as a specially supported type.
   *
   *  @throws HydraConfigException
   *          The class is not a supported type.
   *  @throws ObjectAccessException
   *          An error occured when accessing the object.  See the error
   *          message for more details.
   */
  public static long getTimestamp(Object obj) {
    try {
      if (obj instanceof byte[]) {
	return ArrayOfByte.getTimestamp((byte[]) obj);
      } else if (obj instanceof java.lang.String) {
        throw new HydraConfigException( obj.getClass().getName() + " is not a specially supported type nor a TimestampedObject");
      } else if (obj instanceof java.lang.Integer) {
        throw new HydraConfigException( obj.getClass().getName() + " is not a specially supported type nor a TimestampedObject");
      } else {
	return ((TimestampedObject) obj).getTimestamp();
      }
    } catch(ClassCastException e) {
      throw new HydraConfigException(obj.getClass().getName() + " is neither a specially supported type nor a TimestampedObject", e);
    }
  }

  /**
   *  Resets the timestamp encoded in the object to the current time.
   *  Invokes {@link TimestampedObject#resetTimestamp()} on the object, if it
   *  applies, otherwise tries to handle it as a specially supported type.
   *
   *  @throws HydraConfigException
   *          The class is not a supported type.
   *  @throws ObjectAccessException
   *          An error occured when accessing the object.  See the error
   *          message for more details.
   */
  public static void resetTimestamp(Object obj) {
    try {
      if (obj instanceof byte[]) {
	ArrayOfByte.resetTimestamp((byte[]) obj);
      } else if (obj instanceof java.lang.String) {
        throw new HydraConfigException( obj.getClass().getName() + " is not a specially supported type nor a TimestampedObject");
      } else if (obj instanceof java.lang.Integer) {
        throw new HydraConfigException( obj.getClass().getName() + " is not a specially supported type nor a TimestampedObject");
      } else {
	((TimestampedObject) obj).resetTimestamp();
      }
    } catch(ClassCastException e) {
      throw new HydraConfigException(obj.getClass().getName() + " is neither a specially supported type nor a TimestampedObject", e);
    }
  }

  /**
   * Updates the object by invoking {@link UpdatableObject#update()}.
   *
   * @throws HydraConfigException
   *         The class is not a supported type.
   * @throws ObjectAccessException
   *         An error occured when accessing the object.  See the error
   *         message for more details.
   */
  public static void update(Object obj) {
    try {
      ((UpdatableObject)obj).update();
    } catch (ClassCastException e) {
      String s = obj.getClass().getName() + " is not an UpdatableObject";
      throw new HydraConfigException(s, e);
    }
  }
}
