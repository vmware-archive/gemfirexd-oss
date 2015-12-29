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

/**
 * Abstract superclass for description objects.
 */
public abstract class AbstractDescription {

  /**
   * Returns a sorted map of description fields and their values.
   */
  public abstract SortedMap toSortedMap();

  /**
   * Returns a string representing the sorted field-value map with each entry
   * on a separate line, indented if necessary to fit within parent descriptions
   * for ease of reading in a log file.
   */
  public String toString() {
    StringBuffer buf = new StringBuffer();
    SortedMap map = this.toSortedMap();
    for (Object key : map.keySet()) {
      Object val = map.get(key);
      buf.append(key + "=" + val + "\n");
    }
    return buf.toString();
  }

//------------------------------------------------------------------------------
// General configuration support
//------------------------------------------------------------------------------

  /**
   * Adds the src properties to the non-null dst properties.
   */
  protected Properties addProperties(Properties src, Properties dst) {
    if (src == null) {
      return dst;
    } else if (dst == null) {
      String s = "Atttempt to add to null dst properties";
      throw new HydraInternalException(s);
    } else {
      for (Iterator i = src.keySet().iterator(); i.hasNext();) {
        String key = (String)i.next();
        dst.setProperty(key, src.getProperty(key));
      }
    }
    return dst;
  }

  /**
   * Converts the primitive int array to a List of Integer.
   */
  protected static List asList(int[] ints) {
    List list = new ArrayList();
    for (int i = 0; i < ints.length; i++) {
      list.add(new Integer(ints[i]));
    }
    return list;
  }

  /**
   * Converts the given string to a Boolean.
   */
  protected static Boolean getBooleanFor(String str, Long key) {
    if (str.equalsIgnoreCase("true")) {
      return Boolean.TRUE;
    } else if (str.equalsIgnoreCase("false")) {
      return Boolean.FALSE;
    } else {
      String s = BasePrms.nameForKey(key) + " has illegal value: " + str;
      throw new HydraConfigException(s);
    }
  }

  /**
   * Converts the given string to an Integer.
   */
  protected static Integer getIntegerFor(String str, Long key) {
    try {
      return Integer.valueOf(str);
    } catch (NumberFormatException e) {
      String s = BasePrms.nameForKey(key) + " has illegal value: " + str;
      throw new HydraConfigException(s);
    }
  }

  /**
   * Returns the class with the given name.
   * @throws HydraConfigException if not found.
   */
  protected static Class getClass(Long key, String classname) {
    try {
      return Class.forName(classname);
    } catch (ClassNotFoundException e) {
      String s = BasePrms.nameForKey(key) + " class not found: " + classname;
      throw new HydraConfigException(s);
    }
  }

  /**
   * Returns the classname for the given object.
   */
  protected static String getClassname(Object obj) {
    return obj == null ? null : obj.getClass().getName();
  }

  /**
   * Returns the arg types as a comma-separated string of fully-qualified
   * class names.
   */
  private static String argTypesAsString(Class[] argTypes) {
    String s = "";
    if (argTypes != null) {
      for (int i = 0; i < argTypes.length; i++) {
        if (i != 0) {
          s += ", ";
        }
        s += argTypes[i].getName();
      }
    }
    return s;
  }

  /**
   * Returns the method with the given name and arg types from the class.
   * @throws HydraConfigException if not found.
   */
  private static Method getMethod(Long key, Class cls, String methodname,
                                  Class[] argTypes) {
    try {
      return cls.getDeclaredMethod(methodname, argTypes);
    } catch (NoSuchMethodException e) {
      String s = BasePrms.nameForKey(key) + " method not found: " + cls + "."
               + methodname + "(" + argTypesAsString(argTypes) + ")";
      throw new HydraConfigException(s);
    }
  }

  /**
   * Returns the public static method with the given arg types and return type
   * from the class.
   *
   * @throws HydraConfigException if the method is not found.
   */
  private static Method getMethod(Long key, Class cls, String methodname,
                                  Class[] argTypes, Class returnType) {
    Method method = getMethod(key, cls, methodname, argTypes);
    Class actualReturnType = method.getReturnType();
    if (actualReturnType != returnType) {
      String s = BasePrms.nameForKey(key) + " method " + methodname + " in "
               + cls + " does not have expected return type " + returnType
               + ", instead returns: " + actualReturnType;
      throw new HydraConfigException(s);
    }
    int modifiers = method.getModifiers();
    if (!Modifier.isPublic(modifiers) || !Modifier.isStatic(modifiers)) {
      String s = BasePrms.nameForKey(key) + " method " + methodname + " in "
               + cls + " is not public static";
      throw new HydraConfigException(s);
    }
    return method;
  }

  /**
   * Returns an instance of the class with the given name.
   * @throws HydraConfigException if instantiation fails.
   */
  public static Object getInstance(Long key, String classname) {
    Class cls = getClass(key, classname);
    try {
      return cls.newInstance();
    } catch(Exception e) {
      String s = BasePrms.nameForKey(key)
               + " cannot instantiate class " + classname + " due to "
               + e.getClass().getName() + " (" + e.getMessage() + ")";
      throw new HydraConfigException(s);
    }
  }

  /**
   * Makes sure the given integer is nonnegative.
   * @throws HydraConfigException if the integer is negative.
   */
  protected static Integer getNonnegativeIntegerFor(Integer i, Long key) {
    if (i.intValue() >= 0) {
      return i;
    } else {
      String s = BasePrms.nameForKey(key) + " has negative value: " + i;
      throw new HydraConfigException(s);
    }
  }

  /**
   * Returns the "public static Long" parameters provided by the given class,
   * as Long keys, along with their default values or the empty string.  Assumes
   * that default fields start with "DEFAULT_", are all upper case, and use
   * underscores between words that are capitalized in the corresponding
   * parameter field.
   * @throws HydraConfigException if none are found.
   */
  protected static Map getParametersAndDefaults(Long key, Class cls) {
    Map map = new HashMap();
    Field[] fields = cls.getDeclaredFields();
    for (int i = 0; i < fields.length; i++) {
      Field field = fields[i];
      int m = field.getModifiers();
      if (Modifier.isPublic(m) && Modifier.isStatic(m) &&
         !Modifier.isFinal(m) && field.getType() == Long.class) {

        // compute the key
        String fieldName = field.getName();
        Long pkey = BasePrms.keyForName(cls.getName() + BasePrms.DASH
                                                      + fieldName);

        // compute the default value
        String def = "";
        String defName = convertPrmDefault(fieldName);
        try {
          Field defField = cls.getDeclaredField(defName);
          if (defField != null) {
            int d = defField.getModifiers();
            if (Modifier.isPublic(d) && Modifier.isStatic(d) &&
                Modifier.isFinal(d) && defField.getType() == String.class) {
              def = (String)defField.get(null);
            } else {
              String s = "For " + BasePrms.nameForKey(key) + "=" + cls.getName()
                       + ", field " + defName
                       + " for the default value of field " + fieldName
                       + " is not a public static final String"; 
              throw new HydraConfigException(s);
            }
          }
        } catch (IllegalAccessException e) {
          String s = "For " + BasePrms.nameForKey(key) + "=" + cls.getName()
                   + ", unable to access field " + defName;
          throw new HydraConfigException(s);
        } catch (NoSuchFieldException e) {
          // no default was provided
        }
        map.put(pkey, def);
      }
    }
    if (map.size() == 0) {
      String s = BasePrms.nameForKey(key) + "=" + cls.getName()
               + " contains no public static Long fields";
      throw new HydraConfigException(s);
    }
    return map;
  }

  /**
   * Converts the parameter key to its gemfire-style property name.  For
   * example, <code>passwordFile</code> converts to <code>password-file</code>.
   */
  protected String convertPrm(Long key) {
    String name = BasePrms.nameForKey(key);
    name = name.substring(name.indexOf("-") + 1, name.length());
    StringBuffer buf = new StringBuffer();
    char[] chars = name.toCharArray();
    for (int i = 0; i < chars.length; i++) {
      if (Character.isUpperCase(chars[i])) {
        if (i != 0) {
          buf.append("-");
        }
        buf.append(Character.toLowerCase(chars[i]));
      } else {
        buf.append(chars[i]);
      }
    }
    return buf.toString();
  }

  /**
   * Converts the parameter to its gemfire-style default field name.  For
   * example, <code>passwordFile</code> converts to
   * <code>DEFAULT_PASSWORD_FILE</code>.
   */
  protected static String convertPrmDefault(String prm) {
    StringBuffer buf = new StringBuffer();
    char[] chars = prm.toCharArray();
    for (int i = 0; i < chars.length; i++) {
      if (Character.isUpperCase(chars[i])) {
        if (i != 0) {
          buf.append("_");
        }
        buf.append(chars[i]);
      } else {
        buf.append(Character.toUpperCase(chars[i]));
      }
    }
    return "DEFAULT_" + buf;
  }

  /**
   * Returns the fully expanded path to the file for the given string.
   *
   * @throws HydraConfigException if the file does not exist.
   */
  protected static String getPath(Long key, String val) {
    HostDescription hd = null;
    String clientName = System.getProperty(ClientPrms.CLIENT_NAME_PROPERTY);
    if (clientName == null) { // use the master host info
      hd = TestConfig.getInstance().getMasterDescription()
                     .getVmDescription().getHostDescription();
    } else { // use the client host info
      hd = TestConfig.getInstance().getClientDescription(clientName)
                     .getVmDescription().getHostDescription();
    }
    String path = EnvHelper.expandEnvVars(val, hd);
    if (!FileUtil.exists(path)) {
      String s = "File " + BasePrms.nameForKey(key) + " not found: " + path;
      throw new HydraConfigException(s);
    }
    return path;
  }

  /**
   * Loads the class with the given name.
   * @throws HydraRuntimeException if not found.
   */
  protected static Class loadClass(String classname) {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      return Class.forName(classname, true, cl);
    } catch (ClassNotFoundException e) {
      String s = "Class not found: " + classname;
      throw new HydraRuntimeException(s);
    }
  }

  /**
   * Parses the given string into a class and method.  Validates that the
   * method is a valid public static method with the specified arg types
   * and return type.  Returns the input string.  Accepts null and empty string.
   *
   * @throws HydraConfigException if the class or method is not found.
   */
  protected static String parseMethod(Long key, String val,
                                      Class[] argTypes, Class returnType) {
    if (val != null && val.length() != 0) {
      int last = val.lastIndexOf(".");
      String classname = val.substring(0, last);
      Class cls = getClass(key, classname);
      String methodname = val.substring(last + 1, val.length());
      Method method = getMethod(key, cls, methodname, argTypes, returnType);
    }
    return val;
  }

//------------------------------------------------------------------------------
// SSL configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the SSL description for the given string.
   * @throws HydraConfigException if the given string is not listed in {@link
   *         SSLPrms#names}.
   */
  protected static SSLDescription getSSLDescription(
                                  String str, Long key, TestConfig config) {
    SSLDescription sd = config.getSSLDescription(str);
    if (sd == null) {
      String s = BasePrms.nameForKey(key) + " not found in "
               + BasePrms.nameForKey(SSLPrms.names) + ": " + str;
      throw new HydraConfigException(s);
    } else {
      return sd;
    }
  }
}
