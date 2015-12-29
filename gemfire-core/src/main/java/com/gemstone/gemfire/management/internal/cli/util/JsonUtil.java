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
package com.gemstone.gemfire.management.internal.cli.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonArray;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonException;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonObject;
import com.gemstone.gemfire.management.internal.cli.result.CliJsonSerializable;
import com.gemstone.gemfire.management.internal.cli.result.CliJsonSerializableFactory;
import com.gemstone.gemfire.management.internal.cli.result.ResultDataException;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * This class contains utility methods for JSON (http://www.json.org/) which is 
 * used by classes used for the Command Line Interface (CLI).
 * 
 * @author Abhishek Chaudhari
 * 
 * @since 7.0
 */
public class JsonUtil {

  /**
   * Converts given JSON String in to a Map. 
   * Refer http://www.json.org/ to construct a JSON format.
   * 
   * @param jsonString
   *          jsonString to be converted in to a Map.
   * @return a Map created from
   * 
   * @throws IllegalArgumentException
   *           if the specified JSON string can not be converted in to a Map
   */
  public static Map<String, String> jsonToMap(String jsonString) {
    Map<String, String> jsonMap = new TreeMap<String, String>();
    try {
      GfJsonObject jsonObject = new GfJsonObject(jsonString);
      Iterator<String> keys = jsonObject.keys();
      
      while (keys.hasNext()) {
        String key = keys.next();
        jsonMap.put(key, jsonObject.getString(key));
      }
      
    } catch (GfJsonException e) {
      throw new IllegalArgumentException("Could not convert jsonString : '"+jsonString+"' to map.");
    }
    return jsonMap;
  }
  
  /**
   * Converts given Map in to a JSON string representing a Map. 
   * Refer http://www.json.org/ for more.
   * 
   * @param properties a Map of Strings to be converted in to JSON String
   * @return a JSON string representing the specified Map.
   */
  public static String mapToJson(Map<String, String> properties) {
    return new GfJsonObject(properties).toString();
  }
  
  /**
   * Converts given Object in to a JSON string representing an Object. 
   * Refer http://www.json.org/ for more.
   * 
   * @param object an Object to be converted in to JSON String
   * @return a JSON string representing the specified object.
   */
  public static String objectToJson(Object object) {    
    return new GfJsonObject(object).toString();
  }  

  /**
   * Converts given Object in to a JSON string representing an Object. 
   * If object contains an attribute which itself is another object
   * it will be displayed as className if its json representation
   * exceeds the length
   * 
   * @param object an Object to be converted in to JSON String
   * @return a JSON string representing the specified object.
   */
  public static String objectToJsonNested(Object object, int length) {
    
    GfJsonObject jsonObject = new GfJsonObject(object);
    Iterator<String> iterator = jsonObject.keys();
    while(iterator.hasNext()){
      String key = iterator.next();
      Object value = jsonObject.get(key);
      if(value!=null && !isPrimitiveOrWrapper(value.getClass())){
        GfJsonObject jsonified = new GfJsonObject(value);
        String stringified = jsonified.toString();
        try{
        if(stringified.length()>length){          
          jsonObject.put(key,jsonified.getType());
        }else{
          jsonObject.put(key, stringified);
        }
        }catch (GfJsonException e) {          
          e.printStackTrace();          
        }
      }
    }    
    return jsonObject.toString();
  }
  
  /**
   * Converts given JSON String in to a Object. 
   * Refer http://www.json.org/ to construct a JSON format.
   * 
   * @param jsonString
   *          jsonString to be converted in to a Map.
   * @return an object constructed from given JSON String
   * 
   * @throws IllegalArgumentException
   *           if the specified JSON string can not be converted in to an Object
   */
  public static <T> T jsonToObject(String jsonString, Class<T> klass) {
    T objectFromJson = null;
    try {
      GfJsonObject jsonObject = new GfJsonObject(jsonString);
      objectFromJson = klass.newInstance();
      Method[] declaredMethods = klass.getDeclaredMethods();
      Map<String, Method> methodsMap = new HashMap<String, Method>();
      for (Method method : declaredMethods) {
        methodsMap.put(method.getName(), method);
      }
      
      int noOfFields = jsonObject.size();
      Iterator<String> keys = jsonObject.keys();
      
      while (keys.hasNext()) {
        String key = keys.next();
        Method method = methodsMap.get("set"+capitalize(key));
        if (method != null) {
          Class<?>[] parameterTypes = method.getParameterTypes();
          if (parameterTypes.length == 1) {
            Class<?> parameterType = parameterTypes[0];
            
            Object value = jsonObject.get(key);
            if (isPrimitiveOrWrapper(parameterType)) {
              value = getPrimitiveOrWrapperValue(parameterType, value);
            } else {
              value = jsonToObject(value.toString(), parameterType);
            }
            method.invoke(objectFromJson, new Object[] {value});
            noOfFields--;
          }
          
        }
      }
      
      if (noOfFields != 0) {
        throw new IllegalArgumentException("Not enough setter methods for fields in given JSON String : "+jsonString+" in class : "+klass);
      }
      
    } catch (InstantiationException e) {
      throw new IllegalArgumentException("Couldn't convert JSON to Object of type "+klass, e);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Couldn't convert JSON to Object of type "+klass, e);
    } catch (GfJsonException e) {
      throw new IllegalArgumentException("Couldn't convert JSON to Object of type "+klass, e);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Couldn't convert JSON to Object of type "+klass, e);
    } catch (InvocationTargetException e) {
      throw new IllegalArgumentException("Couldn't convert JSON to Object of type "+klass, e);
    }
    return objectFromJson;
  }  
  
  
  public static Object jsonToObject(String jsonString) {
    Object objectFromJson = null;
    try {
      GfJsonObject jsonObject = new GfJsonObject(jsonString);
      
      Iterator<String> keys = jsonObject.keys();
      
      Object[] arr = new Object[jsonObject.size()];
      int i = 0;
      
      while(keys.hasNext()) {
        String key = keys.next();
        Class<?> klass = ClassPathLoader.getLatest().forName(key);
        arr[i++] = jsonToObject((String)jsonObject.get(key).toString(), klass);
      }
      
      if (arr.length == 1) {
        objectFromJson = arr[0];
      } else {
        objectFromJson = arr;
      }
    } catch (GfJsonException e) {
      throw new IllegalArgumentException("Couldn't convert JSON to Object.", e);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Couldn't convert JSON to Object.", e);
    }
    
    return objectFromJson;
  }
  
  public static String capitalize(String str) {
    String capitalized = str;
    if (str == null || str.isEmpty()) {
      return capitalized;
    }
    capitalized = String.valueOf(str.charAt(0)).toUpperCase() + str.substring(1);
    
    return capitalized;
  }
  
  public static boolean isPrimitiveOrWrapper(Class<?> klass) {
    return klass.isAssignableFrom(Byte.class)
        || klass.isAssignableFrom(byte.class)
        || klass.isAssignableFrom(Short.class)
        || klass.isAssignableFrom(short.class)
        || klass.isAssignableFrom(Integer.class)
        || klass.isAssignableFrom(int.class)
        || klass.isAssignableFrom(Long.class)
        || klass.isAssignableFrom(long.class)
        || klass.isAssignableFrom(Float.class)
        || klass.isAssignableFrom(float.class)
        || klass.isAssignableFrom(Double.class)
        || klass.isAssignableFrom(double.class)
        || klass.isAssignableFrom(Boolean.class)
        || klass.isAssignableFrom(boolean.class)
        || klass.isAssignableFrom(String.class);
  }
  
  public static Object getPrimitiveOrWrapperValue(Class<?> klass, Object value) {
    if (klass.isAssignableFrom(Byte.class) || klass.isAssignableFrom(byte.class)) {
      return value;
    } else if (klass.isAssignableFrom(Short.class) || klass.isAssignableFrom(short.class)) {
      return value;
    } else if (klass.isAssignableFrom(Integer.class) || klass.isAssignableFrom(int.class)) {
      return value;
    } else if (klass.isAssignableFrom(Long.class) || klass.isAssignableFrom(long.class)) {
      return value;
    } else if (klass.isAssignableFrom(Float.class) || klass.isAssignableFrom(Float.class)) {
      return value;
    } else if (klass.isAssignableFrom(Double.class) || klass.isAssignableFrom(double.class)) {
      return value;
    } else if (klass.isAssignableFrom(Boolean.class) || klass.isAssignableFrom(boolean.class)) {
      return value;
    } else if (klass.isAssignableFrom(String.class)) {
      return String.valueOf(value);
    } else {
      return null;
    }
  }
  
  public static int getInt(GfJsonObject jsonObject, String byName) {
    return jsonObject.getInt(byName);
  }
  
  public static long getLong(GfJsonObject jsonObject, String byName) {
    return jsonObject.getLong(byName);
  }
  
  public static double getDouble(GfJsonObject jsonObject, String byName) {
    return jsonObject.getDouble(byName);
  }
  
  public static boolean getBoolean(GfJsonObject jsonObject, String byName) {
    return jsonObject.getBoolean(byName);
  }
  
  public static String getString(GfJsonObject jsonObject, String byName) {
    return jsonObject.getString(byName);
  }
  
  public static GfJsonObject getJSONObject(GfJsonObject jsonObject, String byName) {
    return jsonObject.getJSONObject(byName);
  }
  
  public static String[] getStringArray(GfJsonObject jsonObject, String byName) {
    String[] stringArray = null;
    try {
      GfJsonArray jsonArray = jsonObject.getJSONArray(byName);
      stringArray = GfJsonArray.toStringArray(jsonArray);
    } catch (GfJsonException e) {
      throw new ResultDataException(e.getMessage());
    }
    return stringArray;
  }
  
  public static byte[] getByteArray(GfJsonObject jsonObject, String byName) {
    byte[] byteArray = null;
    try {
      GfJsonArray jsonArray = jsonObject.getJSONArray(byName);
      byteArray = GfJsonArray.toByteArray(jsonArray);
    } catch (GfJsonException e) {
      throw new ResultDataException(e.getMessage());
    }
    return byteArray;
  }
  
  public static List<CliJsonSerializable> getList(GfJsonObject jsonObject, String byName) {
    List<CliJsonSerializable> cliJsonSerializables = Collections.emptyList();
    try {
      GfJsonArray cliJsonSerializableArray = jsonObject.getJSONArray(byName);
      int size = cliJsonSerializableArray.size();
      if (size > 0) {
        cliJsonSerializables = new ArrayList<CliJsonSerializable>();
      }
      for (int i = 0; i < size; i++) {
        GfJsonObject cliJsonSerializableState = cliJsonSerializableArray.getJSONObject(i);
        int jsId = cliJsonSerializableState.getInt(CliJsonSerializable.JSID);
        CliJsonSerializable cliJsonSerializable = CliJsonSerializableFactory.getCliJsonSerializable(jsId);
        cliJsonSerializable.fromJson(cliJsonSerializableState);
        cliJsonSerializables.add(cliJsonSerializable);
      }
    } catch (GfJsonException e) {
      throw new ResultDataException(e.getMessage());
    }
    return cliJsonSerializables;
  }
  
  public static Set<CliJsonSerializable> getSet(GfJsonObject jsonObject, String byName) {
    Set<CliJsonSerializable> cliJsonSerializables = Collections.emptySet();
    try {
      GfJsonArray cliJsonSerializableArray = jsonObject.getJSONArray(byName);
      int size = cliJsonSerializableArray.size();
      if (size > 0) {
        cliJsonSerializables = new HashSet<CliJsonSerializable>();
      }
      for (int i = 0; i < size; i++) {
        GfJsonObject cliJsonSerializableState = cliJsonSerializableArray.getJSONObject(i);
        int jsId = cliJsonSerializableState.getInt(CliJsonSerializable.JSID);
        CliJsonSerializable cliJsonSerializable = CliJsonSerializableFactory.getCliJsonSerializable(jsId);
        cliJsonSerializable.fromJson(cliJsonSerializableState);
        cliJsonSerializables.add(cliJsonSerializable);
      }
    } catch (GfJsonException e) {
      throw new ResultDataException(e.getMessage());
    }
    return cliJsonSerializables;
  }
  
  
  // For testing purpose
  public static void main(String[] args) {
    System.out.println(capitalize("key"));
    System.out.println(capitalize("Key"));
    
    String str = "{\"com.gemstone.gemfire.management.internal.cli.JsonUtil$Employee\":{\"id\":1234,\"name\":\"Foo BAR\",\"department\":{\"id\":456,\"name\":\"support\"}}}";
    Object jsonToObject = jsonToObject(str);
    System.out.println(jsonToObject);
    
    str = "{\"id\":1234,\"name\":\"Foo BAR\",\"department\":{\"id\":456,\"name\":\"support\"}}";
    Object jsonToObject2 = jsonToObject(str, Employee.class);
    System.out.println(jsonToObject2);
  }
  
  public static class Employee {
    private int id;
    private String name;
    private Department department;
    
    public int getId() {
      return id;
    }
    public void setId(int id) {
      this.id = id;
    }
    public String getName() {
      return name;
    }
    public void setName(String name) {
      this.name = name;
    }
    public Department getDepartment() {
      return department;
    }
    public void setDepartment(Department department) {
      this.department = department;
    }
    @Override
    public String toString() {
      return "Employee [id=" + id + ", name=" + name + ", department="
          + department + "]";
    }
  }
  
  public static class Department {
    private int id;
    private String name;
    
    public int getId() {
      return id;
    }
    public void setId(int id) {
      this.id = id;
    }
    public String getName() {
      return name;
    }
    public void setName(String name) {
      this.name = name;
    }
    @Override
    public String toString() {
      return "Department [id=" + id + ", name=" + name + "]";
    }
  }

}
