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
package com.gemstone.gemfire.management.internal.cli.json;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.internal.StructImpl;
import com.gemstone.gemfire.pdx.PdxInstance;

/**
 * A limited functionality JSON parser. Its a DSF based JSON parser. It does not
 * create Object maps and serialize them like JSONObject. It just traverses an Object graph in
 * depth first search manner and appends key values to a String writer.
 * Hence we prevent creating a lot of garbage.
 * 
 * Although it has limited functionality,still a simple use of add() method
 * should suffice for most of the simple JSON use cases.
 * 
 * @author rishim
 * 
 */
public class TypedJson {

  /**
   * Limit of collection length to be serialized in JSON format.
   */
  protected static int COLLECTION_ELEMENT_LIMMIT = 100;

  public static final Object NULL = GfJsonObject.NULL;

  /**
   * If Integer of Float is NAN
   */
  static final String NONFINITE = "Non-Finite";

  private Map seen = new java.util.IdentityHashMap();


  boolean commanate;

  private Map<String, List<Object>> map;

  public TypedJson(String key, Object value) {
    List<Object> list = new ArrayList<Object>();
    this.map = new LinkedHashMap<String, List<Object>>();
    if (value != null) {
      list.add(value);
    }
    this.map.put(key, list);

  }

  public TypedJson() {
    this.map = new LinkedHashMap<String, List<Object>>();
  }

  /**
   * 
   * User can build on this object by adding Objects against a key.
   * 
   * TypedJson result = new TypedJson(); result.add(KEY,object); If users add
   * more objects against the same key the newly added object will be appended
   * to the existing key forming an array of objects.
   * 
   * If the KEY is a new one then it will be a key map value.
   * 
   * @param key
   *          Key against which an object will be added
   * @param value
   *          Object to be added
   * @return TypedJson object
   */
  public TypedJson add(String key, Object value) {
    List<Object> list = this.map.get(key);
    if (list != null) {
      list.add(value);
    } else {
      list = new ArrayList<Object>();
      list.add(value);
      this.map.put(key, list);
    }
    return this;
  }

  public String toString() {
    StringWriter w = new StringWriter();
    synchronized (w.getBuffer()) {
      try {
        return this.write(w).toString();
      } catch (GfJsonException e) {
        return null;
      }
    }
  }

  public int length() {
    return this.map.size();
  }

  Writer write(Writer writer) throws GfJsonException {
    try {
      boolean addComma = false;
      final int length = this.length();
      Iterator<String> keys = map.keySet().iterator();
      writer.write('{');

      if (length == 1) {
        Object key = keys.next();
        writer.write(quote(key.toString()));
        writer.write(':');

        writeList(writer, this.map.get(key));
      } else if (length != 0) {
        while (keys.hasNext()) {
          Object key = keys.next();
          if (addComma) {
            writer.write(',');
          }
          writer.write(quote(key.toString()));
          writer.write(':');

          writeList(writer, this.map.get(key));
          commanate = false;
          addComma = true;
        }

      }

      writer.write('}');

      return writer;
    } catch (IOException exception) {
      throw new GfJsonException(exception);
    }
  }

  Writer writeList(Writer writer, List<Object> myArrayList) throws GfJsonException {
    try {
      boolean addComma = false;
      int length = myArrayList.size();

      if (length == 0) {
        writeValue(writer, null);
      }
      if (length == 1) {
        writeValue(writer, myArrayList.get(0));

      } else if (length != 0) {
        writer.write('[');
        for (int i = 0; i < length; i += 1) {
          if (addComma) {
            writer.write(',');
          }
          writeValue(writer, myArrayList.get(i));
          commanate = false;
          addComma = true;
        }
        writer.write(']');
      }

      return writer;
    } catch (IOException e) {
      throw new GfJsonException(e);
    }
  }

  static String quote(String string) {
    StringWriter sw = new StringWriter();
    synchronized (sw.getBuffer()) {
      try {
        return quote(string, sw).toString();
      } catch (IOException ignored) {
        // will never happen - we are writing to a string writer
        return "";
      }
    }
  }

  static boolean visitChildren(Object object) {
    Class type = object.getClass();
    if (isPrimitiveOrWrapper(type)) {
      return false;
    }
    if (isSpecialObject(object)) {
      return false;
    }
    return true;
  }

  static boolean isPrimitiveOrWrapper(Class<?> klass) {
    return klass.isAssignableFrom(Byte.class) || klass.isAssignableFrom(byte.class)
        || klass.isAssignableFrom(Short.class) || klass.isAssignableFrom(short.class)
        || klass.isAssignableFrom(Integer.class) || klass.isAssignableFrom(int.class)
        || klass.isAssignableFrom(Long.class) || klass.isAssignableFrom(long.class)
        || klass.isAssignableFrom(Float.class) || klass.isAssignableFrom(float.class)
        || klass.isAssignableFrom(Double.class) || klass.isAssignableFrom(double.class)
        || klass.isAssignableFrom(Boolean.class) || klass.isAssignableFrom(boolean.class)
        || klass.isAssignableFrom(String.class);
  }

  static boolean isSpecialObject(Object object) {
    Class type = object.getClass();
    if (type.isArray() || type.isEnum()) {
      return true;
    }
    if ((object instanceof Collection) || (object instanceof Map) || (object instanceof PdxInstance)
        || (object instanceof Struct)) {
      return true;
    }
    return false;
  }

  final void writeVal(Writer w, Object value) throws IOException {
    w.write('{');
    addVal(w, value);
    w.write('}');
  }

  void addVal(Writer w, Object object) {
    if (object == null) {
      return;
    }
    boolean newObject = !seen.containsKey(object);
    if (newObject) {
      // seen.put(object, null); // TODO
      if (visitChildren(object)) {
        writeChildrens(w, object);
      }
    }
  }

  void writeKeyValue(Writer w, Object key, Object value, Class type) throws IOException {
    if (commanate) {
      w.write(",");
    }

    if (value == null || value.equals(null)) {
      w.write(quote(key.toString()));
      w.write(':');
      w.write("null");
      commanate = true;
      return;
    }
    Class clazz = value.getClass();
    w.write(quote(key.toString()));
    w.write(':');

    if (type != null) {
      writeType(w, type);
    }

    if (isPrimitiveOrWrapper(clazz)) {
      commanate = true;
      writePrimitives(w, value);
    } else if (isSpecialObject(value)) {
      commanate = false;
      writeSpecialObjects(w, value);
    } else {
      commanate = false;
      writeVal(w, value);
    }
    endType(w, clazz);
    return;
  }

  void writePrimitives(Writer w, Object value) throws IOException {
    if (value instanceof Number) {
      w.write(numberToString((Number) value));
      return;
    }

    w.write(quote(value.toString()));
  }

  void writeArray(Writer w, Object object) throws IOException {

    if (commanate) {
      w.write(",");
    }
    w.write('[');
    int length = Array.getLength(object);
    int elements = 0;
    for (int i = 0; i < length && elements < COLLECTION_ELEMENT_LIMMIT; i += 1) {
      Object item = Array.get(object, i);
      if (i != 0) {
        w.write(",");
      }
      Class clazz = item.getClass();

      if (isPrimitiveOrWrapper(clazz)) {
        writePrimitives(w, item);
      } else if (isSpecialObject(item)) {
        writeSpecialObjects(w, item);
      } else {
        writeVal(w, item);
      }
      elements++;
      commanate = false;
    }
    w.write(']');
    commanate = true;
    return;
  }

  void writeEnum(Writer w, Object object) throws IOException {

    if (commanate) {
      w.write(",");
    }
    w.write(quote(object.toString()));
    commanate = true;
    return;
  }

  void writeTypedJson(Writer w, TypedJson object) throws IOException {

    if (commanate) {
      w.write(",");
    }
    w.write(quote(object.toString()));
    commanate = true;
    return;
  }

  void writeValue(Writer w, Object value) {
    try {
      if (value == null || value.equals(null)) {
        w.write("null");
        return;
      }
      Class rootClazz = value.getClass();
      writeType(w, rootClazz);

      if (isPrimitiveOrWrapper(rootClazz)) {
        writePrimitives(w, value);
      } else if (isSpecialObject(value)) {
        writeSpecialObjects(w, value);
      } else {
        writeVal(w, value);
      }
      endType(w, rootClazz);
    } catch (IOException e) {
    }

  }

  void startKey(Writer writer, String key) throws IOException {
    if (key != null) {
      writer.write('{');
      writer.write(quote(key.toString()));
      writer.write(':');
    }
  }

  void endKey(Writer writer, String key) throws IOException {
    if (key != null) {
      writer.write('}');
    }

  }

  boolean writeSpecialObjects(Writer w, Object object) throws IOException {
    Class clazz = object.getClass();

    if (clazz.isArray()) {
      writeArray(w, object);
      return true;
    }
    if (clazz.isEnum()) {
      writeEnum(w, object);
      return true;
    }

    if (object instanceof TypedJson) {
      this.writeTypedJson(w, (TypedJson) object);
      return true;
    }

    if (object instanceof Collection) {
      Collection collection = (Collection) object;
      Iterator iter = collection.iterator();
      int elements = 0;
      w.write('{');
      while (iter.hasNext() && elements < COLLECTION_ELEMENT_LIMMIT) {
        Object item = iter.next();
        writeKeyValue(w, elements, item, item.getClass());
        elements++;
      }
      w.write('}');
      return true;
    }

    if (object instanceof Map) {
      Map map = (Map) object;
      Iterator i = map.entrySet().iterator();
      int elements = 0;
      w.write('{');
      while (i.hasNext() && elements < COLLECTION_ELEMENT_LIMMIT) {
        Map.Entry e = (Map.Entry) i.next();
        Object value = e.getValue();
        writeKeyValue(w, e.getKey(), value, value.getClass());
        elements++;
      }
      w.write('}');
      return true;
    }

    if (object instanceof PdxInstance) {
      PdxInstance pdxInstance = (PdxInstance) object;
      w.write('{');
      for (String field : pdxInstance.getFieldNames()) {
        Object fieldValue = pdxInstance.getField(field);
        writeKeyValue(w, field, fieldValue, fieldValue.getClass());

      }
      w.write('}');
      return true;
    }

    if (object instanceof Struct) {
      StructImpl impl = (StructImpl) object;
      String fields[] = impl.getFieldNames();
      Object[] values = impl.getFieldValues();
      Map<String, Object> structMap = new HashMap<String, Object>();

      w.write('{');
      for (int i = 0; i < fields.length; i++) {
        Object fieldValue = values[i];
        writeKeyValue(w, fields[i], fieldValue, fieldValue.getClass());
      }
      w.write('}');
      return true;
    }
    
    return false;
  }

  void writeType(Writer w, Class clazz) throws IOException {
    if (clazz != TypedJson.class) {
      w.write('[');
      w.write(quote(clazz.getCanonicalName()));
      w.write(",");
    }

  }

  void endType(Writer w, Class clazz) throws IOException {
    if (clazz != TypedJson.class) {
      w.write(']');
    }

  }

  void writeChildrens(Writer w, Object object) {
    Class klass = object.getClass();

    // If klass is a System class then set includeSuperClass to false.

    boolean includeSuperClass = klass.getClassLoader() != null;

    Method[] methods = includeSuperClass ? klass.getMethods() : klass.getDeclaredMethods();
    for (int i = 0; i < methods.length; i += 1) {
      try {
        Method method = methods[i];
        if (Modifier.isPublic(method.getModifiers()) && !Modifier.isStatic(method.getModifiers())) {
          String name = method.getName();
          String key = "";
          if (name.startsWith("get")) {
            if ("getClass".equals(name) || "getDeclaringClass".equals(name)) {
              key = "";
            } else {
              key = name.substring(3);
            }
          } else if (name.startsWith("is")) {
            key = name.substring(2);
          }
          if (key.length() > 0 && Character.isUpperCase(key.charAt(0)) && method.getParameterTypes().length == 0) {
            if (key.length() == 1) {
              key = key.toLowerCase();
            } else if (!Character.isUpperCase(key.charAt(1))) {
              key = key.substring(0, 1).toLowerCase() + key.substring(1);
            }
            method.setAccessible(true);
            Object result = method.invoke(object, (Object[]) null);
            writeKeyValue(w, key, result, method.getReturnType());
          }
        }
      } catch (Exception ignore) {
      }
    }
  }

  /**
   * Produce a string from a Number.
   * 
   * @param number
   *          A Number
   * @return A String.
   */
  public static String numberToString(Number number) {
    if (number == null) {
      return "";
    }
    if (number != null) {
      if (number instanceof Double) {
        if (((Double) number).isInfinite() || ((Double) number).isNaN()) {
          return "Non-Finite";
        }
      } else if (number instanceof Float) {
        if (((Float) number).isInfinite() || ((Float) number).isNaN()) {
          return "Non-Finite";
        }
      }
    }

    // Shave off trailing zeros and decimal point, if possible.

    String string = number.toString();
    if (string.indexOf('.') > 0 && string.indexOf('e') < 0 && string.indexOf('E') < 0) {
      while (string.endsWith("0")) {
        string = string.substring(0, string.length() - 1);
      }
      if (string.endsWith(".")) {
        string = string.substring(0, string.length() - 1);
      }
    }
    return string;
  }

  public static Writer quote(String string, Writer w) throws IOException {
    if (string == null || string.length() == 0) {
      w.write("\"\"");
      return w;
    }

    char b;
    char c = 0;
    String hhhh;
    int i;
    int len = string.length();

    w.write('"');
    for (i = 0; i < len; i += 1) {
      b = c;
      c = string.charAt(i);
      switch (c) {
      case '\\':
      case '"':
        w.write('\\');
        w.write(c);
        break;
      case '/':
        if (b == '<') {
          w.write('\\');
        }
        w.write(c);
        break;
      case '\b':
        w.write("\\b");
        break;
      case '\t':
        w.write("\\t");
        break;
      case '\n':
        w.write("\\n");
        break;
      case '\f':
        w.write("\\f");
        break;
      case '\r':
        w.write("\\r");
        break;
      default:
        if (c < ' ' || (c >= '\u0080' && c < '\u00a0') || (c >= '\u2000' && c < '\u2100')) {
          hhhh = "000" + Integer.toHexString(c);
          w.write("\\u" + hhhh.substring(hhhh.length() - 4));
        } else {
          w.write(c);
        }
      }
    }
    w.write('"');
    return w;
  }

}
