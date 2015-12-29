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
package com.gemstone.gemfire.pdx.internal.json; 
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;


import org.codehaus.jackson.*;
import org.codehaus.jackson.JsonGenerator.Feature;
import org.codehaus.jackson.impl.DefaultPrettyPrinter;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.pdx.FieldType;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.internal.PdxField;

/*
 * This class converts PdxInstance into JSON document.
 */
public class PdxToJSON 
{
  public static boolean PDXTOJJSON_UNQUOTEFIELDNAMES = Boolean.getBoolean("pdxToJson.unQuoteFieldNames");
  private PdxInstance m_pdxInstance;
  public PdxToJSON(PdxInstance pdx)
  {
    m_pdxInstance = pdx;
  }
  
  public String getJSON()
  {
    JsonFactory jf = new JsonFactory();
   // OutputStream os = new ByteArrayOutputStream();
    HeapDataOutputStream hdos = new HeapDataOutputStream(com.gemstone.gemfire.internal.shared.Version.CURRENT);
    try {
      JsonGenerator jg = jf.createJsonGenerator(hdos, JsonEncoding.UTF8);
      enableDisableJSONGeneratorFeature(jg);
      getJSONString(jg, m_pdxInstance);
      jg.close();
      return new String(hdos.toByteArray());
    } catch (IOException e) {
      // TODO Auto-generated catch block
      throw new RuntimeException(e.getMessage());
    }finally{
        hdos.close();
    }
  }
  
  public byte[] getJSONByteArray()
  {
    JsonFactory jf = new JsonFactory();
    HeapDataOutputStream hdos = new HeapDataOutputStream(com.gemstone.gemfire.internal.shared.Version.CURRENT);
    try {
      JsonGenerator jg = jf.createJsonGenerator(hdos, JsonEncoding.UTF8);
      enableDisableJSONGeneratorFeature(jg);
      getJSONString(jg, m_pdxInstance);
      jg.close();      
      return hdos.toByteArray();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      throw new RuntimeException(e.getMessage());
    }finally {
      hdos.close();
    }    
  }
  
  private void enableDisableJSONGeneratorFeature(JsonGenerator jg) {
    jg.enable(Feature.ESCAPE_NON_ASCII);
    jg.disable(Feature.AUTO_CLOSE_TARGET); 
    jg.setPrettyPrinter(new DefaultPrettyPrinter());
    if(PDXTOJJSON_UNQUOTEFIELDNAMES)
      jg.disable(Feature.QUOTE_FIELD_NAMES);
  }
  
  private String getJSONString(JsonGenerator jg, PdxInstance pdxInstance) throws JsonGenerationException, IOException
  {
    jg.writeStartObject();
    
    List<String> pdxFields = pdxInstance.getFieldNames();
    
    for (String pf : pdxFields)
    {
      Object value = pdxInstance.getField(pf);
      if(value == null)
      {
        jg.writeFieldName(pf);
        jg.writeNull();
      }
      else if(value.getClass().equals(Boolean.class))
      {
        jg.writeFieldName(pf);
        boolean b = (Boolean)value;
        jg.writeBoolean(b);  
      }
      else if(value.getClass().equals(Byte.class))
      {
        jg.writeFieldName(pf);
        Byte b = (Byte)value;
        jg.writeNumber(b);  
      }
      else if(value.getClass().equals(Short.class))
      {
        jg.writeFieldName(pf);
        Short b = (Short)value;
        jg.writeNumber(b);  
      }
      else if(value.getClass().equals(Integer.class))
      {
        jg.writeFieldName(pf);
        int i = (Integer)value;
        jg.writeNumber(i);
      }
      else if(value.getClass().equals(Long.class))
      {
        jg.writeFieldName(pf);
        long i = (Long)value;
        jg.writeNumber(i);
      }
      else if(value.getClass().equals(BigInteger.class))
      {
        jg.writeFieldName(pf);
        BigInteger i = (BigInteger)value;
        jg.writeNumber(i);
      }
      else if(value.getClass().equals(Double.class))
      {
        jg.writeFieldName(pf);
        double d = (Double)value;
        jg.writeNumber(d);
      }
      else if(value.getClass().equals(String.class))
      {
        jg.writeFieldName(pf);
        String s = (String)value;
        jg.writeString(s);
      }
      else 
      {
        if(value instanceof PdxInstance)
        {
          jg.writeFieldName(pf);
          getJSONString(jg, (PdxInstance)value);
        }
        else if(value instanceof List)
        {
          jg.writeFieldName(pf);
          getJSONStringFromList(jg, (List)value);
        }
        else
        {
          throw new IllegalStateException("PdxInstance returns unknwon pdxfield " + pf + " for type " + value);
        }
      } 
      
      
    }
    jg.writeEndObject();
    return null;
  }

  private void getJSONStringFromList(JsonGenerator jg, List list) throws JsonGenerationException, IOException
  {
    jg.writeStartArray();
    
    for (Object obj : list)
    {
      if(obj == null)
      {
        jg.writeNull();
      }
      else if(obj.getClass().equals(Boolean.class))
      {
        jg.writeBoolean(((Boolean)obj));  
      }
      else if(obj.getClass().equals(Byte.class))
      {
        byte i = (Byte)obj;
        jg.writeNumber(i);
      }
      else if(obj.getClass().equals(Short.class))
      {
        short i = (Short)obj;
        jg.writeNumber(i);
      }
      else if(obj.getClass().equals(Integer.class))
      {
        int i = (Integer)obj;
        jg.writeNumber(i);
      }
      else if(obj.getClass().equals(Long.class))
      {
        long i = (Long)obj;
        jg.writeNumber(i);
      }
      else if(obj.getClass().equals(BigInteger.class))
      {
        BigInteger i = (BigInteger)obj;
        jg.writeNumber(i);
      }
      else if(obj.getClass().equals(Float.class))
      {
        float i = (Float)obj;
        jg.writeNumber(i);
      }
      else if(obj.getClass().equals(Double.class))
      {
        double d = (Double)obj;
        jg.writeNumber(d);
      }
      else if(obj.getClass().equals(BigDecimal.class))
      {
        BigDecimal i = (BigDecimal)obj;
        jg.writeNumber(i);
      }
      else if(obj.getClass().equals(String.class))
      {
        String s = (String)obj;
        jg.writeString(s);
      }
      else if(obj.getClass().equals(List.class))
      {
        getJSONStringFromList(jg, (List)list);
      }
      else if(obj instanceof PdxInstance)
      {
          getJSONString(jg, (PdxInstance)obj);
      }
      else
      {
        throw new IllegalStateException("List has unknown field " + obj.getClass().toString());
      }
      
    }
    jg.writeEndArray();    
  }
}
