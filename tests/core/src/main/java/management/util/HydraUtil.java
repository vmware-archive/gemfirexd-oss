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
package management.util;

import hydra.Log;
import hydra.Prms;
import hydra.TestConfig;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.TabularDataSupport;

import management.jmx.JMXPrms;
import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.query.CqEvent;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonException;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;


@SuppressWarnings({"unchecked", "rawtypes"})
public class HydraUtil {
  
  public static final String NEW_LINE = System.getProperty("line.separator");
  public static final String TAB = "\t";
  public static void createLogWriter(){
    Log.createLogWriter("hydrautil", "fine");
  }

  public static void logInfo(String message) {
    LogWriter logger = Log.getLogWriter();
    logger.info(message);
  }

  public static void logFine(String message) {
    LogWriter logger = Log.getLogWriter();
    logger.fine(message);
  }
  
  public static void logFinest(String message) {
    LogWriter logger = Log.getLogWriter();
    logger.finest(message);
  }


  public static void logError(String message) {
    LogWriter logger = Log.getLogWriter();
    logger.error(message);
  }

  public static void logError(String message, Throwable e) {
    LogWriter logger = Log.getLogWriter();
    logger.error(message, e);
  }

  public static void logErrorAndRaiseException(String message, Throwable e) throws TestException {
    LogWriter logger = Log.getLogWriter();
    logger.error(message, e);
    throw new TestException(message, e);
  }

  public static void logErrorAndRaiseException(String string) {
    logError(string);
    throw new TestException(string);
  }

  public static void logInfo(String message, Throwable e) {
    LogWriter logger = Log.getLogWriter();
    logger.info(message, e);
  }

  
  public static String ObjectToString(Object object) {

    if (object == null)
      return "<<NULL>>";
    if (object.getClass().isArray()) {
      String type = object.getClass().getComponentType().toString();
      if(!isPrimitive(type)){
        StringBuilder sb = new StringBuilder();
        sb.append("Array [");
        Object a[] = (Object[]) object;
        for (Object aa : a)
          sb.append(ObjectToString(aa)).append(", ");
        sb.append("]");
        return sb.toString();
      }else{
        //primitive
        return handlePrimitiveArray(object,type);
      }
    }else if (object instanceof CommandResult){
    	return handleCommandResult((CommandResult)object);
    }
    else if (object instanceof AttributeList){
      AttributeList list = (AttributeList) object;
      StringBuilder sb = new StringBuilder();
      sb.append(" JMXAttributeList [").append(NEW_LINE);     
      for(Object a : list){
        Attribute attr = (Attribute)a;
        Object key = attr.getName();
        Object value = attr.getValue();
        sb.append(TAB).append("[").append("Attribute:").append(ObjectToString(key))
          .append(" Value:").append(ObjectToString(value)).append("]").append(NEW_LINE);
      }
      sb.append("]");
      return sb.toString();
    }
    else if (object instanceof Map){
      Map map = (Map) object;
      StringBuilder sb = new StringBuilder();
      sb.append(" Map [").append(NEW_LINE);
      Set<Entry> set = map.entrySet();
      for(Map.Entry e : set){
        Object key = e.getKey();
        Object value = e.getValue();
        sb.append(TAB).append("[").append("K:").append(ObjectToString(key))
          .append(" V:").append(ObjectToString(value)).append("]").append(NEW_LINE);
      }
      sb.append("]");
      return sb.toString();
    }
    else if (object instanceof CqEvent) {
      CqEvent event = (CqEvent) object;
      StringBuilder sb = new StringBuilder();
      sb.append("CqEvent [");
      sb.append(" QueryName=<").append(event.getCq().getName());
      sb.append("> BaseOperation=").append(event.getBaseOperation());
      sb.append(" QueryOperation=").append(event.getQueryOperation());
      sb.append(" Key=").append(event.getKey());
      sb.append(" NewValue=").append(event.getNewValue());
      sb.append(" ]");
      return sb.toString();
    }
    else if (object instanceof TabularDataSupport){
      Map map = (Map) object;
      return ObjectToString(map);
    }
    else if(object instanceof CompositeData){
      StringBuilder sb = new StringBuilder();
      CompositeData data = (CompositeData) object;
      CompositeType type = data.getCompositeType();
      Set<String> keys = type.keySet();
      sb.append("CompositeData type - " + type.getTypeName()).append(NEW_LINE)
      .append(" description " + type.getDescription()).append(NEW_LINE)
      .append(" data ").append(NEW_LINE);
      for(String key : keys){
        if(data.containsKey(key)){
          sb.append(TAB).append("[ K: " + key).append(" V :" + ObjectToString(data.get(key))).append("]").append(NEW_LINE);
        }
      }
      sb.append(NEW_LINE);
      return sb.toString();
    }if (object instanceof List) {
      List list = (List)object;
      StringBuilder sb = new StringBuilder();
      sb.append(" List [").append(NEW_LINE);     
      for(Object a : list){
        sb.append(TAB).append(ObjectToString(a)).append(NEW_LINE);
      }
      sb.append("]");
      return sb.toString();
    }
    else
      return object.toString();
  }

  private static boolean isPrimitive(String type) {
    String primitiveTypes[] = { "int", "double", "byte", "boolean", "char", "float" };
    for(String s : primitiveTypes)
      if(s.equals(type))
        return true;
    return false;
  }

  private static String handleCommandResult(CommandResult object) {
	StringBuilder sb = new StringBuilder();
	sb.append(" Status : " + object.getStatus()).append(NEW_LINE);
	try {
		sb.append(" Raw JSON : " + object.getContent().toIndentedString(5));
		sb.append(NEW_LINE);
	} catch (GfJsonException e) {
		sb.append(" Error getting raw JSON : " + e.getMessage());
		logError(" Error getting raw JSON : ", e);
	}
	return sb.toString();
}

private static String handlePrimitiveArray(Object object, String type) {
    StringBuilder sb = new StringBuilder();
    
    if("int".equals(type)){
      int a[] = (int[])object;
      sb.append(" intArray[");
      for(int i : a){
        sb.append(i).append(",");
      }
      sb.append("]");
    }else if("double".equals(type)){
      double a[] = (double[])object;
      sb.append(" doubleArray[");
      for(double i : a){
        sb.append(i).append(",");
      }
      sb.append("]");
    }else if("byte".equals(type)){
      byte a[] = (byte[])object;
      sb.append(" byteArray[");
      for(byte i : a){
        sb.append(i).append(",");
      }
      sb.append("]");
    }else if("boolean".equals(type)){
      boolean a[] = (boolean[])object;
      sb.append(" booleanArray[");
      for(boolean i : a){
        sb.append(i).append(",");
      }
      sb.append("]");
    }else if("char".equals(type)){
      char a[] = (char[])object;
      sb.append(" charArray[");
      for(char i : a){
        sb.append(i).append(",");
      }
      sb.append("]");
    }
    return sb.toString();
  }

  public static boolean isConcurrentTest() {
    if(runninghydra()){
    return !TestConfig.tab().booleanAt(Prms.serialExecution);
    }else return false;
  }

  public static boolean isSerialTest() {
    if(runninghydra()){
      return TestConfig.tab().booleanAt(Prms.serialExecution);
    }else return true;
  }

  public static void main(String[] args) {

    Object a[] = { "sgjdkfg", 1232, 1234.5454, true };

    System.out.println(ObjectToString(a));

  }

  public static Object getInstanceOfClass(String klass) {
    try {
      Class cklass = Class.forName(klass);
      Object object;
      object = cklass.newInstance();
      return object;
    } catch (InstantiationException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (IllegalAccessException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (ClassNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  public static String getStackTraceAsString(Throwable throwable) {
    final Writer result = new StringWriter();
    final PrintWriter printWriter = new PrintWriter(result);
    if (throwable != null)
      throwable.printStackTrace(printWriter);
    else {
      try {
        result.append("<No StackTrace>");
      } catch (IOException e) {
        // ignore its just in-memory writer
      }
    }
    return result.toString();
  }
  
 
  
  private static Random localRandom = new Random();
  private static Random getRandomGen() {
    if(runninghydra())
    return TestConfig.tab().getRandGen();
    else {
      return localRandom;
    }
  }

  public static boolean runninghydra() {
    try{
      hydra.TestConfig.tab();
      return true;
    }catch(Exception e){
      return false;
    }
    
  }
  
  public static  <V> V getRandomElement(List<V> coll){       
    int size = coll.size();
    if(runninghydra()){
      int randomElement=0;
      if(size>1){
        size = size-1;
        randomElement = getRandomGen().nextInt(size);
      }
      else
        randomElement = 0;      
      return coll.get(randomElement);
    }else{
      int randomElement=0;
      if(size>1){
        size = size-1;
        randomElement = getRandomGen().nextInt(size);
      }
      else randomElement = 0;       
      return coll.get(randomElement);
    }    
  }

  public static  <V> V getRandomElement(V[] coll){
    int size = coll.length;
    if(runninghydra()){
      int randomElement=0;
      if(size>1){
        size = size-1;
        randomElement = getRandomGen().nextInt(size);
      }
      else
        randomElement = 0;      
      return coll[randomElement];
    }else{
      int randomElement=0;
      if(size>1){
        size = size-1;
        randomElement = getRandomGen().nextInt(size);
      }
      else randomElement = 0;       
      return coll[randomElement];
    }
  }
  
  public static  <V> V getRandomElement(Set<V> coll){
    int size = coll.size();
    int randomElement=0;
    if(runninghydra()){      
      if(size>1){
        size = size-1;
        randomElement = getRandomGen().nextInt(size);
      }
      else
        randomElement = 0;            
    }else{     
      if(size>1){
        size = size-1;
        randomElement = getRandomGen().nextInt(size);
      }
      else randomElement = 0;
    }
    int i=0;
    for(V v : coll){      
      if(i==randomElement)
        return v;
      else i++;
    }
    return null;
  }
  
  public static  <V> V getRandomElement(Collection<V> coll){
    int size = coll.size();
    int randomElement=0;
    if(runninghydra()){      
      if(size>1){
        size = size-1;
        randomElement = getRandomGen().nextInt(size);
      }
      else
        randomElement = 0;            
    }else{     
      if(size>1){
        size = size-1;
        randomElement = getRandomGen().nextInt(size);
      }
      else randomElement = 0;
    }
    int i=0;
    for(V v : coll){      
      if(i==randomElement)
        return v;
      else i++;
    }
    return null;
  }
  
  public static int getnextRandomInt(int maxExclusive){
    return TestConfig.tab().getRandGen().nextInt(maxExclusive-1);
  }
  
  public static int getnextNonZeroRandomInt(int maxExclusive){
    int randomInt = 0;
    while(randomInt==0)
     randomInt = getnextRandomInt(maxExclusive);    
    return randomInt;
  }
  
  public static boolean getRandomBoolean(){
    return TestConfig.tab().getRandGen().nextBoolean();
  }
  
  public static void sleepForReplicationJMX() {
    try {
      long waitTime = ManagementConstants.REFRESH_TIME*TestConfig.tab().longAt(JMXPrms.sleepTimeFactor);
      Log.getLogWriter().info("Sleeping for " + waitTime + " ms for JMX Replication ");
      Thread.sleep(waitTime);
    } catch (InterruptedException e) {
    }
  }

  public static void sleepForDataUpdaterJMX(long time) {
    try {
      long waitTime = time * 1000 * TestConfig.tab().longAt(JMXPrms.sleepTimeFactor);
      Log.getLogWriter().info("Sleeping for " + waitTime + " ms for mbean to update ");
      Thread.sleep(waitTime);
    } catch (InterruptedException e) {
    }
  }
  
 public static void threadDump() {
	java.lang.management.ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();
	ThreadInfo infos[] = mxBean.dumpAllThreads(true, true);
	StringBuilder sb = new StringBuilder();
	for(ThreadInfo info : infos){
		sb.append(info);		
	}
	logInfo(sb.toString());
 }
 
 public static String generateNamedDoubleSuffixedNames(String prefix, int n, int m, String s[], boolean varyFirst, boolean useComma) {
   String v = "";
   if (varyFirst) {
     for (int j = 1; j <= m; j++) {
       for (int i = 1; i <= n; i++) {
         //v += prefix + "_" + s[i] + i + "_" + j;
         v += prefix + "_" + s[i-1]  + "_" + j;
         if (i*j < m*n) {
           if (useComma) v += ",";
           v += " ";
         }
       }
     }
   } else {
     for (int i = 1; i <= n; i++) {
       for (int j = 1; j <= m; j++) {
         v += prefix + "_" + i + "_" + s[j-1];// + j;
         if (i*j < m*n) {
           if (useComma) v += ",";
           v += " ";
         }
       }
     }
   }
   return v;
 }
 
 public static String generateNamedDoubleSuffixedNames(String prefix, int n, int m, String s, boolean varyFirst, boolean useComma) {
   String array[] = s.split("\\|");
   return generateNamedDoubleSuffixedNames(prefix, n, m, array, varyFirst, useComma);
 }
 
 @SuppressWarnings({ "rawtypes", "unchecked" })
 public static Object copyMap(Map map) {
   Map map2 = new HashMap();    
   Set<Map.Entry> set = map.entrySet();
   for(Map.Entry e : set){
     map2.put(e.getKey(), e.getValue());
   }
   return map2;
 }
  
}
