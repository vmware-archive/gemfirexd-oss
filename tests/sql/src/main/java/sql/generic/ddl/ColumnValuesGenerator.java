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
package sql.generic.ddl;

import static sql.generic.SqlUtilityHelper.getTableName;
import hydra.Log;
import hydra.TestConfig;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;

import sql.datagen.FKMappedColumn;
import sql.datagen.MappedColumnInfo;
import sql.datagen.Mapper;
import sql.datagen.ValueListMappedColumn;
import sql.generic.GenericBBHelper;
import sql.generic.SQLGenericPrms;
import sql.generic.SQLOldTest;
import util.TestException;
import util.TestHelper;

/**
 * ColumnValuesGenerator
 * 
 * @author Namrata Thanvi
 */

public class ColumnValuesGenerator {
  HashMap<String,MappedColumnInfo> columnNameMapping;
  
  
   ColumnValuesGenerator(Executor executor ) {
    try{
    String mapperFile = System.getProperty("JTESTS") + "/" + TestConfig.tab().stringAt(SQLGenericPrms.mapperFile,null);
    Mapper mapper = Mapper.getMapper();
    mapper.parseMapperFile(mapperFile, executor.getConnection());
    columnNameMapping =  mapper.getColumnNameMapping();
    } catch ( Exception e ){
      throw new TestException( e.getMessage() + " " + TestHelper.getStackTrace(e));
    }
  }
  
   
   public boolean  columnExistInMapper(String columnName){
     if (columnNameMapping.get(columnName) != null )
       return true;
     else
       return false;
     
   }
   
   
   public List<Object> getValueList (String columnName , int columnType , int defaultSize){      
      
     //if valueList already populated the take it from table.     
     TableInfo tableInfo = GenericBBHelper.getTableInfo(getTableName(columnName));
     if ( tableInfo !=null ) {
       ColumnInfo columnInfo = tableInfo.getColumn(columnName);
       return  columnInfo.getValueList();
     }
     
     List<Object> valueList;
      if (columnExistInMapper(columnName)) {
       
        valueList= getValueList(columnName, columnType);
      }
      else {
        int max = SQLOldTest.random.nextInt(defaultSize);
        if (max <= 0)
          max = 1;
        int min = SQLOldTest.random.nextInt(max);
        if (min <= 0)
          min = 1;
        valueList =  generateValueListBasedOntype(
            columnType, min, max);
      }
      return valueList;
   }
   
   List<Object> getValueList(String columnName , int columnType){
     MappedColumnInfo obj = columnNameMapping.get(columnName);
     if ( obj instanceof ValueListMappedColumn ){
       return getColumnValueList( columnName, columnType); 
     } else if ( obj instanceof FKMappedColumn ) {
        return getFKColumnValueList( columnName, columnType);
     } else
       return getColumnRandomValueList( columnName , columnType);
   }
   
   List<Object> getColumnRandomValueList(String columnName , int columnType ){
     MappedColumnInfo column = (MappedColumnInfo) columnNameMapping.get(columnName);
     int start = column.getMinLen();
     int end = column.getMaxLen();
     if (  end <= 0) {
       end=start;
     }
     return generateValueListBasedOntype(columnType ,start,end);
   }
       
   public List<Object> generateValueListBasedOntype(int sqlType , int start , int end ){ 
     if ( sqlType ==  java.sql.Types.BIGINT || sqlType == java.sql.Types.SMALLINT || sqlType == java.sql.Types.NUMERIC || sqlType ==  java.sql.Types.INTEGER ) {       
        return  generateListNumericValues(start, end);
     } if (sqlType == java.sql.Types.DECIMAL ) {
       return  generateListDecimalValues(start, end);
     } else if (sqlType == java.sql.Types.VARCHAR) {
      return generateListStringValues(start, end);
     } else if (sqlType == java.sql.Types.CHAR) {
       return   generateListCharValues();
     } else if (sqlType == java.sql.Types.DATE  ) {
       return generateListDateValues();
     }else if (sqlType == java.sql.Types.TIME || sqlType == java.sql.Types.TIMESTAMP ) {
     return generateListTimestampValues();
     } else if ( sqlType == java.sql.Types.BIT || sqlType == java.sql.Types.BOOLEAN  ) {
         List<Object> valueList = new ArrayList<Object>();
         valueList.add(new Boolean(true));
         valueList.add(new Boolean(false));
         return valueList;
     }else 
     return new ArrayList<Object>();
   }
   
   List<Object> getColumnValueList(String columnName , int columnType){
     ValueListMappedColumn generic = (ValueListMappedColumn) columnNameMapping.get(columnName);
     List<Object> list = new ArrayList<Object>();
     for ( Object obj : ((ValueListMappedColumn)generic).getValueList()) {
      list.add(obj); 
     }
     getSortedList(list);
     return list;
   }
   
   List<Object> getFKColumnValueList(String columnName , int columnType){
     FKMappedColumn generic = (FKMappedColumn) columnNameMapping.get(columnName);
     return getValueList(generic.getFullFkParentColumn(), columnType , 1);
   }
   
   List<Object> getStringValueList(String columnName , String targetColumn, int columnType){
     return getValueList(targetColumn, columnType);
   }
   
   public List<Object> generateListTimestampValues (){
     List<Object> valueList = new ArrayList<Object>();
     while (valueList.size() < 20 ){
       valueList.add(generateTimestamp());
     }
     getSortedList(valueList);
     return valueList;
   }
   
   public List<Object> generateListDateValues (){
     List<Object> valueList = new ArrayList<Object>();
     while (valueList.size() < 20 ){
       valueList.add(generateDate());
     }
     getSortedList(valueList);
     return valueList;
   }
   
   
   public  List<Object> generateListNumericValues ( int start , int end){
    
     List<Object>  valueList = new ArrayList<Object>();
     
    Long startRange= getStartRange(start);
    Long endRange =  getEndRange(end);
    generateNumberRange(startRange,endRange , valueList );
    getSortedList(valueList);    
    return valueList;
   }
   
   public  List<Object> generateListDecimalValues ( int start , int end){     
      List<Object>  valueList = new ArrayList<Object>();
      
     while (valueList.size() < 20 ) {
       valueList.add(getDecimalNumber(start, end));
     }
     getSortedList(valueList);
     return valueList;
    }
   
   public List<Object> generateListCharValues ( ){     
      List<Object>  valueList = new ArrayList<Object>();
      
     for (int ch =0 ; ch<=255 ; ch++ ){
       valueList.add((char)(ch));
     }
     return valueList;
    }  
   
   
   List<Object> generateListStringValues ( int start , int end ){   
     List<Object>  valueList = new ArrayList<Object>();
     generateStringRange(start,end, valueList);
     getSortedList(valueList);
     return valueList;
    }
   
   List<Object> generateStringRange(int start , int end , List<Object>  valueList){
     while (valueList.size() < 20 ) {
        valueList.add(getRandomString((int) getRandomNumber(start, end)));
     }    
     return valueList;
     
   }
   
   
   <T> void getSortedList(List<T> list){
     TreeSet<T> set = new TreeSet<T>(list);
     list.clear();
     list.addAll(set);
   }
   
   String getRandomString(int charLength){
     String mystr = "";
     int start = 65;
     int end = 122;
     while (mystr.length() < charLength ){
       mystr+= (char)((int)(SQLOldTest.random.nextDouble() * (end - start + 1) ) + start);
     }
     
     return mystr;
     
   }
   Long getStartRange(int digits ){
     int count =1;
     long digit=1;
     while ( count < digits ){
         digit *=10;
         count++;
     }
     
     return digit;
   }
   
   Long getEndRange(int digits ){
     int count =1;
     String digit = "9";
     while ( count < digits ){
         digit+="9";
         count++;
     }
     return Long.parseLong(digit);
   }
   
    void generateNumberRange(Long start , Long end ,List<Object> valueList ) {     
     while ( valueList.size() < 20) { 
       long currentVal= (long)(SQLOldTest.random.nextDouble()*(end-start+1)) + start;
       valueList.add(currentVal);              
     }
     valueList.add(start);
     valueList.add (end);
   }
   
    
    BigDecimal getDecimalNumber (int start , int end){      
      int currentVal=  (int) getRandomNumber(start,end);           
      String number = "";
      while ( currentVal > 0 ) {
        number+=SQLOldTest.random.nextInt(9);
        currentVal--;
      }      
      return new BigDecimal(number);
    }
    
    long getRandomNumber(long start , long end ){
      return (long)(SQLOldTest.random.nextDouble()*(end-start +1)) + start;
    }
        
    
    protected Date generateDate(){
      final SimpleDateFormat formatterTimestamp = new SimpleDateFormat(
          getFormatterTimestamp());
      java.util.Date baseDate = null;
      try{
        baseDate = formatterTimestamp.parse(getBaseTime());  
      }catch (ParseException e){
        throw new RuntimeException ("Error is formating Date " , e);
      }
      
      double addOn = SQLOldTest.random.nextDouble() * 86400000 * 100;
      long newTime = baseDate.getTime() + (long)addOn;
      Date newDate = new Date(newTime);
      return newDate;
    }
    
    
    protected Timestamp generateTimestamp() {

      final SimpleDateFormat formatterTimestamp = new SimpleDateFormat(
          getFormatterTimestamp());
      java.util.Date baseDate = null;
      try {
        baseDate = formatterTimestamp.parse(getBaseTime());
      }
      catch (ParseException e) {
        throw new TestException ("Error is formating TimeStamp " , e);
      }      
      double addOn = SQLOldTest.random.nextDouble() * 86400000 * 100;

      long newTime = baseDate.getTime() + (long)addOn;
      Timestamp newDate = new Timestamp(newTime);

      return newDate;
    }
    
    protected String timestampToString(Timestamp ts){
      final SimpleDateFormat formatterTimestamp = new SimpleDateFormat(
          getFormatterTimestamp());
      return formatterTimestamp.format(ts);
    }
    
    protected String getFormatterTimestamp() {
      return "yyyy-MM-dd HH:mm:ss";
    }

    protected String getFormatterDate() {
      return "yyyy-MM-dd";
    }

    protected String getBaseTime() {
      return "2012-01-01 01:01:00";
    }
}
