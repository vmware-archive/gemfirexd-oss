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
package com.pivotal.gemfirexd.jdbc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import junit.framework.TestSuite;
import junit.textui.TestRunner;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.pdx.JSONFormatter;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.diag.JSONProcedures;
import com.pivotal.gemfirexd.internal.engine.jayway.jsonpath.InvalidPathException;
import com.pivotal.gemfirexd.internal.engine.jayway.jsonpath.JsonPath;

public class JsonPathTest extends JdbcTestBase{

  final static String DOCUMENT =    
      "{ \"store\": {\n" +
              "    \"book\": [ \n" +
              "      { \"category\": \"reference\",\n" +
              "        \"author\": \"Nigel Rees\",\n" +
              "        \"title\": \"Sayings of the Century\",\n" +
              "        \"price\": 8.95\n" +
              "      },\n" +
              "      { \"category\": \"fiction\",\n" +
              "        \"author\": \"Evelyn Waugh\",\n" +
              "        \"title\": \"Sword of Honour\",\n" +
              "        \"price\": 12.99\n" +
              "      },\n" +
              "      { \"category\": \"fiction\",\n" +
              "        \"author\": \"Herman Melville\",\n" +
              "        \"title\": \"Moby Dick\",\n" +
              "        \"isbn\": \"0-553-21311-3\",\n" +
              "        \"price\": 8.99\n" +
              "      },\n" +
              "      { \"category\": \"fiction\",\n" +
              "        \"author\": \"J. R. R. Tolkien\",\n" +
              "        \"title\": \"The Lord of the Rings\",\n" +
              "        \"isbn\": \"0-395-19395-8\",\n" +
              "        \"price\": 22.99\n" +
              "      }\n" +
              "    ],\n" +
              "    \"bicycle\": {\n" +
              "      \"color\": \"red\",\n" +
              "      \"price\": 19.95\n" +
              "    }\n" +
              "  }\n" +
              "}";

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(JsonPathTest.class));
  }
  
  public JsonPathTest(String name) {
    super(name);
  }
  
  public static void simpleJSONPathOps(Connection conn, boolean isPartitioned) 
      throws Exception {
    Statement stmt = conn.createStatement();
    
    String createTable = "CREATE table t1(col1 int, col2 json) persistent";
    if (isPartitioned) {
      createTable = createTable + " partition by (col1)";
    }
    else {
      createTable = createTable + " replicate";
    }
    stmt.execute(createTable);

    stmt.execute("insert into t1 values (1, '" + DOCUMENT + "')");
    
    String all_authors_in_store = "Nigel Rees,Evelyn Waugh,Herman Melville,J. R. R. Tolkien";
    ResultSet rs = stmt
        .executeQuery("SELECT json_evalPath(col2, '$.store.book[*].author') FROM T1 WHERE COL1 = 1");
    while (rs.next()) {
      String jsonDoc = rs.getString(1);
      System.out.println("KBKBKB : $.store.book[*].author : " + jsonDoc);
      assertEquals(all_authors_in_store, jsonDoc);
    }
    
    String all_authors = "Nigel Rees,Evelyn Waugh,Herman Melville,J. R. R. Tolkien";
    rs = stmt
        .executeQuery("SELECT json_evalPath(col2, '$..author') FROM T1 WHERE COL1 = 1");
    while (rs.next()) {
      String jsonDoc = rs.getString(1);
      System.out.println("KBKBKB : $..author : " + jsonDoc);
      assertEquals(all_authors, jsonDoc);
    }
    
    String all_things_in_store1 = "{\n"+
      "  \"book\" : [ {\n"+
      "    \"category\" : \"reference\",\n"+
      "    \"author\" : \"Nigel Rees\",\n"+
      "    \"title\" : \"Sayings of the Century\",\n"+
      "    \"price\" : 8.95\n"+
      "  }, {\n"+
      "    \"category\" : \"fiction\",\n"+
      "    \"author\" : \"Evelyn Waugh\",\n"+
      "    \"title\" : \"Sword of Honour\",\n"+
      "    \"price\" : 12.99\n"+
      "  }, {\n"+
      "    \"category\" : \"fiction\",\n"+
      "    \"author\" : \"Herman Melville\",\n"+
      "    \"title\" : \"Moby Dick\",\n"+
      "    \"isbn\" : \"0-553-21311-3\",\n"+
      "    \"price\" : 8.99\n"+
      "  }, {\n"+
      "    \"category\" : \"fiction\",\n"+
      "    \"author\" : \"J. R. R. Tolkien\",\n"+
      "    \"title\" : \"The Lord of the Rings\",\n"+
      "    \"isbn\" : \"0-395-19395-8\",\n"+
      "    \"price\" : 22.99\n"+
      "  } ],\n"+
      "  \"bicycle\" : {\n"+
      "    \"color\" : \"red\",\n"+
      "    \"price\" : 19.95\n"+
      "  }\n"+
      "}";
    
    rs = stmt
        .executeQuery("SELECT json_evalPath(col2, '$.store.*') FROM T1 WHERE COL1 = 1");
    while (rs.next()) {
      String jsonDoc = rs.getString(1);
      System.out.println("KBKBKB : $.store.* : " + jsonDoc);
      assertEquals(all_things_in_store1, jsonDoc);
    }
    
    String price_of_everything_in_store = "8.95,12.99,8.99,22.99,19.95";
    rs = stmt
        .executeQuery("SELECT json_evalPath(col2, '$.store..price') FROM T1 WHERE COL1 = 1");
    while (rs.next()) {
      String jsonDoc = rs.getString(1);
      System.out.println("KBKBKB : $.store..price : " + jsonDoc);
      assertEquals(price_of_everything_in_store, jsonDoc);
    }
    
    String third_book = "{\n"+
        "  \"category\" : \"fiction\",\n"+
        "  \"author\" : \"Herman Melville\",\n"+
        "  \"title\" : \"Moby Dick\",\n"+
        "  \"isbn\" : \"0-553-21311-3\",\n"+
        "  \"price\" : 8.99\n"+
        "}";
  
    rs = stmt
        .executeQuery("SELECT json_evalPath(col2, '$..book[2]') FROM T1 WHERE COL1 = 1");
    while (rs.next()) {
      String jsonDoc = rs.getString(1);
      System.out.println("KBKBKB : $..book[2] : " + jsonDoc);
      assertEquals(third_book, jsonDoc);
    }
    
    String last_book = "{\n"+
      "  \"category\" : \"fiction\",\n"+
      "  \"author\" : \"J. R. R. Tolkien\",\n"+
      "  \"title\" : \"The Lord of the Rings\",\n"+
      "  \"isbn\" : \"0-395-19395-8\",\n"+
      "  \"price\" : 22.99\n"+
      "}";
      
     rs = stmt
      .executeQuery("SELECT json_evalPath(col2, '$..book[(@.length-1)]') FROM T1 WHERE COL1 = 1");
  while (rs.next()) {
    String jsonDoc = rs.getString(1);
    System.out.println("KBKBKB : $..book[(@.length-1)] : " + jsonDoc);
    assertEquals(last_book, jsonDoc);
  }
    
  String first_two_books = "{\n"+
      "  \"category\" : \"reference\",\n"+
      "  \"author\" : \"Nigel Rees\",\n"+
      "  \"title\" : \"Sayings of the Century\",\n"+
      "  \"price\" : 8.95\n"+
      "},\n"+
      "{\n"+
      "  \"category\" : \"fiction\",\n"+
      "  \"author\" : \"Evelyn Waugh\",\n"+
      "  \"title\" : \"Sword of Honour\",\n"+
      "  \"price\" : 12.99\n"+
      "}";
   rs = stmt
        .executeQuery("SELECT json_evalPath(col2, '$..book[0,1]') FROM T1 WHERE COL1 = 1");
    while (rs.next()) {
      String jsonDoc = rs.getString(1);
      System.out.println("KBKBKB : $..book[0,1] : " + jsonDoc);
      assertEquals(first_two_books, jsonDoc);
    }
    
    rs = stmt
        .executeQuery("SELECT json_evalPath(col2, '$..book[:2]') FROM T1 WHERE COL1 = 1");
    while (rs.next()) {
      String jsonDoc = rs.getString(1);
      System.out.println("KBKBKB : $..book[:2]: " + jsonDoc);
      assertEquals(first_two_books, jsonDoc);
    }
    
    String books_with_isbn = "{\n"+
        "  \"category\" : \"fiction\",\n"+
        "  \"author\" : \"Herman Melville\",\n"+
        "  \"title\" : \"Moby Dick\",\n"+
        "  \"isbn\" : \"0-553-21311-3\",\n"+
       "  \"price\" : 8.99\n"+
       "},\n"+
       "{\n"+
       "  \"category\" : \"fiction\",\n"+
       "  \"author\" : \"J. R. R. Tolkien\",\n"+
       "  \"title\" : \"The Lord of the Rings\",\n"+
       "  \"isbn\" : \"0-395-19395-8\",\n"+
       "  \"price\" : 22.99\n"+
       "}";
    
    
     rs = stmt
        .executeQuery("SELECT json_evalPath(col2, '$..book[?(@.isbn)]') FROM T1 WHERE COL1 = 1");
    while (rs.next()) {
      String jsonDoc = rs.getString(1);
      System.out.println("KBKBKB : $..book[?(@.isbn)] : " + jsonDoc);
      assertEquals(books_with_isbn, jsonDoc);
    }
    
  String books_with_lesser_price = "{\n"+
      "  \"category\" : \"reference\",\n"+
      "  \"author\" : \"Nigel Rees\",\n"+
      "  \"title\" : \"Sayings of the Century\",\n"+
      "  \"price\" : 8.95\n"+
      "},\n"+
      "{\n"+
      "  \"category\" : \"fiction\",\n"+
      "  \"author\" : \"Herman Melville\",\n"+
      "  \"title\" : \"Moby Dick\",\n"+
      "  \"isbn\" : \"0-553-21311-3\",\n"+
      "  \"price\" : 8.99\n"+
      "}";
     rs = stmt
        .executeQuery("SELECT json_evalPath(col2, '$..book[?(@.price<10)]') FROM T1 WHERE COL1 = 1");
    while (rs.next()) {
      String jsonDoc = rs.getString(1);
      System.out.println("KBKBKB : $..book[?(@.price<10)] : " + jsonDoc);
      assertEquals(books_with_lesser_price, jsonDoc);
    }

    String expectedResult = "{\n"+
    "  \"category\" : \"fiction\",\n"+
    "  \"author\" : \"Evelyn Waugh\",\n"+
    "  \"title\" : \"Sword of Honour\",\n"+
    "  \"price\" : 12.99\n"+
    "},\n"+
    "{\n"+
    "  \"category\" : \"fiction\",\n"+
    "  \"author\" : \"J. R. R. Tolkien\",\n"+
    "  \"title\" : \"The Lord of the Rings\",\n"+
    "  \"isbn\" : \"0-395-19395-8\",\n"+
    "  \"price\" : 22.99\n"+
    "}";
    
    rs = stmt
        .executeQuery("SELECT json_evalPath(col2, '$.store.book[?(@.category==fiction && @.price > 10)]') FROM T1 WHERE COL1 = 1");
    while (rs.next()) {
      String jsonDoc = rs.getString(1);
      System.out.println("KBKBKB : $.store.book[?(@.category==fiction && @.price > 10)] : " + jsonDoc);
      assertEquals(expectedResult, jsonDoc);
    }
    
    expectedResult = "{\n"+
    "  \"category\" : \"reference\",\n"+
    "  \"author\" : \"Nigel Rees\",\n"+
    "  \"title\" : \"Sayings of the Century\",\n"+
    "  \"price\" : 8.95\n"+
    "},\n"+
    "{\n"+
    "  \"category\" : \"fiction\",\n"+
    "  \"author\" : \"Evelyn Waugh\",\n"+
    "  \"title\" : \"Sword of Honour\",\n"+
    "  \"price\" : 12.99\n"+
    "},\n"+
    "{\n"+
    "  \"category\" : \"fiction\",\n"+
    "  \"author\" : \"J. R. R. Tolkien\",\n"+
    "  \"title\" : \"The Lord of the Rings\",\n"+
    "  \"isbn\" : \"0-395-19395-8\",\n"+
    "  \"price\" : 22.99\n"+
    "}"; 
    
    rs = stmt
        .executeQuery("SELECT json_evalPath(col2, '$.store.book[?(@.category==reference || @.price > 10)]') FROM T1 WHERE COL1 = 1");
    while (rs.next()) {
      String jsonDoc = rs.getString(1);
      System.out.println("KBKBKB : $.store.book[?(@.category==reference || @.price > 10)] : " + jsonDoc);
      assertEquals(expectedResult, jsonDoc);
    }
    
    rs = stmt
        .executeQuery("SELECT json_evalPath(col2, '$..author') from T1 WHERE JSON_EVALPATH(col2, '$..book[?(@.price>20)]' ) IS NOT NULL");
    while (rs.next()) {
      String jsonDoc = rs.getString(1);
      System.out.println("KBKBKB : $.store.book[?(@.category==reference || @.price > 10)] : " + jsonDoc);
      assertEquals(all_authors, jsonDoc);
    }
    
  }
  
  public void testGenericJsonPath() throws Exception {
    Properties props = new Properties();
    props.setProperty("log-level", "config");
    Connection cxn = TestUtil.getConnection(props);
    simpleJSONPathOps(cxn, true);
  }
  
  public void test51300_WrongKey() throws Exception {
    Properties props = new Properties();
    props.setProperty("log-level", "config");
    Connection cxn = TestUtil.getConnection(props);
    Statement stmt = cxn.createStatement();

    String DocWithNullValue = "{\n" 
        + "  \"_id\":\"01001\",\n"
        + "  \"city\":\"AGAWAM\",\n" 
        + "  \"loc\":[  \n" 
        + "     -72.622739,\n"
        + "     42.070206\n" 
        + "  ],\n" 
        + "  \"pop\":9000,\n"
        + "  \"state\":\"MH\"\n" 
        + "}";

    String createTable = "CREATE table t1(col1 int, col2 json) persistent partition by (col1)";
    stmt.execute(createTable);

    stmt.execute("insert into t1 values (1, '" + DocWithNullValue + "')");

    ResultSet rs = stmt
        .executeQuery("SELECT json_evalPath(col2, 'sta') FROM T1 WHERE COL1 = 1");
    while (rs.next()) {
      String jsonDoc = rs.getString(1);
      assertEquals(null, jsonDoc);
    }
  }
  
  public void test51301_NullValue() throws Exception {
    Properties props = new Properties();
    props.setProperty("log-level", "config");
    Connection cxn = TestUtil.getConnection(props);
    Statement stmt = cxn.createStatement();

    String DocWithNullValue = "{\n" 
        + "  \"_id\":\"01001\",\n"
        + "  \"city\":\"AGAWAM\",\n" 
        + "  \"loc\":[  \n" 
        + "     -72.622739,\n"
        + "     42.070206\n" 
        + "  ],\n" 
        + "  \"pop\":9000,\n"
        + "  \"state\":null\n" 
        + "}";

    String createTable = "CREATE table t1(col1 int, col2 json) persistent partition by (col1)";
    stmt.execute(createTable);

    stmt.execute("insert into t1 values (1, '" + DocWithNullValue + "')");

    ResultSet rs = stmt
        .executeQuery("SELECT json_evalPath(col2, 'state') FROM T1 WHERE COL1 = 1");
    while (rs.next()) {
      String jsonDoc = rs.getString(1);
      assertEquals(null, jsonDoc);
    }
  }

  public void test_bug51316() throws Exception {
    Properties props = new Properties();
    props.setProperty("log-level", "config");
    Connection cxn = TestUtil.getConnection(props);
    Statement stmt = cxn.createStatement();
    
    
    String jsonString = "{\n"
    +"  \"buyorder\" : [ {\n"
    +"    \"cid\" : 23,\n"
    +"    \"sid\" : 34,\n"
    +"    \"tid\" : 56\n"
    +"  }, {\n"
    +"    \"cid\" : 24,\n"
    +"    \"sid\" : 35,\n"
    +"    \"tid\" : 56\n"
    +"  }, {\n"
    +"    \"cid\" : 25,\n"
    +"    \"sid\" : 34,\n"
    +"    \"tid\" : 56\n"
    +"  }, {\n"
    +"    \"cid\" : 26,\n"
    +"    \"sid\" : 33,\n"
    +"    \"tid\" : 56\n"
    +"  } ]\n"
    +"}";
    
    String createTable = "CREATE table t1(col1 int, col2 json) persistent partition by (col1)";
    stmt.execute(createTable);
    
    stmt.execute("insert into t1 values ("+ 1 +", '" + jsonString + "')");
    
    ResultSet rs = stmt
        .executeQuery("SELECT json_evalPath(col2, '$') FROM T1 WHERE COL1 = 1");
    while (rs.next()) {
      String jsonDoc = rs.getString(1);
      assertEquals(jsonString, jsonDoc);
    }
  }
  
  public void testEvalAttribute() throws Exception {
    Properties props = new Properties();
    props.setProperty("log-level", "config");
    Connection cxn = TestUtil.getConnection(props);
    Statement stmt = cxn.createStatement();
    
    String createTable = "CREATE table t1(col1 int, col2 json) persistent partition by (col1)";
    stmt.execute(createTable);
    
    int rowid = 0;
    String line;
    String JTESTS = getResourcesDir();
    String jsonStringsDir = JTESTS + File.separator + "com" + File.separator + "gemstone"  + File.separator + 
                               "gemfire" + File.separator + "pdx"  + File.separator + "jsonStrings";
    File file = new File(jsonStringsDir+File.separator+"json_mongodb.txt");
    BufferedReader br = new BufferedReader(new FileReader(file));
    while ((line = br.readLine()) != null) {
      rowid++;
      stmt.execute("insert into t1 values ("+ rowid +", '" + line + "')");
    }
    br.close();
    
    List sqlList = new ArrayList();
    ResultSet rs = stmt
        .executeQuery("SELECT json_evalPath(col2, '$..[?(@.pop>=10000)]._id') FROM T1");
    while (rs.next()) {
      String jsonDoc = rs.getString(1);
      sqlList.add(jsonDoc);
    }
    assertEquals(25000, sqlList.size());
    
    //SELECT * FROM T1 WHERE JSON_evalAttribute(col2, '$..[?(@.pop>=10000)]') and JSON_evalAttribute(col2, '$..[?(@.state=='MA')]')
    rs = stmt
        .executeQuery("SELECT * FROM T1 WHERE json_evalPath(col2, '$..[?(@.pop>=10000)]') IS NOT NULL");
    
    sqlList = new ArrayList();
    while (rs.next()) {
      String jsonDoc = rs.getString(2);
      sqlList.add(jsonDoc);
    }
    assertEquals(6046, sqlList.size());
  }
  
  public void test51307_HandleArrayIndexOutOFBoundExcepption() throws Exception {
    Properties props = new Properties();
    props.setProperty("log-level", "config");
    Connection cxn = TestUtil.getConnection(props);
    Statement stmt = cxn.createStatement();

    String jsonString = "{\n" + "\"buyorder\":[\n"
        + "           {\"cid\":23 , \"sid\":34 , \"tid\":56} ,\n"
        + "           {\"cid\":24 , \"sid\":35 , \"tid\":56} ,\n"
        + "           {\"cid\":25 , \"sid\":34 , \"tid\":56} ,\n"
        + "           {\"cid\":26 , \"sid\":33 , \"tid\":56} ,\n"
        + "           {\"cid\":27 , \"sid\":34 , \"tid\":56} ,\n"
        + "           {\"cid\":28 , \"sid\":34 , \"tid\":56} ,\n"
        + "           {\"cid\":29 , \"sid\":33 , \"tid\":56} ,\n"
        + "           {\"cid\":30 , \"sid\":34 , \"tid\":56}\n"
        + "          ]\n" + "}";

    String createTable = "CREATE table t1(col1 int, col2 json) persistent partition by (col1)";
    stmt.execute(createTable);

    stmt.execute("insert into t1 values (1, '" + jsonString + "')");

    String result = "{\n" + "  \"cid\" : 23,\n" + "  \"sid\" : 34,\n"
        + "  \"tid\" : 56\n" + "}";
    ResultSet rs = stmt
        .executeQuery("SELECT json_evalPath(col2, '$..buyorder[(@.cid==23)]') FROM T1 WHERE COL1 = 1");
    try {
      while (rs.next()) {
        String jsonDoc = rs.getString(1);
        assertEquals(result, jsonDoc);
      }
    }
    catch (Exception e) {
      assertTrue("Expected InvalidPathException",
          e.getCause() instanceof InvalidPathException);
    }
  }
  
  
  public void testJsonPath() throws Exception {
    String myDoc = "{\n"+
    "  \"firstName\": \"John\",\n"+
    "  \"lastName\" : \"doe\",\n"+
    "  \"age\"      : 26,\n"+
    "  \"address\"  :\n"+
    "    [\n"+
    "       {\n"+
    "           \"streetAddress\": \"Kashid\",\n"+
    "           \"city\"         : \"Pune\",\n"+
    "           \"postalCode\"   : \"45879\"\n"+
    "        },\n"+
    "        {\n"+
    "           \"streetAddress\": \"Ooti\",\n"+
    "           \"city\"         : \"Karnatak\",\n"+
    "           \"postalCode\"   : \"548975\"\n"+
    "        }\n"+
    "     ],\n"+
    "  \"phoneNumbers\":\n"+
    "  [\n"+
    "      {\n"+
    "        \"type\"  : \"iPhone\",\n"+
    "        \"number\": \"0123-4567-8888\"\n"+
    "      },\n"+
    "      {\n"+
    "        \"type\"  : \"home\",\n"+
    "        \"number\": \"0123-4567-8910\",\n"+
    "        \"address\"  :\n"+
    "          [\n"+
    "           {\n"+
    "               \"streetAddress\": \"naist street\",\n"+
    "               \"city\"         : \"Nara\",\n"+
    "               \"postalCode\"   : \"630-0192\"\n"+
    "           },\n"+
    "           {\n"+
    "               \"streetAddress\": \"Collector\",\n"+
    "               \"city\"         : \"Malegaon\",\n"+
    "               \"postalCode\"   : \"423105\"\n"+
   "            }\n"+
    "          ]\n"+
    "      },\n"+
    "      {\n"+
    "        \"type\"  : \"office\",\n"+
    "        \"number\": \"0456-4567-8910\",\n"+
    "        \"address\"  :\n"+
    "           [{\n"+
    "               \"streetAddress\": \"wakadewadi\",\n"+
    "               \"city\"         : \"pune\",\n"+
    "               \"postalCode\"   : \"411025\"\n"+
    "           }]\n"+
    "       }\n"+
    "  ]\n"+
  "}";
    
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    CacheFactory cf = new CacheFactory(props);
    cf.setPdxReadSerialized(true);
    Cache cache = cf.create(DistributedSystem.connect(props)); 
    //My Tests
    Object obj;
    String actualResult;
    String expectedResult;
    PdxInstance pdx = JSONFormatter.fromJSON(myDoc);
    
    expectedResult = "John";
    obj = JsonPath.jsonPathQuery(pdx, "firstName");
    actualResult = JSONProcedures.convertObjToJsonString(obj);
    assertEquals(expectedResult, actualResult);
        
    obj = JsonPath.jsonPathQuery(pdx, "$.firstName");
    actualResult = JSONProcedures.convertObjToJsonString(obj);
    assertEquals(expectedResult, actualResult);
        
    obj = JsonPath.jsonPathQuery(pdx, "$..firstName");
    actualResult = JSONProcedures.convertObjToJsonString(obj);
    assertEquals(expectedResult, actualResult);
        
    expectedResult = "{\n"+
    "  \"type\" : \"iPhone\",\n"+
    "  \"number\" : \"0123-4567-8888\"\n"+
    "}";
    obj = JsonPath.jsonPathQuery(pdx, "$.phoneNumbers[?(@.type==iPhone)]");
    actualResult = JSONProcedures.convertObjToJsonString(obj);
    assertEquals(expectedResult, actualResult);
    
    expectedResult= "Pune,Karnatak,Nara,Malegaon,pune";
    obj = JsonPath.jsonPathQuery(pdx, "$..city");
    actualResult = JSONProcedures.convertObjToJsonString(obj);
    assertEquals(expectedResult, actualResult);
    
    expectedResult = "{\n"+
    "  \"type\" : \"iPhone\",\n"+
    "  \"number\" : \"0123-4567-8888\"\n"+
    "},\n"+
    "{\n"+
    "  \"type\" : \"home\",\n"+
    "  \"number\" : \"0123-4567-8910\",\n"+
    "  \"address\" : [ {\n"+
    "    \"streetAddress\" : \"naist street\",\n"+
    "    \"city\" : \"Nara\",\n"+
    "    \"postalCode\" : \"630-0192\"\n"+
    "  }, {\n"+
    "    \"streetAddress\" : \"Collector\",\n"+
    "    \"city\" : \"Malegaon\",\n"+
    "    \"postalCode\" : \"423105\"\n"+
    "  } ]\n"+
    "},\n"+
    "{\n"+
    "  \"type\" : \"office\",\n"+
    "  \"number\" : \"0456-4567-8910\",\n"+
    "  \"address\" : [ {\n"+
    "    \"streetAddress\" : \"wakadewadi\",\n"+
    "    \"city\" : \"pune\",\n"+
    "    \"postalCode\" : \"411025\"\n"+
    "  } ]\n"+
    "}";
    obj = JsonPath.jsonPathQuery(pdx, "phoneNumbers");
    actualResult = JSONProcedures.convertObjToJsonString(obj);
    assertEquals(expectedResult, actualResult);

    expectedResult = "naist street,Collector,wakadewadi";
    obj = JsonPath.jsonPathQuery(pdx, "phoneNumbers[*]..streetAddress");
    actualResult = JSONProcedures.convertObjToJsonString(obj);
    assertEquals(expectedResult, actualResult);

    expectedResult = "{\n"+
    "  \"streetAddress\" : \"naist street\",\n"+
    "  \"city\" : \"Nara\",\n"+
    "  \"postalCode\" : \"630-0192\"\n"+
    "},\n"+
    "{\n"+
    "  \"streetAddress\" : \"Collector\",\n"+
    "  \"city\" : \"Malegaon\",\n"+
    "  \"postalCode\" : \"423105\"\n"+
    "},\n"+
    "{\n"+
    "  \"streetAddress\" : \"wakadewadi\",\n"+
    "  \"city\" : \"pune\",\n"+
    "  \"postalCode\" : \"411025\"\n"+
    "}";
    
    obj = JsonPath.jsonPathQuery(pdx, "phoneNumbers[*].address");
    actualResult = JSONProcedures.convertObjToJsonString(obj);
    assertEquals(expectedResult, actualResult);

    expectedResult = "Kashid,Ooti,naist street,Collector,wakadewadi";
    obj = JsonPath.jsonPathQuery(pdx, "$..address.streetAddress");
    actualResult = JSONProcedures.convertObjToJsonString(obj);
    assertEquals(expectedResult, actualResult);
    
    expectedResult = "{\n"+
    "  \"streetAddress\" : \"Ooti\",\n"+
    "  \"city\" : \"Karnatak\",\n"+
    "  \"postalCode\" : \"548975\"\n"+
    "},\n"+
    "{\n"+
    "  \"streetAddress\" : \"naist street\",\n"+
    "  \"city\" : \"Nara\",\n"+
    "  \"postalCode\" : \"630-0192\"\n"+
    "},\n"+
    "{\n"+
    "  \"streetAddress\" : \"Collector\",\n"+
    "  \"city\" : \"Malegaon\",\n"+
    "  \"postalCode\" : \"423105\"\n"+
    "}";
    
    obj = JsonPath.jsonPathQuery(pdx, "$..address[?(@.streetAddress=='Collector' | @.city=='Nara' | @.postalCode=='548975')]");
    actualResult = JSONProcedures.convertObjToJsonString(obj);
    assertEquals(expectedResult, actualResult);
    
    expectedResult = "{\n"+
    "  \"streetAddress\" : \"Collector\",\n"+
    "  \"city\" : \"Malegaon\",\n"+
    "  \"postalCode\" : \"423105\"\n"+
    "}";
    obj = JsonPath.jsonPathQuery(pdx, "$..address[?(@.streetAddress=='Collector' & @.city=='Malegaon' & @.postalCode=='423105')]");
    actualResult = JSONProcedures.convertObjToJsonString(obj);
    assertEquals(expectedResult, actualResult);
    cache.close();
  }
}
