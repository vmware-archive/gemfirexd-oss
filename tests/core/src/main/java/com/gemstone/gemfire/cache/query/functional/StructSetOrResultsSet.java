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
/*
 * StructSetOrResultsSet.java
 * Utlity Class : Can be used to compare the results (StructSet OR ResultsSet) under the scenario without/with Index Usage.
 * Created on June 13, 2005, 11:16 AM
 */

package com.gemstone.gemfire.cache.query.functional;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import junit.framework.TestCase;

import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.types.ObjectType;


/**
 * @author vikramj, shobhit
 */
public class StructSetOrResultsSet extends TestCase {

    /** Creates a new instance of StructSetOrResultsSet
    public void CompareQueryResultsWithoutAndWithIndexes(Object[][] r, int len) {

        Set set1=null;
        Set set2=null;
        Iterator itert1=null;
        Iterator itert2=null;
        ObjectType type1,type2;

        for(int j=0;j<len;j++){
            type1 = ((SelectResults)r[j][0]).getCollectionType().getElementType();
            type2 = ((SelectResults)r[j][1]).getCollectionType().getElementType();
            if ((type1.getClass().getName()).equals(type2.getClass().getName())){
                System.out.println("Both Search Results are of the same Type i.e.--> "+((SelectResults)r[j][0]).getCollectionType().getElementType());
            }else {
                System.out.println("Classes are : " + type1.getClass().getName() + " " + type2.getClass().getName());
                fail("FAILED:Search result Type is different in both the cases");
            }
            if (((SelectResults)r[j][0]).size()==((SelectResults)r[j][1]).size()){
                System.out.println("Both Search Results are non-zero and are of Same Size i.e.  Size= "+((SelectResults)r[j][1]).size());
            }else {
                fail("FAILED:Search result size is different in both the cases . the sizes are ="+((SelectResults)r[j][0]).size() + " and " + ((SelectResults)r[j][1]).size());
            }
            set2=(((SelectResults)r[j][1]).asSet());
            set1=(((SelectResults)r[j][0]).asSet());

            if (r[j][0] instanceof StructSet) {
                boolean pass = true;
                itert1 = set1.iterator();
                while (itert1.hasNext()){
                    StructImpl p1= (StructImpl)itert1.next();
                    itert2 = set2.iterator();
                    boolean found = false;
                    while(itert2.hasNext()){
                        StructImpl p2= (StructImpl)itert2.next();
                        Object [] values1 = p1.getFieldValues();
                        Object [] values2 = p2.getFieldValues();
                        if(values1.length != values2.length){
                            fail("The length of the values in struct fields does not match");
                        }
                        boolean exactMatch = true;
                        for (int k = 0; k < values1.length; k++) {
                            if (!values1[k].equals(values2[k]))
                                exactMatch = false;
                        }
                        if(exactMatch)
                            found = true;
                    }

                    if(!found)
                        pass = false;
                }

                if(!pass)
                    fail("Test failed");
                System.out.println("Results found are StructSet and both of them are Equal.");
            } else {
                boolean pass = true;
                itert1 = set1.iterator();
                while (itert1.hasNext()){
                    Object p1= itert1.next();
                    itert2 = set2.iterator();
                    boolean found = false;
                    while(itert2.hasNext()){
                        Object p2= itert2.next();
                        if( p2.equals(p1)) {
                            found = true;
                        }
                    }
                    if(!found)
                        pass = false;
                }
                System.out.println("Results found are ResultsSet and both of them are Equal.");
                if(!pass)
                    fail("Test failed");
            }
        }
    }*/

  public void CompareQueryResultsWithoutAndWithIndexes(Object[][] r, int len,String queries[]) {
    CompareQueryResultsWithoutAndWithIndexes(r,  len,false,queries);
  }

    /** Creates a new instance of StructSetOrResultsSet */
  public void CompareQueryResultsWithoutAndWithIndexes(Object[][] r, int len, boolean checkOrder, String queries[]) {

    Collection coll1 = null;
    Collection coll2 = null;
    Iterator itert1 = null;
    Iterator itert2 = null;
    ObjectType type1, type2;
    for (int j = 0; j < len; j++) {
      type1 = ((SelectResults)r[j][0]).getCollectionType().getElementType();
      type2 = ((SelectResults)r[j][1]).getCollectionType().getElementType();
      if ((type1.getClass().getName()).equals(type2.getClass().getName())) {
        System.out.println("Both SelectResults are of the same Type i.e.--> "
            + ((SelectResults)r[j][0]).getCollectionType().getElementType());
      }
      else {
        System.out.println("Classes are : " + type1.getClass().getName() + " "
            + type2.getClass().getName());
        fail("FAILED:Select result Type is different in both the cases."+ "; failed query="+queries[j]);
      }
      if (((SelectResults)r[j][0]).size() == ((SelectResults)r[j][1]).size()) {
        System.out.println("Both SelectResults are of Same Size i.e.  Size= "
            + ((SelectResults)r[j][1]).size());
      }
      else {
        fail("FAILED:SelectResults size is different in both the cases. Size1="
            + ((SelectResults)r[j][0]).size() + " Size2 = "
            + ((SelectResults)r[j][1]).size()+"; failed query="+queries[j]);
      }
      if(checkOrder) {
        coll2 = (((SelectResults)r[j][1]).asList());
        coll1 = (((SelectResults)r[j][0]).asList());
      }else {
        coll2 = (((SelectResults)r[j][1]).asSet());
        coll1 = (((SelectResults)r[j][0]).asSet());
      }
//      boolean pass = true;
      itert1 = coll1.iterator();
      itert2 = coll2.iterator();
      while (itert1.hasNext()) {
        Object p1 = itert1.next();
        if(!checkOrder) {
          itert2 = coll2.iterator();
        }

        boolean exactMatch = false;
        while (itert2.hasNext()) {
          Object p2 = itert2.next();
          if (p1 instanceof Struct) {
            Object[] values1 = ((Struct)p1).getFieldValues();
            Object[] values2 = ((Struct)p2).getFieldValues();
            assertEquals(values1.length, values2.length);
            boolean elementEqual = true;
            for (int i = 0; i < values1.length; ++i) {
              elementEqual = elementEqual
                  && ((values1[i] == values2[i]) || values1[i]
                      .equals(values2[i]));
            }
            exactMatch = elementEqual;
          }
          else {
            exactMatch = (p2 == p1) || p2.equals(p1);
          }
          if (exactMatch || checkOrder) {
            break;
          }
        }
        if (!exactMatch) {
          fail("Atleast one element in the pair of SelectResults " + 
               "supposedly identical, is not equal "+ 
               "Match not found for :" + p1 +
               "; failed query="+queries[j]);
        }
      }
    }
  }

  /** Creates a new instance of StructSetOrResultsSet */
  public void CompareCountStarQueryResultsWithoutAndWithIndexes(Object[][] r, int len, boolean checkOrder, String queries[]) {

    Integer count1, count2;
    Iterator<Integer> itert1, itert2;
    SelectResults result1, result2;
    boolean exactMatch = true;
    for (int j = 0; j < len; j++) {
      result1 = ((SelectResults)r[j][0]);
      result2 = ((SelectResults)r[j][1]);
      assertEquals(queries[j], 1, result1.size());
      assertEquals(queries[j], 1, result2.size());

      if ((result1.asList().get(0).getClass().getName()).equals(result2.asList().get(0).getClass().getName())) {
        System.out.println("Both SelectResults are of the same Type i.e.--> "
            + ((SelectResults)r[j][0]).getCollectionType().getElementType());
      }
      else {
        fail("FAILED:Select result Type is different in both the cases."+ "; failed query="+queries[j]);
      }

      if (((SelectResults)r[j][0]).size() == ((SelectResults)r[j][1]).size()) {
        System.out.println("Both SelectResults are of Same Size i.e.  Size= "
            + ((SelectResults)r[j][1]).size());
      }
      else {
        fail("FAILED:SelectResults size is different in both the cases. Size1="
            + ((SelectResults)r[j][0]).size() + " Size2 = "
            + ((SelectResults)r[j][1]).size()+"; failed query="+queries[j]);
      }

//      boolean pass = true;
      itert1 = result1.iterator();
      itert2 = result2.iterator();
      while (itert1.hasNext()) {
        Integer p1 = itert1.next();
        Integer p2 = itert2.next();
        System.out.println("result1: "+p1+"result2: "+p2);
        exactMatch &= p1.intValue() == p2.intValue();

      }
      if (!exactMatch) {
        fail("Atleast one element in the pair of SelectResults supposedly identical, is not equal "
            + "; failed query=" + queries[j]);
      }
    }
  }

  /**
   * Compares two ArrayLists containing query results with/without order.
   *
   * @param r Array of ArrayLists
   * @param len Length of array of ArrayLists
   * @param checkOrder
   * @param queries
   */
  public void CompareQueryResultsAsListWithoutAndWithIndexes(Object[][] r,
      int len, boolean checkOrder, String queries[]) {
    CompareQueryResultsAsListWithoutAndWithIndexes(r, len, checkOrder, true,
        queries);
  }

  public void CompareQueryResultsAsListWithoutAndWithIndexes(Object[][] r,
      int len, boolean checkOrder, boolean checkClass, String queries[]) {
    Integer count1, count2;
    Iterator<Integer> itert1, itert2;
    ArrayList result1, result2;
    for (int j = 0; j < len; j++) {
      result1 = ((ArrayList) r[j][0]);
      result2 = ((ArrayList) r[j][1]);
      result1.trimToSize();
      result2.trimToSize();
      //assertFalse(queries[j], result1.size()==0);
      //assertFalse(queries[j], result2.size()==0);

      if (checkClass) {
        if ((result1.get(0).getClass().getName()).equals(result2.get(0)
            .getClass().getName())) {
          System.out.println("Both SelectResults are of the same Type i.e.--> "
              + result1.get(0).getClass().getName());
        } else {
          fail("FAILED:Select result Type is different in both the cases."+ result1.get(0).getClass().getName() +"and"+ result1.get(0).getClass().getName()
              + "; failed query=" + queries[j]);
        }
      }

      if (result1.size() == result2.size()) {
        System.out.println("Both SelectResults are of Same Size i.e.  Size= "
            + result2.size());
      } else {
        fail("FAILED:SelectResults size is different in both the cases. Size1="
            + result1.size() + " Size2 = " + result2.size() + "; failed query="
            + queries[j]);
      }

      // boolean pass = true;
      itert1 = result1.iterator();
      itert2 = result2.iterator();
      while (itert1.hasNext()) {
        Object p1 = itert1.next();
        if (!checkOrder) {
          itert2 = result2.iterator();
        }

        boolean exactMatch = false;
        while (itert2.hasNext()) {
          Object p2 = itert2.next();
          if (p1 instanceof Struct) {
            Object[] values1 = ((Struct) p1).getFieldValues();
            Object[] values2 = ((Struct) p2).getFieldValues();
            assertEquals(values1.length, values2.length);
            boolean elementEqual = true;
            for (int i = 0; i < values1.length; ++i) {
              elementEqual = elementEqual
                  && ((values1[i] == values2[i]) || values1[i]
                      .equals(values2[i]));
            }
            exactMatch = elementEqual;
          } else {
            exactMatch = (p2 == p1) || p2.equals(p1);
          }
          if (exactMatch || checkOrder) {
            break;
          }
        }
        if (!exactMatch) {
          fail("Atleast one element in the pair of SelectResults supposedly identical, is not equal "
              + "; failed query=" + queries[j]);
        }
      }
    }
  }
}
