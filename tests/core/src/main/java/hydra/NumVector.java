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
 * A Vector subclass with methods for storing representations of ints, 
 * longs, and doubles.  Has corresponding methods for retrieving instances 
 * of same (for example, longAt()) without doing explicit casts.
 *
 * Also implements some statistical functions so that you easily
 * get mean, std deviation, and so forth of a NumVector of data.
 *
 * Not overwhelmingly efficient in terms of either time or space.  Running 
 * on a JavaSoft VM on my old Sparc, this takes about 900 milliseconds to run  
 * the stats on 10k numbers.  
 *
 * @author  <A HREF="mailto:nastosm@gemstone.com">Mike Nastos</A>
 * @version 1.0, 01/05/98
 */


public class NumVector extends Vector implements Serializable {

  public NumVector() {
    super();
  }
  public NumVector(Collection c) {
    super(c);
  }
  public NumVector(int initialCapacity) {
    super(initialCapacity);
  }
  public NumVector(int initialCapacity, int capacityIncrement) {
    super(initialCapacity, capacityIncrement);
  }

 /**
  *
  * Adds the elements of aVector to the receiver 
  */
  public void addAll(Vector aVector) {
    for (int i = 0; i < aVector.size(); i++) { 
       this.add(aVector.elementAt(i));
    }
  }

 /**
  * 
  * Adds a double to the receiver
  *
  * @param   arg the double to be stored 
  */
  public void add(double arg) { 
     super.add(new Double(arg));
  }

  /**
  * 
  * Adds an int to the receiver
  *
  * @param   arg the int to be stored 
  */
  public void add(int arg) { 
     super.add(new Integer(arg));
  }

 /**
  * 
  * Adds a long to the receiver
  *
  * @param   arg the long to be stored 
  */
  public void add(long arg) { 
     super.add(new Long(arg));
  }

  /**
  *
  * Returns the item at index pos as a double
  *
  * @param   pos  the index of the item to be returned 
  */
  public double doubleAt(int pos) {
     Object element;
     Long lElement; 
     Integer iElement;
     Double dElement;

     element = super.elementAt(pos);
     if (element instanceof Integer) {
        iElement = (Integer) element;      
        return iElement.doubleValue(); 
     }
     if (element instanceof Double) {
        dElement = (Double) element;      
        return dElement.doubleValue(); 
     }
     else {
        lElement = (Long) element;
        return lElement.doubleValue();
     }
  }

  /**
  *
  * Returns the item at index pos as an int 
  *
  * @param   pos  the index of the item to be returned 
  */
  public long intAt(int pos) {
     Object element;
     Long lElement; 
     Integer iElement;
     Double dElement;

     element = super.elementAt(pos);
     if (element instanceof Integer) {
        iElement = (Integer) element;      
        return iElement.intValue(); 
     }
     if (element instanceof Double) {
        dElement = (Double) element;      
        return dElement.intValue(); 
     }
     else {
        lElement = (Long) element;
        return lElement.intValue();
     }
  }

  /**
  *
  * Returns the item at index pos as a long 
  *
  * @param   pos  the index of the item to be returned 
  */
  public long longAt(int pos) {
     Object element;
     Long lElement; 
     Integer iElement;
     Double dElement;

     element = super.elementAt(pos);
     if (element instanceof Integer) {
        iElement = (Integer) element;      
        return iElement.longValue(); 
     }
     if (element instanceof Double) {
        dElement = (Double) element;      
        return dElement.longValue(); 
     }
     else {
        lElement = (Long) element;
        return lElement.longValue();
     }
  }


  /**
  *
  * Returns a double representing  he mean of the receiver's contents 
  *
  */
  public double mean() {
     int num = this.size();
     double sum = this.sum();
     return sum/num; 
  }

  /**
  *
  * Returns a double representing the stddev of the receiver's contents 
  *
  */
  public double stddev() {
    double statRay[] = this.stats();
    return statRay[2];
  }

  /**
  *
  * Calculates mean, std dev, and so on, then returns the resulting
  * stats as a String. 
  *
  */
  public String printStats() {

     if (this.size() == 0)
         return null;

     double statRay[] = this.stats();
     return "Number of task times: " + (int) statRay[0] +
            "\nMean: " + (float) statRay[1] + " ms" +
            "\nStandard deviation: " + (float) statRay[2] + " ms" +
            "\nSum: " + (float) statRay[3] + " ms" +
            "\nMax Value: " + (float) statRay[4] + " ms" +
            "\nMin Value: " + (float) statRay[5] + " ms";
  }

  /**
  *
  * Calculates mean, std dev, and so on, then returns the resulting
  * stats as a String in table-row format. 
  *
  */
  public String printStatsAsTableRow() {

     if (this.size() == 0)
         return "No data present.";

     double statRay[] = this.stats();
     return "   " + (int) statRay[0] +
            "    " + (float) statRay[1] +
            "    " + (float) statRay[2] +
            "    " + (float) statRay[3] +
            "    " + (float) statRay[4] +   
            "    " + (float) statRay[5]; 
  }

  /**
  *
  * Returns a String for use as a column labels in a table
  * stat printout.
  *
  */
  public String printStatsTableHeader() {

    if (this.size() == 0)
       return "No data present.";

    double statRay[] = this.stats();
    return "Set sz "  +
            "Mean    " + 
            "Std dev    " + 
            "Sum    " + 
            "Max    " +
            "Min    "; 

  }

  /**
  *
  * Calculates mean, std dev, and so on, then returns the resulting 
  * stats as an array of doubles.  The format of the array is:
  * 
  *   [0] data set size
  *   [1] standard deviation 
  *   [2] sum of the data items 
  *   [3] the max value 
  *   [4] the min value
  */
  public double[] stats() {
//     double avg = 0.0; 
     double sum = 0.0;
     double hi, lo, dev, mean, square, squareMean;
//     double median, mode;
     double statRay[] = new double[6];
     NumVector squares = new NumVector();
//     Hashtable freqs = new Hashtable();
     int num = this.size();
     double curr; 
     lo = hi = this.doubleAt(0);
     for (int i = 0; i < num; i++) { 
        curr = this.doubleAt(i);
        sum += curr;
        if (curr < lo)
           lo = curr;
        if (curr > hi)
          hi = curr;
     };
     mean = sum/num; 

     for (int i = 0; i < this.size(); i++) { 
        curr = this.doubleAt(i);
        dev = Math.abs(curr - mean);
        square = dev * dev;
        squares.add(square); 
     }

     mean = sum/num; 
     squareMean = squares.mean(); 
     dev = Math.sqrt(squareMean);
     statRay[0] = this.size();
     statRay[1] = mean;
     statRay[2] = dev;
     statRay[3] = sum;
     statRay[4] = hi;
     statRay[5] = lo;
     return statRay;
  }

  /**
  *
  * Returns a double representing the sum of the receiver's contents
  *
  */
  public double sum() {
     double sum = 0.0;
     double curr; 
     for (int i = 0; i < this.size(); i++) { 
        curr = this.doubleAt(i);
        sum += curr;
     };
     return sum; 
  }

  /**
  *
  * A test routine that creates a NumVector and calculates the stats 
  *
  */
  public static void main(String[] args) {
    int i1;
//    int i2;
    double d1;
//    double d2;
    long l1;
//    long l2;
    NumVector v = new NumVector();
    
    l1 = 34L;
    i1 = 45; 
    d1 = 34.93;
    v.add(l1); 
    v.add(i1);
    v.add(d1);
    System.out.println(v.intAt(0));
    System.out.println(v.longAt(0));
    System.out.println(v.elementAt(0));
    System.out.println(v.longAt(1));
    System.out.println(v.doubleAt(2));
    System.out.println(v.doubleAt(1));

    long begin = 0;
    long end = 0;
    for (int i = 0; i < 100; i++) {
       v.add(i);
    }
    begin = System.currentTimeMillis();
    for (int i = 0; i < v.size(); i++) {
       v.intAt(i);
    }
    end = System.currentTimeMillis();
    System.out.println("Access time for " + v.size() + " elements " + 
                         (end - begin));
    begin = System.currentTimeMillis();
    System.out.println("Mean of values: " + v.mean());
    end = System.currentTimeMillis();
    System.out.println("Time to compute mean for " + v.size() + " elements " + 
                         (end - begin));
 
    begin = System.currentTimeMillis();
    System.out.println(v.printStats());
    end = System.currentTimeMillis();
    System.out.println("Time to compute all stats for " + 
                         v.size() + " elements " + 
                         (end - begin));
    NumVector newVec = new NumVector();
    newVec.addAll(v);
    System.out.println("After addAll, size is: " + newVec.size());

    System.out.println(newVec.printStatsTableHeader() + "\n"); 
    System.out.println(newVec.printStatsAsTableRow() + "\n"); 
  } 
}


