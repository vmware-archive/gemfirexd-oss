/*
 * Copyright 2002-2008 the original author or authors.
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

import java.math.BigInteger;

/**
 *
 * Systematically generates permutations. 
 *
 * The class is very easy to use. Suppose that you wish to generate all
 * permutations of the strings "a", "b", "c", and "d". Put them into an array.
 * Keep calling the permutation generator's getNext () method until there are no
 * more permutations left. The getNext () method returns an array of integers,
 * which tell you the order in which to arrange your original array of strings.
 * See {@link #main} for an example.
 *
 * One caveat. Don't use this class on large sets. Recall that the number of
 * permutations of a set containing n elements is n factorial, which is a very
 * large number even when n is as small as 20. 20! is 2,432,902,008,176,640,000. 
 *
 * The algorithm is from Kenneth H. Rosen, "Discrete Mathematics and Its
 * Applications", 2nd edition (NT:McGraw-Hill, 1991), pp. 282-284.
 *
 * This source code is free for you to use in whatever way you wish.
 *
 * @author Michael Gilleland, Merriam Park Software
 */

public class PermutationGenerator {
  private int[] a;
  private BigInteger numLeft;
  private BigInteger total;

  //-----------------------------------------------------------
  // Constructor. WARNING: Don't make n too large.
  // Recall that the number of permutations is n!
  // which can be very large, even when n is as small as 20 --
  // 20! = 2,432,902,008,176,640,000 and
  // 21! is too big to fit into a Java long, which is
  // why we use BigInteger instead.
  //----------------------------------------------------------

  public PermutationGenerator (int n) {
    if (n < 1) {
      throw new IllegalArgumentException ("Min 1");
    }
    a = new int[n];
    total = getFactorial (n);
    reset ();
  }

  //------
  // Reset
  //------

  public void reset () {
    for (int i = 0; i < a.length; i++) {
      a[i] = i;
    }
    numLeft = new BigInteger (total.toString ());
  }

  //------------------------------------------------
  // Return number of permutations not yet generated
  //------------------------------------------------

  public BigInteger getNumLeft () {
    return numLeft;
  }

  //------------------------------------
  // Return total number of permutations
  //------------------------------------

  public BigInteger getTotal () {
    return total;
  }

  //-----------------------------
  // Are there more permutations?
  //-----------------------------

  public boolean hasMore () {
    return numLeft.compareTo (BigInteger.ZERO) == 1;
  }

  //------------------
  // Compute factorial
  //------------------

  private static BigInteger getFactorial (int n) {
    BigInteger fact = BigInteger.ONE;
    for (int i = n; i > 1; i--) {
      fact = fact.multiply (new BigInteger (Integer.toString (i)));
    }
    return fact;
  }

  //--------------------------------------------------------
  // Generate next permutation (algorithm from Rosen p. 284)
  //--------------------------------------------------------

  public int[] getNext () {

    if (numLeft.equals (total)) {
      numLeft = numLeft.subtract (BigInteger.ONE);
      return a;
    }

    int temp;

    // Find largest index j with a[j] < a[j+1]

    int j = a.length - 2;
    while (a[j] > a[j+1]) {
      j--;
    }

    // Find index k such that a[k] is smallest integer
    // greater than a[j] to the right of a[j]

    int k = a.length - 1;
    while (a[j] > a[k]) {
      k--;
    }

    // Interchange a[j] and a[k]

    temp = a[k];
    a[k] = a[j];
    a[j] = temp;

    // Put tail end of permutation after jth position in increasing order

    int r = a.length - 1;
    int s = j + 1;

    while (r > s) {
      temp = a[s];
      a[s] = a[r];
      a[r] = temp;
      r--;
      s++;
    }

    numLeft = numLeft.subtract (BigInteger.ONE);
    return a;

  }

  public static void main( String[] args ) {
    int[] indices;
    String[] elements =  {"a", "b", "c", "d" };
    PermutationGenerator x = new PermutationGenerator( elements.length );
    StringBuffer permutation;
    while ( x.hasMore () ) {
      permutation = new StringBuffer();
      indices = x.getNext();
      for ( int i = 0; i < indices.length; i++ )
        permutation.append( elements[ indices[i] ] );
      System.out.println( permutation.toString() );
    }
  }
}
