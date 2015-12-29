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

package hydratest;

//import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.internal.Assert;

import hydra.*;

/**
 *
 *  A client that tests hydra.
 *
 */

public class HydraTestClient {

  public static void checkVecPrms() {
    ConfigHashtable tab = TestConfig.tab();
    HydraVector v,w;
    HydraVector f,g,h;

    // hydratest.HydraTestPrms-prm01 = a;
    v = tab.vecAt( HydraTestPrms.prm01 );
      w = new HydraVector("a");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAt( HydraTestPrms.prm01, new HydraVector("Z") );
      w = new HydraVector("a");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAt( HydraTestPrms.prm01, 0, new HydraVector("Z") );
      w = new HydraVector("a");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAt( HydraTestPrms.prm01, 1, new HydraVector("Z") );
      w = new HydraVector("Z");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAtWild( HydraTestPrms.prm01, 0, new HydraVector("Z") );
      w = new HydraVector("a");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAtWild( HydraTestPrms.prm01, 1, new HydraVector("Z") );
      w = new HydraVector("a");
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm02 = a b;
    v = tab.vecAt( HydraTestPrms.prm02 );
      w = new HydraVector("a"); w.add("b");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAt( HydraTestPrms.prm02, new HydraVector("Z") );
      w = new HydraVector("a"); w.add("b");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAt( HydraTestPrms.prm02, 0, new HydraVector("Z") );
      w = new HydraVector("a"); w.add("b");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAt( HydraTestPrms.prm02, 1, new HydraVector("Z") );
      w = new HydraVector("Z");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAtWild( HydraTestPrms.prm02, 0, new HydraVector("Z") );
      w = new HydraVector("a"); w.add("b");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAtWild( HydraTestPrms.prm02, 1, new HydraVector("Z") );
      w = new HydraVector("a"); w.add("b");
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm03 = a b c;
    v = tab.vecAt( HydraTestPrms.prm03 );
      w = new HydraVector("a"); w.add("b"); w.add("c");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAt( HydraTestPrms.prm03, new HydraVector("Z") );
      w = new HydraVector("a"); w.add("b"); w.add("c");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAt( HydraTestPrms.prm03, 0, new HydraVector("Z") );
      w = new HydraVector("a"); w.add("b"); w.add("c");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAt( HydraTestPrms.prm03, 1, new HydraVector("Z") );
      w = new HydraVector("Z");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAtWild( HydraTestPrms.prm03, 0, new HydraVector("Z") );
      w = new HydraVector("a"); w.add("b"); w.add("c");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAtWild( HydraTestPrms.prm03, 1, new HydraVector("Z") );
      w = new HydraVector("a"); w.add("b"); w.add("c");
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm04 = a, b c;
    v = tab.vecAt( HydraTestPrms.prm04 );
      f = new HydraVector("a");
      g = new HydraVector("b"); g.add("c");
      w = new HydraVector( f ); w.add( g );
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAt( HydraTestPrms.prm04, new HydraVector("Z") );
      f = new HydraVector("a");
      g = new HydraVector("b"); g.add("c");
      w = new HydraVector( f ); w.add( g );
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAt( HydraTestPrms.prm04, 0, new HydraVector("Z") );
      w = new HydraVector("a");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAt( HydraTestPrms.prm04, 1, new HydraVector("Z") );
      w = new HydraVector("b"); w.add("c");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAt( HydraTestPrms.prm04, 2, new HydraVector("Z") );
      w = new HydraVector("Z");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAtWild( HydraTestPrms.prm04, 0, new HydraVector("Z") );
      w = new HydraVector("a");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAtWild( HydraTestPrms.prm04, 1, new HydraVector("Z") );
      w = new HydraVector("b"); w.add("c");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAtWild( HydraTestPrms.prm04, 2, new HydraVector("Z") );
      w = new HydraVector("b"); w.add("c");
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm05 = a b, c;
    v = tab.vecAt( HydraTestPrms.prm05 );
      f = new HydraVector("a"); f.add("b");
      g = new HydraVector("c");
      w = new HydraVector( f ); w.add( g );
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAt( HydraTestPrms.prm05, new HydraVector("Z") );
      f = new HydraVector("a"); f.add("b");
      g = new HydraVector("c");
      w = new HydraVector( f ); w.add( g );
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAt( HydraTestPrms.prm05, 0, new HydraVector("Z") );
      w = new HydraVector("a"); w.add("b");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAt( HydraTestPrms.prm05, 1, new HydraVector("Z") );
      w = new HydraVector("c");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAt( HydraTestPrms.prm05, 2, new HydraVector("Z") );
      w = new HydraVector("Z");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAtWild( HydraTestPrms.prm05, 0, new HydraVector("Z") );
      w = new HydraVector("a"); w.add("b");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAtWild( HydraTestPrms.prm05, 1, new HydraVector("Z") );
      w = new HydraVector("c");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAtWild( HydraTestPrms.prm05, 2, new HydraVector("Z") );
      w = new HydraVector("c");
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm06 = a, b, c;
    v = tab.vecAt( HydraTestPrms.prm06 );
      f = new HydraVector("a");
      g = new HydraVector("b");
      h = new HydraVector("c");
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAt( HydraTestPrms.prm06, new HydraVector("Z") );
      f = new HydraVector("a");
      g = new HydraVector("b");
      h = new HydraVector("c");
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAt( HydraTestPrms.prm06, 0, new HydraVector("Z") );
      w = new HydraVector("a");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAt( HydraTestPrms.prm06, 1, new HydraVector("Z") );
      w = new HydraVector("b");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAt( HydraTestPrms.prm06, 2, new HydraVector("Z") );
      w = new HydraVector("c");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAt( HydraTestPrms.prm06, 3, new HydraVector("Z") );
      w = new HydraVector("Z");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAtWild( HydraTestPrms.prm06, 0, new HydraVector("Z") );
      w = new HydraVector("a");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAtWild( HydraTestPrms.prm06, 1, new HydraVector("Z") );
      w = new HydraVector("b");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAtWild( HydraTestPrms.prm06, 2, new HydraVector("Z") );
      w = new HydraVector("c");
      Assert.assertTrue( equals(v,w), v + "!=" + w );
    v = tab.vecAtWild( HydraTestPrms.prm06, 3, new HydraVector("Z") );
      w = new HydraVector("c");
      Assert.assertTrue( equals(v,w), v + "!=" + w );

  }
  private static boolean equals( HydraVector v, HydraVector w ) {
    if ( v == null ) {
      return ( w == null );
    } else {
      return v.equals( w );
    }
  }
  public static void checkPlusEquals() {
    ConfigHashtable tab = TestConfig.tab();
    Object o;
    String s, t;
    HydraVector v,w;
    HydraVector f,g,h;

    /**
     *  Simple parameters
     */

    // hydratest.HydraTestPrms-prm01  = a;
    o = tab.get( HydraTestPrms.prm01 );
    Assert.assertTrue( o instanceof String, o + "!String" );
    s = (String) o;
      t = "a";
      Assert.assertTrue( s.equals(t), s + "!=" + t );

    // hydratest.HydraTestPrms-prm02 += a;
    o = tab.get( HydraTestPrms.prm02 );
    Assert.assertTrue( o instanceof String, o + "!String" );
    s = (String) o;
      t = "a";
      Assert.assertTrue( s.equals(t), s + "!=" + t );

    // hydratest.HydraTestPrms-prm03  = a;
    // hydratest.HydraTestPrms-prm03 += b;
    o = tab.get( HydraTestPrms.prm03 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      w = new HydraVector( "a" ); w.add( "b" );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm04 += a;
    // hydratest.HydraTestPrms-prm04 += b;
    o = tab.get( HydraTestPrms.prm04 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      w = new HydraVector( "a" ); w.add( "b" );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm05  = a1 a2;
    o = tab.get( HydraTestPrms.prm05 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      w = new HydraVector( "a1" ); w.add( "a2" );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm06 += a1 a2;
    o = tab.get( HydraTestPrms.prm06 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      w = new HydraVector( "a1" ); w.add( "a2" );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm07  = a1 a2;
    // hydratest.HydraTestPrms-prm07 += b1 b2;
    o = tab.get( HydraTestPrms.prm07 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      w = new HydraVector( "a1" ); w.add( "a2" ); w.add( "b1" ); w.add( "b2" );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm08 += a1 a2;
    // hydratest.HydraTestPrms-prm08 += b1 b2;
    o = tab.get( HydraTestPrms.prm08 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      w = new HydraVector( "a1" ); w.add( "a2" ); w.add( "b1" ); w.add( "b2" );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm09  = a;
    // hydratest.HydraTestPrms-prm09 += b1 b2;
    o = tab.get( HydraTestPrms.prm09 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      w = new HydraVector( "a" ); w.add( "b1" ); w.add( "b2" );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm10 += a;
    // hydratest.HydraTestPrms-prm10 += b1 b2;
    o = tab.get( HydraTestPrms.prm10 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      w = new HydraVector( "a" ); w.add( "b1" ); w.add( "b2" );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm11  = a1 a2;
    // hydratest.HydraTestPrms-prm11 += b;
    o = tab.get( HydraTestPrms.prm11 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      w = new HydraVector( "a1" ); w.add( "a2" ); w.add( "b" );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm12 += a1 a2;
    // hydratest.HydraTestPrms-prm12 += b;
    o = tab.get( HydraTestPrms.prm12 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      w = new HydraVector( "a1" ); w.add( "a2" ); w.add( "b" );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    /**
     *  List parameters
     */

    // hydratest.HydraTestPrms-prm13  = a1 , a2 , a3;
    o = tab.get( HydraTestPrms.prm13 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a1" );
      g = new HydraVector( "a2" );
      h = new HydraVector( "a3" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm14 += a1 , a2 , a3;
    o = tab.get( HydraTestPrms.prm14 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a1" );
      g = new HydraVector( "a2" );
      h = new HydraVector( "a3" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm15  = a1 , a2 , a3;
    // hydratest.HydraTestPrms-prm15 += b;
    o = tab.get( HydraTestPrms.prm15 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a1" ); f.add( "b" );
      g = new HydraVector( "a2" ); g.add( "b" );
      h = new HydraVector( "a3" ); h.add( "b" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm16 += a1 , a2 , a3;
    // hydratest.HydraTestPrms-prm16 += b;
    o = tab.get( HydraTestPrms.prm16 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a1" ); f.add( "b" );
      g = new HydraVector( "a2" ); g.add( "b" );
      h = new HydraVector( "a3" ); h.add( "b" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm17  = a;
    // hydratest.HydraTestPrms-prm17 += b1 , b2 , b3;
    o = tab.get( HydraTestPrms.prm17 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a" ); f.add( "b1" );
      g = new HydraVector( "a" ); g.add( "b2" );
      h = new HydraVector( "a" ); h.add( "b3" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm18 += a;
    // hydratest.HydraTestPrms-prm18 += b1 , b2 , b3;
    o = tab.get( HydraTestPrms.prm18 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a" ); f.add( "b1" );
      g = new HydraVector( "a" ); g.add( "b2" );
      h = new HydraVector( "a" ); h.add( "b3" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm19  = a1 , a2 , a3;
    // hydratest.HydraTestPrms-prm19 += b1 , b2 , b3;
    o = tab.get( HydraTestPrms.prm19 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a1" ); f.add( "b1" );
      g = new HydraVector( "a2" ); g.add( "b2" );
      h = new HydraVector( "a3" ); h.add( "b3" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm20 += a1 , a2 , a3;
    // hydratest.HydraTestPrms-prm20 += b1 , b2 , b3;
    o = tab.get( HydraTestPrms.prm20 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a1" ); f.add( "b1" );
      g = new HydraVector( "a2" ); g.add( "b2" );
      h = new HydraVector( "a3" ); h.add( "b3" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm21  = a1 , a2;
    // hydratest.HydraTestPrms-prm21 += b1 , b2, b3;
    o = tab.get( HydraTestPrms.prm21 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a1" ); f.add( "b1" );
      g = new HydraVector( "a2" ); g.add( "b2" );
      h = new HydraVector( "a2" ); h.add( "b3" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm22 += a1 , a2;
    // hydratest.HydraTestPrms-prm22 += b1 , b2, b3;
    o = tab.get( HydraTestPrms.prm22 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a1" ); f.add( "b1" );
      g = new HydraVector( "a2" ); g.add( "b2" );
      h = new HydraVector( "a2" ); h.add( "b3" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm23  = a1 , a2, a3;
    // hydratest.HydraTestPrms-prm23 += b1 , b2;
    o = tab.get( HydraTestPrms.prm23 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a1" ); f.add( "b1" );
      g = new HydraVector( "a2" ); g.add( "b2" );
      h = new HydraVector( "a3" ); h.add( "b2" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm24 += a1 , a2, a3;
    // hydratest.HydraTestPrms-prm24 += b1 , b2;
    o = tab.get( HydraTestPrms.prm24 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a1" ); f.add( "b1" );
      g = new HydraVector( "a2" ); g.add( "b2" );
      h = new HydraVector( "a3" ); h.add( "b2" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    /**
     *  List of list parameters
     */

    // hydratest.HydraTestPrms-prm25  = a11 a12 , a21 a22 , a31 a32;
    o = tab.get( HydraTestPrms.prm25 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a11" ); f.add( "a12" );
      g = new HydraVector( "a21" ); g.add( "a22" );
      h = new HydraVector( "a31" ); h.add( "a32" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm26 += a11 a12 , a21 a22 , a31 a32;
    o = tab.get( HydraTestPrms.prm26 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a11" ); f.add( "a12" );
      g = new HydraVector( "a21" ); g.add( "a22" );
      h = new HydraVector( "a31" ); h.add( "a32" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm27  = a11 a12 , a21 a22 , a31 a32;
    // hydratest.HydraTestPrms-prm27 += b;
    o = tab.get( HydraTestPrms.prm27 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a11" ); f.add( "a12" ); f.add("b" );
      g = new HydraVector( "a21" ); g.add( "a22" ); g.add("b" );
      h = new HydraVector( "a31" ); h.add( "a32" ); h.add("b" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm28 += a11 a12 , a21 a22 , a31 a32;
    // hydratest.HydraTestPrms-prm28 += b;
    o = tab.get( HydraTestPrms.prm28 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a11" ); f.add( "a12" ); f.add("b" );
      g = new HydraVector( "a21" ); g.add( "a22" ); g.add("b" );
      h = new HydraVector( "a31" ); h.add( "a32" ); h.add("b" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm29  = a;
    // hydratest.HydraTestPrms-prm29 += b11 b12 , b21 b22 , b31 b32;
    o = tab.get( HydraTestPrms.prm29 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a" ); f.add( "b11" ); f.add("b12" );
      g = new HydraVector( "a" ); g.add( "b21" ); g.add("b22" );
      h = new HydraVector( "a" ); h.add( "b31" ); h.add("b32" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm30 += a;
    // hydratest.HydraTestPrms-prm30 += b11 b12 , b21 b22 , b31 b32;
    o = tab.get( HydraTestPrms.prm30 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a" ); f.add( "b11" ); f.add("b12" );
      g = new HydraVector( "a" ); g.add( "b21" ); g.add("b22" );
      h = new HydraVector( "a" ); h.add( "b31" ); h.add("b32" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm31  = a11 a12 , a21 a22 , a31 a32;
    // hydratest.HydraTestPrms-prm31 += b11 b12 , b21 b22 , b31 b32;
    o = tab.get( HydraTestPrms.prm31 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a11" ); f.add( "a12" ); f.add( "b11" ); f.add("b12" );
      g = new HydraVector( "a21" ); g.add( "a22" ); g.add( "b21" ); g.add("b22" );
      h = new HydraVector( "a31" ); h.add( "a32" ); h.add( "b31" ); h.add("b32" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm32 += a11 a12 , a21 a22 , a31 a32;
    // hydratest.HydraTestPrms-prm32 += b11 b12 , b21 b22 , b31 b32;
    o = tab.get( HydraTestPrms.prm32 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a11" ); f.add( "a12" ); f.add( "b11" ); f.add("b12" );
      g = new HydraVector( "a21" ); g.add( "a22" ); g.add( "b21" ); g.add("b22" );
      h = new HydraVector( "a31" ); h.add( "a32" ); h.add( "b31" ); h.add("b32" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm33  = a11 a12 , a21 a22;
    // hydratest.HydraTestPrms-prm33 += b11 b12 , b21 b22 , b31 b32;
    o = tab.get( HydraTestPrms.prm33 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a11" ); f.add( "a12" ); f.add( "b11" ); f.add("b12" );
      g = new HydraVector( "a21" ); g.add( "a22" ); g.add( "b21" ); g.add("b22" );
      h = new HydraVector( "a21" ); h.add( "a22" ); h.add( "b31" ); h.add("b32" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm34 += a11 a12 , a2;
    // hydratest.HydraTestPrms-prm34 += b11 b12 , b21 b22 , b31 b32;
    o = tab.get( HydraTestPrms.prm34 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a11" ); f.add( "a12" ); f.add( "b11" ); f.add("b12" );
      g = new HydraVector( "a2" ); g.add( "b21" ); g.add("b22" );
      h = new HydraVector( "a2" ); h.add( "b31" ); h.add("b32" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm35  = a11 a12 , a21 a22 , a31 a32;
    // hydratest.HydraTestPrms-prm35 += b11 b12 , b21 b22;
    o = tab.get( HydraTestPrms.prm35 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a11" ); f.add( "a12" ); f.add( "b11" ); f.add("b12" );
      g = new HydraVector( "a21" ); g.add( "a22" ); g.add( "b21" ); g.add("b22" );
      h = new HydraVector( "a31" ); h.add( "a32" ); h.add( "b21" ); h.add("b22" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm36 += a11 a12 , a21 a22 , a31 a32;
    // hydratest.HydraTestPrms-prm36 += b11 b12 , b21 b22;
    o = tab.get( HydraTestPrms.prm36 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a11" ); f.add( "a12" ); f.add( "b11" ); f.add("b12" );
      g = new HydraVector( "a21" ); g.add( "a22" ); g.add( "b21" ); g.add("b22" );
      h = new HydraVector( "a31" ); h.add( "a32" ); h.add( "b21" ); h.add("b22" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    /**
     *  Mixed parameters
     */

    // hydratest.HydraTestPrms-prm37  = a1, a21 a22 , a31 a32;
    // hydratest.HydraTestPrms-prm37 += b;
    o = tab.get( HydraTestPrms.prm37 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a1" ); f.add("b" );
      g = new HydraVector( "a21" ); g.add( "a22" ); g.add( "b" );
      h = new HydraVector( "a31" ); h.add( "a32" ); h.add( "b" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm38 += a1, a21 a22 , a31 a32;
    // hydratest.HydraTestPrms-prm38 += b;
    o = tab.get( HydraTestPrms.prm38 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a1" ); f.add("b" );
      g = new HydraVector( "a21" ); g.add( "a22" ); g.add( "b" );
      h = new HydraVector( "a31" ); h.add( "a32" ); h.add( "b" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm39  = a;
    // hydratest.HydraTestPrms-prm39 += b1, b21 b22 , b31 b32;
    o = tab.get( HydraTestPrms.prm39 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a" ); f.add("b1" );
      g = new HydraVector( "a" ); g.add( "b21" ); g.add( "b22" );
      h = new HydraVector( "a" ); h.add( "b31" ); h.add( "b32" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm40 += a;
    // hydratest.HydraTestPrms-prm40 += b1, b21 b22 , b31 b32;
    o = tab.get( HydraTestPrms.prm40 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a" ); f.add("b1" );
      g = new HydraVector( "a" ); g.add( "b21" ); g.add( "b22" );
      h = new HydraVector( "a" ); h.add( "b31" ); h.add( "b32" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm41  = a11 a12 , a2 , a31 a32;
    // hydratest.HydraTestPrms-prm41 += b;
    o = tab.get( HydraTestPrms.prm41 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a11" ); f.add( "a12" ); f.add("b" );
      g = new HydraVector( "a2" ); g.add( "b" );
      h = new HydraVector( "a31" ); h.add( "a32" ); h.add( "b" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm42 += a11 a12 , a2 , a31 a32;
    // hydratest.HydraTestPrms-prm42 += b;
    o = tab.get( HydraTestPrms.prm42 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a11" ); f.add( "a12" ); f.add("b" );
      g = new HydraVector( "a2" ); g.add( "b" );
      h = new HydraVector( "a31" ); h.add( "a32" ); h.add( "b" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm43  = a;
    // hydratest.HydraTestPrms-prm43 += b11 b12 , b2 , b31 b32;
    o = tab.get( HydraTestPrms.prm43 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a" ); f.add( "b11" ); f.add("b12" );
      g = new HydraVector( "a" ); g.add( "b2" );
      h = new HydraVector( "a" ); h.add( "b31" ); h.add( "b32" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );

    // hydratest.HydraTestPrms-prm44 += a;
    // hydratest.HydraTestPrms-prm44 += b11 b12 , b2 , b31 b32;
    o = tab.get( HydraTestPrms.prm44 );
    Assert.assertTrue( o instanceof HydraVector, o + "!HydraVector" );
    v = (HydraVector) o;
      f = new HydraVector( "a" ); f.add( "b11" ); f.add("b12" );
      g = new HydraVector( "a" ); g.add( "b2" );
      h = new HydraVector( "a" ); h.add( "b31" ); h.add( "b32" );
      w = new HydraVector( f ); w.add( g ); w.add( h );
      Assert.assertTrue( equals(v,w), v + "!=" + w );
  }
//  private static boolean plusequals( HydraVector v, HydraVector w ) {
//    if ( v == null || w == null ) {
//      return false;
//    } else {
//      return v.equals( w );
//    }
//  }
  public static void checkHydraSubthread() {
    Runnable r = new Runnable() {
      public void run() {
        RemoteTestModule mod = RemoteTestModule.getCurrentThread();
        TestTask task = mod.getCurrentTask();
        Log.getLogWriter().info("VM " + RemoteTestModule.getMyVmid() + " running " +
                                 task.getTaskTypeString());
      }
    };
    HydraSubthread t = new HydraSubthread(r);
    Log.getLogWriter().info("Starting subthread of " + t.getRemoteMod());
    t.start();
    try {
      t.join();
    } catch (InterruptedException e) {
    }
  }
}
