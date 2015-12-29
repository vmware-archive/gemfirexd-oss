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

package perffmwk;

import hydra.*;

//import java.io.*;
import java.util.*;
//import java.util.regex.*;

// @todo Determine the difference between "test properties" and
//       "system properties"
/**
 *
 *  A StatConfig contains a statistics specification, trim specification,
 *  and latest properties.  It is used to hold results of generating or
 *  parsing statistics.spec, trim.spec, and latest.prop files.
 *  <p>
 *  Instances of this class are not available to hydra clients.
 *
 */

public class StatConfig {

  private String testDir;
  private Properties latest = null;
  private Properties testprops = null;
  private SortedMap trimspecs = new TreeMap();
  private SortedMap statspecs = new TreeMap();
  private Map sysprops = new HashMap();
  boolean isNativeClient = false; // whether this is a native client test

  /** Cache of matching archives for specs (key is logical spec name,
   * val is list of archives) */
  protected Map matchingArchiveCache = new HashMap();

  //////////////////////////////////////////////////////////////////////////////
  ////    CONSTRUCTORS                                                      ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Creates a new <code>StatConfig</code> for a test whose output
   * resides in the given directory.
   */
  protected StatConfig( String testDir ) {
    // assumed to be absolute path
    this.testDir = testDir;

    // assumed to be a native client test if there is a driver log
    this.isNativeClient = FileUtil.exists(this.testDir + "/Driver.log");
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    INSTANCE METHODS                                                  ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Returns the name of the test that was run to generate this
   * <code>StatConfig</code>. 
   */
  protected String getTestName() {
    return this.latest == null ? null : this.latest.getProperty( "TestName" );
  }

  /**
   * Returns whether this is a native client test.
   */
  protected boolean isNativeClient() {
    return this.isNativeClient;
  }

  /**
   * Returns the description of the test that was run to generate this
   * <code>StatConfig</code>. 
   */
  protected String getTestDescription() {
    return this.latest == null ? null : this.latest.getProperty( "hydra.Prms-testDescription" );
  }

  /**
   * Returns the repository of GemFire source used by the test that was run to
   * generate this <code>StatConfig</code>.
   */
  protected String getSourceRepository() {
    return this.latest == null ? null
                               : this.latest.getProperty("source.repository");
  }

  /**
   * Returns the revision of GemFire source used by the test that was run to
   * generate this <code>StatConfig</code>.
   */
  protected String getSourceRevision() {
    return this.latest == null ? null
                               : this.latest.getProperty("source.revision");
  }

  /**
   * Returns the date of GemFire source used by the test that was run to
   * generate this <code>StatConfig</code>.
   */
  protected String getSourceDate() {
    return this.latest == null ? null
                               : this.latest.getProperty("source.date");
  }

  /**
   * Returns the version of GemFire used by the test that was run to
   * generate this <code>StatConfig</code>.
   */
  protected String getBuildVersion() {
    return this.latest == null ? null : this.latest.getProperty( "build.version" );
  }

  /**
   * Returns the build date of the version of GemFire used by the test
   * that was run to generate this <code>StatConfig</code>.
   */
  protected String getBuildDate() {
    return this.latest == null ? null : this.latest.getProperty( "build.date" );
  }

  /**
   * Returns the version of JDK used by the build that was run to
   * generate this <code>StatConfig</code>.
   */
  protected String getBuildJDK() {
    return this.latest == null ? null : this.latest.getProperty( "build.jdk" );
  }

  /**
   * Returns the repository of native client source used by the test that was
   * run to generate this <code>StatConfig</code>.
   */
  protected String getNativeClientSourceRepository() {
    return this.latest == null ? null
                               : this.latest.getProperty("nativeClient.repository");
  }

  /**
   * Returns the revision of C++ source used by the test that was run to
   * generate this <code>StatConfig</code>.
   */
  protected String getNativeClientSourceRevision() {
    return this.latest == null ? null
                               : this.latest.getProperty("nativeClient.revision");
  }

  /**
   * Returns the version of native client used by the test that was run to
   * generate this <code>StatConfig</code>.
   */
  protected String getNativeClientBuildVersion() {
    return this.latest == null ? null
                               : this.latest.getProperty("nativeClient.version");
  }

  /**
   * Returns the version of JDK used by the test that was run to
   * generate this <code>StatConfig</code>.
   */
  protected String getRuntimeJDK() {
    return this.latest == null ? null : this.latest.getProperty( "runtime.jdk" );
  }

  /**
   * Returns the Java VM name used by the test that was run to
   * generate this <code>StatConfig</code>.
   */
  protected String getJavaVMName() {
    return this.latest == null ? null : this.latest.getProperty("java.vm.name");
  }

  /**
   * Returns the test output directory from which this
   * <code>StatConfig</code> was created.
   */
  protected String getTestDir() {
    return this.testDir;
  }

  /**
   * Sets the properties from latest.prop for the test run from which
   * this <code>StatConfig</code> was created.
   */
  protected void setLatestProperties( Properties p ) {
    this.latest = p;
  }

  /**
   * Returns the properties from latest.prop for the test run from
   * which this <code>StatConfig</code> was created.
   */
  protected Properties getLatestProperties() {
    return this.latest;
  }

  /**
   * Returns the test properties (a.k.a. the "test config
   * properties") for the test run from which this
   * <code>StatConfig</code> was created.
   */
  protected void setTestProperties( Properties p ) {
    this.testprops = p;
  }

  /**
   * Returns the test properties (a.k.a. the "test config
   * properties") for the test run from which this
   * <code>StatConfig</code> was created.
   */
  protected Properties getTestProperties() {
    return this.testprops;
  }

  protected Map getSystemProperties() {
    return this.sysprops;
  }

  protected void addSystemProperty( String key, String val ) {
    this.sysprops.put( key, val );
  }

  /**
   * Returns all of the statistic {@link TrimSpec}s for this test run.
   * The keys of the map are the names of the trim specs and the
   * values are the <code>TrimSpec</code> objects.
   */
  protected SortedMap getTrimSpecs() {
    return this.trimspecs;
  }

  /**
   * Returns the <code>TrimSpec</code> with the given name.  If a
   * <code>TrimSpec</codE> with that name does not exist, a new one
   * will be created.
   */
  protected synchronized TrimSpec getTrimSpec( String name ) {
    TrimSpec trimspec = (TrimSpec) this.trimspecs.get( name );
    if ( trimspec == null ) {
      trimspec = (TrimSpec) this.trimspecs.get( name );
      if ( trimspec == null ) {
	trimspec = new TrimSpec( name );
	addTrimSpec( trimspec );
      }
    }
    return trimspec;
  }

  /**
   * Adds a <code>TrimSpec</code> for the test run represented by this
   * <code>StatConfig</code>.
   */
  protected void addTrimSpec( TrimSpec trimspec ) {
    String name = trimspec.getName();
    if ( this.trimspecs.get( name ) != null ) {
      String s = "Duplicate trim specification with name: " + name;
      throw new StatConfigException( s );
    }
    this.trimspecs.put( name, trimspec );
  }

  /**
   * Returns the statistics spec with the given id.
   */
  protected Vector getStatSpecsWithId( StatSpecId id ) {
    Vector matches = null;
    String idstr = id.toString();
    synchronized( this.statspecs ) {
      for ( Iterator i = this.statspecs.values().iterator(); i.hasNext(); ) {
	StatSpec statspec = (StatSpec) i.next();
	if ( statspec.getId().toString().startsWith( idstr ) ) {
	  if ( matches == null ) {
	    matches = new Vector();
	  }
	  matches.add( statspec );
	}
	  
      }
    }
    return matches;
  }

  /**
   * Returns the <code>StatSpec</code> with the given key.  The key is
   * composed of the stat spec's {@linkplain StatSpec#getId id} and
   * {@linkplain StatSpec#getName name}.
   */
  protected StatSpec getStatSpecByKey( String key ) {
    return (StatSpec) this.statspecs.get( key );
  }

  /**
   * Returns the <code>StatSpec</code> with the given name.  If no
   * <code>StatSpec</code> with that name exists, then
   * <code>null</code> is returned.
   */
  protected StatSpec getStatSpec( String name ) {
    for ( Iterator i = this.statspecs.values().iterator(); i.hasNext(); ) {
      StatSpec statspec = (StatSpec) i.next();
      if ( statspec.getName().equals( name ) )
        return statspec;
    }
    return null;
  }

  /**
   * Returns all of the <code>StatSpec</code>s from the test run from
   * which this <code>StatConfig</code> was created.
   */
  protected SortedMap getStatSpecs() {
    return this.statspecs;
  }

  /**
   * Returns all of the <code>StatSpec</code>s from the test run from
   * which this <code>StatConfig</code> was created.  Excludes Exprs.
   */
  protected Map getStatSpecInstances() {
    SortedMap map = new TreeMap();
    for (Iterator i = this.statspecs.keySet().iterator(); i.hasNext();) {
      String key = (String)i.next();
      Object obj = this.statspecs.get(key);
      if (!(obj instanceof Expr)) {
        map.put(key, obj);
      }
    }
    return map;
  }

  /**
   * Returns all of the <code>Expr</code>s from the test run from
   * which this <code>StatConfig</code> was created.  Excludes StatSpecs.
   */
  protected Map getExprInstances() {
    SortedMap map = new TreeMap();
    for (Iterator i = this.statspecs.keySet().iterator(); i.hasNext();) {
      String key = (String)i.next();
      Object obj = this.statspecs.get(key);
      if (obj instanceof Expr) {
        map.put(key, obj);
      }
    }
    return map;
  }

  /**
   * Adds a <code>StatSpec</code> to this <code>StatConfig</code>.
   */
  protected synchronized void addStatSpec( StatSpec statspec ) {
    String name = statspec.getName();
    synchronized( this.statspecs ) {
      if (getStatSpec(name) != null) {
          throw new StatConfigException( "Logical statspec name " + name + " occurs more than once in statspec file" );
      }
      putStatSpec(statspec);
    }
  }

  /**
   * Adds a statspec whether it already exists or not.
   */
  private void putStatSpec(StatSpec statspec) {
    if (statspec instanceof Expr) {
      this.statspecs.put(statspec.getName(), statspec);
    } else {
      String key = statspec.getName() + " " + statspec.getId().toString();
      this.statspecs.put(key, statspec);
    }
  }

  /**
   * {@linkplain StatSpec#setStatConfig Associates} this
   * <code>StatConfig</code> all of the {@link StatSpec}s in the given
   * map.
   */
  protected void setStatSpecs( SortedMap statspecs ) {
    this.statspecs = statspecs;
    for ( Iterator i = this.statspecs.values().iterator(); i.hasNext(); ) {
      StatSpec statspec = (StatSpec) i.next();
      statspec.setStatConfig( this );
    }
  }

  /**
   * Resets the stat of this <code>StatConfig</code>
   */
  protected void resetStatSpecs() {
    this.statspecs = new TreeMap();
    this.matchingArchiveCache = new HashMap();
  }

  /**
   * Returns a list of archives associated with the given spec.
   */
  protected List getMatchingArchiveCache( String specname ) {
    return (List) this.matchingArchiveCache.get( specname );
  }

  /**
   * Associates the list of arhives with the given spec.
   */
  protected void setMatchingArchiveCache( String specname, List archives ) {
    this.matchingArchiveCache.put( specname, archives );
  }

  //////////////////////////////////////////////////////////////////////////// 
  ////    COMPARING                                                       ////
  //////////////////////////////////////////////////////////////////////////// 

  /** 
   * Used by {@link PerfComparer} to throw out stats
   * (<code>StatSpec</code>s) that will not be compared.
   *
   * @see StatSpec#distill
   */
  protected SortedMap distillStatSpecsForComparison() {
    if ( this.statspecs != null ) {
      SortedMap distilledstatspecs = new TreeMap();
      for ( Iterator i = this.statspecs.keySet().iterator(); i.hasNext(); ) {
        String key = (String) i.next();
	StatSpec statspec = (StatSpec) this.statspecs.get( key );
	StatSpec distilledstatspec = statspec.distill();
	if ( distilledstatspec != null ) {
	  distilledstatspecs.put( key, distilledstatspec );
	}
      }
      this.statspecs = distilledstatspecs;

      // ensure that stats needed for expressions are included
      for (Iterator i = this.statspecs.keySet().iterator(); i.hasNext();) {
        String key = (String)i.next();
	Object statspec = this.statspecs.get(key);
        if (statspec instanceof Expr) {
          Expr expr = (Expr)statspec;
          putStatSpec(expr.getNumerator());
          putStatSpec(expr.getDenominator());
        }
      }
    }
    return this.statspecs;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    HYDRA SYSTEM PROPERTIES                                           ////
  //////////////////////////////////////////////////////////////////////////////

//  /**
//   *  Returns the system properties in the properties.
//   */
//  private Vector getSystemProperties( Properties p ) {
//    Vector result = new Vector();
//    String names = p.getProperty( "hydra.GemFirePrms-names" );
//    StringTokenizer s = new StringTokenizer( names, " ", false );
//    while ( s.hasMoreTokens() ) {
//      String token = s.nextToken();
//      if ( ! token.equals( "[" ) && ! token.equals( "]" )
//                                 && ! token.equals( "HYDRAVECTOR" ) ) {
//        StringTokenizer ss = new StringTokenizer( token, ",", false );
//        while ( ss.hasMoreTokens() ) {
//          result.add( ss.nextToken() );
//        }
//      }
//    }
//    return result.size() == 0 ? null : result;
//  }

  //////////////////////////////////////////////////////////////////////////////
  ////    STATISTIC ARCHIVES                                                ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Returns all statistic archives for the test in a single list.
   */
  protected List getStatisticArchivesAsList() {
    List archives = null;
    if (this.isNativeClient) {
      return TestFileUtil.getNativeClientStatisticArchivesAsList(this.testDir);
    } else {
      archives = TestFileUtil.getStatisticArchivesAsList(this.testDir);
      if (archives == null || archives.size() == 0) {
        return null;
      }
    }
    return archives;
  }

  /**
   *  Returns all statistic archives for the test mapped by system name.
   */
  protected SortedMap getStatisticArchives() {
    SortedMap archives = TestFileUtil.getStatisticArchives(this.testDir);
    if (archives == null || archives.size() == 0) {
      return null;
    }
    return archives;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    PRINTING                                                          ////
  //////////////////////////////////////////////////////////////////////////////

  public String toString() {
    StringBuffer buf = new StringBuffer();
    if ( this.trimspecs.size() > 0 ) {
        buf.append( "\nTrim Specifications\n\n" );
        for ( Iterator i = this.trimspecs.values().iterator(); i.hasNext(); ) {
          TrimSpec trimspec = (TrimSpec) i.next();
          buf.append( trimspec.toSpecString() + "\n" );
        }
    }
    if ( this.statspecs.size() > 0 ) {
        buf.append( "\nStatistic Specifications\n\n" );
        for ( Iterator i = this.statspecs.values().iterator(); i.hasNext(); ) {
          StatSpec statspec = (StatSpec) i.next();
          buf.append( statspec.toSpecString() + "\n" );
        }
    }
    //buf.append( "\nLatest.Props\n\n" ).append( this.latest );
    return buf.toString();
  }
}
