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

import com.gemstone.gemfire.internal.*;
import com.gemstone.gemfire.*;

import hydra.Log;

import java.io.*;
import java.util.*;
import java.util.regex.*;

/** 
 *
 *  Holds an individual statistics specification.
 *
 */

public class StatSpec implements StatArchiveReader.StatSpec, Serializable {

  /** The logical name of the spec */
  protected String name;

  /** The stat spec id */
  private StatSpecId id;

  /** The trim spec name */
  private String trimspecName;

  /** The stat config */
  protected StatConfig statconfig;

  /** Determines if the class type should be an exact match; this is useful
   *  for stats that have the same name in types that have similar names,
   *  such as the product's CachePerfStats and the framework's 
   *  cacheperf.CachePerfStats */
  protected boolean exactMatch = false;

  /** The description of the statistic referenced by this spec */
  private String description = "No description";

  private int filter = StatArchiveReader.StatValue.FILTER_PERSEC;
  private int combine = NONE; // this is the same as "raw"

  private boolean min = false;
  private boolean max = false;
  private boolean maxminusmin = false;
  private boolean mean = false;
  private boolean stddev = false;

  private boolean minExpr = false;
  private boolean maxExpr = false;
  private boolean maxminusminExpr = false;
  private boolean meanExpr = false;
  private boolean stddevExpr = false;

  private boolean minCompare = false;
  private boolean maxCompare = false;
  private boolean maxminusminCompare = false;
  private boolean meanCompare = false;
  private boolean stddevCompare = false;

  //////////////////////////////////////////////////////////////////////////// 
  ////    CONSTRUCTORS                                                    ////
  //////////////////////////////////////////////////////////////////////////// 

  protected StatSpec( String name ) {
    this.name = name;
  }
  protected StatSpec( PerformanceStatistics statInst, StatisticDescriptor statDesc ) {
    this.name = null;
    this.id = new StatSpecId( statInst, statDesc );
    this.trimspecName = statInst.getTrimSpecName();

    if ( statDesc.isCounter() )
      this.filter = StatArchiveReader.StatValue.FILTER_PERSEC;
    else
      this.filter = StatArchiveReader.StatValue.FILTER_NONE;

    this.combine = GLOBAL;

    if ( this.filter == StatArchiveReader.StatValue.FILTER_PERSEC ) {
      this.min = true;
      this.max = true;
      this.mean = true;
      this.stddev = true;
      this.meanCompare = true;
    } else { // FILTER_NONE
      if ( statDesc.isCounter() ) {
        this.maxminusmin = true;
	this.maxminusminCompare = true;
      } else { // gauge
        this.min = true;
        this.max = true;
        this.maxCompare = true;
        this.mean = true;
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////// 
  ////    StatArchiveReader.StatSpec                                      ////
  //////////////////////////////////////////////////////////////////////////// 

  /**
   *  Implements {@link com.gemstone.gemfire.internal.StatArchiveReader.StatSpec#getCombineType}.
   */
  public int getCombineType() {
    return this.combine;
  }

  //////////////////////////////////////////////////////////////////////////// 
  ////    StatArchiveReader.ValueFilter                                   ////
  //////////////////////////////////////////////////////////////////////////// 

  public boolean archiveMatches(File archive) {
    if (this.statconfig != null && this.statconfig.isNativeClient()) {
      return true; // native client tests always use all archives
    }
    return getMatchingArchives().contains(archive);
  }

  public boolean typeMatches(String typeName) {
    if (exactMatch) {
      return typeName.toLowerCase().equals(getTypeName().toLowerCase());
    } else {
      return typeName.toLowerCase().endsWith(getTypeName().toLowerCase());
    }
  }

  public boolean statMatches(String statName) {
    return getStatName().equalsIgnoreCase(statName);
  }

  public boolean instanceMatches(String textId, long numericId) {
    Matcher m = getInstancePattern().matcher(textId);
    if (m.matches()) {
      return true;
    }
    m = getInstancePattern().matcher(String.valueOf(numericId));
    return m.matches();
  }

  public String getName() {
    return this.name;
  }
  public String getTypeName() {
    return this.id.getTypeName();
  }
  public Pattern getInstancePattern() {
    return this.id.getInstancePattern();
  }
  public String getStatName() {
    return this.id.getStatName();
  }
  protected List getMatchingArchives() {
    List archives = this.statconfig.getMatchingArchiveCache( this.name );
    if ( archives == null ) {
      SortedMap allArchives = this.statconfig.getStatisticArchives();
      archives = findMatchingArchives( allArchives, getId() );
      this.statconfig.setMatchingArchiveCache( this.name, archives );
    }
    return archives;
  }
  protected List findMatchingArchives( SortedMap allArchives, StatSpecId id ) {
    List matchingArchives = new ArrayList();
    Pattern pattern = id.getSystemPattern();
    for ( Iterator i = allArchives.keySet().iterator(); i.hasNext(); ) {
      String logicalSystemName = (String) i.next();
      if ( pattern.matcher( logicalSystemName ).matches() ) {
	List archives = (List) allArchives.get( logicalSystemName );
	if ( archives != null ) {
	  matchingArchives.addAll( archives );
	  if ( Log.getLogWriter().finestEnabled() ) {
	    Log.getLogWriter().finest( "Matched " + id.getSystemName() + " ==> " + archives );
	  }
	}
      }
    }
    return matchingArchives;
  }

  //////////////////////////////////////////////////////////////////////////// 
  ////    ACCESSORS                                                       ////
  //////////////////////////////////////////////////////////////////////////// 

  public void setExactMatch( boolean boolArg ) {
    this.exactMatch = boolArg;
  }
  public void setStatConfig( StatConfig statconfig ) {
    this.statconfig = statconfig;
  }
  public StatSpecId getId() {
    return this.id;
  }
  public void setId( StatSpecId id ) {
    this.id = id;
  }
  public int getFilter() {
    return this.filter;
  }
  public void setFilter( String filterType ) {
    this.filter = getFilterFromString( filterType );
  }
  public void setFilter( int filter ) {
    this.filter = filter;
  }
  public void setCombineType( String combineType ) {
    this.combine = getCombineFromString( combineType );
  }
  public void setCombineType( int combine ) {
    this.combine = combine;
  }
  public boolean getMin() {
    return this.min;
  }
  public void setMin( boolean b ) {
    this.min = b;
  }
  public boolean getMax() {
    return this.max;
  }
  public void setMax( boolean b ) {
    this.max = b;
  }
  public boolean getMaxMinusMin() {
    return this.maxminusmin;
  }
  public void setMaxMinusMin( boolean b ) {
    this.maxminusmin = b;
  }
  public boolean getMean() {
    return this.mean;
  }
  public void setMean( boolean b ) {
    this.mean = b;
  }
  public boolean getStddev() {
    return this.stddev;
  }
  public void setStddev( boolean b ) {
    this.stddev = b;
  }
  public boolean getMinExpr() {
    return this.minExpr;
  }
  public void setMinExpr( boolean b ) {
    this.minExpr = b;
  }
  public boolean getMaxExpr() {
    return this.maxExpr;
  }
  public void setMaxExpr( boolean b ) {
    this.maxExpr = b;
  }
  public boolean getMaxMinusMinExpr() {
    return this.maxminusminExpr;
  }
  public void setMaxMinusMinExpr( boolean b ) {
    this.maxminusminExpr = b;
  }
  public boolean getMeanExpr() {
    return this.meanExpr;
  }
  public void setMeanExpr( boolean b ) {
    this.meanExpr = b;
  }
  public boolean getStddevExpr() {
    return this.stddevExpr;
  }
  public void setStddevExpr( boolean b ) {
    this.stddevExpr = b;
  }
  public boolean getMinCompare() {
    return this.minCompare;
  }
  public void setMinCompare( boolean b ) {
    this.minCompare = b;
  }
  public boolean getMaxCompare() {
    return this.maxCompare;
  }
  public void setMaxCompare( boolean b ) {
    this.maxCompare = b;
  }
  public boolean getMaxMinusMinCompare() {
    return this.maxminusminCompare;
  }
  public void setMaxMinusMinCompare( boolean b ) {
    this.maxminusminCompare = b;
  }
  public boolean getMeanCompare() {
    return this.meanCompare;
  }
  public void setMeanCompare( boolean b ) {
    this.meanCompare = b;
  }
  public boolean getStddevCompare() {
    return this.stddevCompare;
  }
  public void setStddevCompare( boolean b ) {
    this.stddevCompare = b;
  }
  public String getTrimSpecName() {
    return this.trimspecName;
  }
  public void setTrimSpecName( String trimspecName ) {
    this.trimspecName = trimspecName;
  }

  /**
   * Returns a description of the statistic referred to by this
   * <code>StatSpec</code>.
   *
   * author David Whitlock
   */
  public String getDescription() {
    return this.description;
  }

  /**
   * Sets the description of the statistic  referred to by this
   * <code>StatSpec</code>.
   *
   * author David Whitlock
   */
  public void setDescription(String description) {
    this.description = description;
  }

  //////////////////////////////////////////////////////////////////////////// 
  ////    COMPARING                                                       ////
  //////////////////////////////////////////////////////////////////////////// 

  /** Used by PerfComparer to throw out stats that will not be compared */
  protected StatSpec distill() {
    // turn off operations that are not being compared
    if (!getMinCompare() && !getMinExpr()) setMin(false);
    if (!getMaxCompare() && !getMaxExpr()) setMax(false);
    if (!getMeanCompare() && !getMeanExpr()) setMean(false);
    if (!getMaxMinusMinCompare() && !getMaxMinusMinExpr()) setMaxMinusMin(false);
    if (!getStddevCompare() && !getStddevExpr()) setStddev(false);
    if (!getMinCompare() && !getMaxCompare() && !getMeanCompare() &&
        !getMaxMinusMinCompare() && !getStddevCompare() &&
        !getMinExpr() && !getMaxExpr() && !getMeanExpr() &&
        !getMaxMinusMinExpr() && !getStddevExpr()) {
        return null;
    }
    return this;
  }

  //////////////////////////////////////////////////////////////////////////// 
  ////    PRINTING                                                        ////
  //////////////////////////////////////////////////////////////////////////// 

  public String toString() {
    StringBuffer buf = new StringBuffer();
    buf.append( this.name + " " );

    if (PerfReporter.brief) {
      buf.append("\n  " + this.getDescription());

    } else {
      buf.append( this.id + "\n" );
      buf.append( StatSpecTokens.FILTER_TYPE + "=" + getFilterAsString() + " " );
      buf.append( StatSpecTokens.COMBINE_TYPE + "=" + getCombineAsString() + " " );
      String o;
      o = getOpsAsString();
      if ( o != null )
        buf.append( StatSpecTokens.OP_TYPES + "=" + o + " " );
      buf.append( StatSpecTokens.TRIM_SPEC_NAME + "=" + this.trimspecName );
    }

    return buf.toString();
  }
  public String toSpecString() {
    return StatSpecTokens.STATSPEC + " " + toString() + "\n;";
  }
  private int getFilterFromString( String filterType ) {
    if ( filterType.equalsIgnoreCase( StatSpecTokens.FILTER_PERSEC ) ) {
      return StatArchiveReader.StatValue.FILTER_PERSEC;
    } else if ( filterType.equalsIgnoreCase( StatSpecTokens.FILTER_NONE ) ) {
      return StatArchiveReader.StatValue.FILTER_NONE;
    } else {
      throw new PerfStatException( "Illegal filter type: " + filterType );
    }
  }
  private String getFilterAsString() {
    switch( this.filter ) {
      case StatArchiveReader.StatValue.FILTER_PERSEC: return StatSpecTokens.FILTER_PERSEC;
      case StatArchiveReader.StatValue.FILTER_NONE: return StatSpecTokens.FILTER_NONE;
      default: throw new StatConfigException( "Should not happen" );
    }
  }
  private int getCombineFromString( String combineType ) {
    if ( combineType.equalsIgnoreCase( StatSpecTokens.RAW ) ) {
      return NONE;
    } else if ( combineType.equalsIgnoreCase( StatSpecTokens.COMBINE ) ) {
      return FILE;
    } else if ( combineType.equalsIgnoreCase( StatSpecTokens.COMBINE_ACROSS_ARCHIVES ) ) {
      return GLOBAL;
    } else {
      throw new PerfStatException( "Illegal combine type: " + combineType );
    }
  }
  private String getCombineAsString() {
    switch( this.combine ) {
      case NONE: return StatSpecTokens.RAW;
      case FILE: return StatSpecTokens.COMBINE;
      case GLOBAL: return StatSpecTokens.COMBINE_ACROSS_ARCHIVES;
      default: throw new StatConfigException( "Should not happen" );
    }
  }
  String getOpsAsString() {
    List l = new ArrayList();
    if ( this.getMin() ) {
      if ( this.getMinCompare() ) {
        l.add( StatSpecTokens.MIN_C );
      } else if ( this.getMinExpr() ) {
        l.add( StatSpecTokens.MIN_E );
      } else {
        l.add( StatSpecTokens.MIN );
      }
    }
    if ( this.getMax() ) {
      if ( this.getMaxCompare() ) {
        l.add( StatSpecTokens.MAX_C );
      } else if ( this.getMaxExpr() ) {
        l.add( StatSpecTokens.MAX_E );
      } else {
        l.add( StatSpecTokens.MAX );
      }
    }
    if ( this.getMaxMinusMin() ) {
      if ( this.getMaxMinusMinCompare() ) {
        l.add( StatSpecTokens.MAXMINUSMIN_C );
      } else if ( this.getMaxMinusMinExpr() ) {
        l.add( StatSpecTokens.MAXMINUSMIN_E );
      } else {
        l.add( StatSpecTokens.MAXMINUSMIN );
      }
    }
    if ( this.getMean() ) {
      if ( this.getMeanCompare() ) {
        l.add( StatSpecTokens.MEAN_C );
      } else if ( this.getMeanExpr() ) {
        l.add( StatSpecTokens.MEAN_E );
      } else {
        l.add( StatSpecTokens.MEAN );
      }
    }
    if ( this.getStddev() ) {
      if ( this.getStddevCompare() ) {
        l.add( StatSpecTokens.STDDEV_C );
      } else if ( this.getStddevExpr() ) {
        l.add( StatSpecTokens.STDDEV_E );
      } else {
        l.add( StatSpecTokens.STDDEV );
      }
    }
    StringBuffer buf = new StringBuffer();
    if ( l.size() > 0 ) {
      int count = -1;
      for ( Iterator i = l.iterator(); i.hasNext(); ) {
	String op = (String) i.next();
	if ( ++count > 0 )
	  buf.append( "," );
	buf.append( op );
      }
      return buf.toString();
    }
    return null;
  }
}
