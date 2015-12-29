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

import hydra.Log;
import java.text.*;
import java.util.*;

/**
 *
 *  Contains the value of a statistic.
 *
 */

public class PerfStatValue {

  private static final DecimalFormat format = new DecimalFormat("###.###");

  StatSpec statspec;
  TrimSpec trimspec;
  Double min, max, maxminusmin, mean, stddev;
  String productVersion;
  boolean isLargerBetter;
  Integer samples;
  SortedSet archives;

  public PerfStatValue( StatSpec statspec, TrimSpec trimspec ) {
    this.statspec = statspec;
    this.trimspec = trimspec;
  }
  public void setProductVersion( String s ) {
    this.productVersion = s;
  }
  public void setIsLargerBetter( boolean b ) {
    this.isLargerBetter = b;
  }
  public void setMin( double d ) {
    this.min = new Double( d );
  }
  public void setMax( double d ) {
    this.max = new Double( d );
  }
  public void setMaxMinusMin( double d ) {
    this.maxminusmin = new Double( d );
  }
  public void setMean( double d ) {
    this.mean = new Double( d );
  }
  public void setStddev( double d ) {
    this.stddev = new Double( d );
  }
  public void setSamples( int i ) {
    this.samples = new Integer( i );
    this.trimspec.setSamples( i );
  }
  public void setArchives( SortedSet s ) {
    this.archives = s;
  }

  public String getProductVersion() {
    return this.productVersion;
  }
  public boolean isLargerBetter() {
    return this.isLargerBetter;
  }
  public double getMin() {
    if ( this.min == null ) {
      throw new PerfStatException( "Min has not been specified in the statspec for this value" );
    }
    return this.min.doubleValue();
  }
  public double getMax() {
    if ( this.max == null ) {
      throw new PerfStatException( "Max has not been specified in the statspec for this value" );
    }
    return this.max.doubleValue();
  }
  public double getMaxMinusMin() {
    if ( this.maxminusmin == null ) {
      throw new PerfStatException( "MaxMinusMin has not been specified in the statspec for this value" );
    }
    return this.maxminusmin.doubleValue();
  }
  public double getMean() {
    if ( this.mean == null ) {
      throw new PerfStatException( "Mean has not been specified in the statspec for this value" );
    }
    return this.mean.doubleValue();
  }
  public double getStddev() {
    if ( this.stddev == null ) {
      throw new PerfStatException( "Stddev has not been specified in the statspec for this value" );
    }
    return this.stddev.doubleValue();
  }
  public int getSamples() {
    if ( this.samples == null ) {
      throw new PerfStatException( "There are no samples for this value" );
    }
    return this.samples.intValue();
  }

  boolean isFlatline() {
    return (this.min == null         || this.min.doubleValue() == 0) &&
           (this.max == null         || this.max.doubleValue() == 0) &&
           (this.maxminusmin == null || this.maxminusmin.doubleValue() == 0) &&
           (this.mean == null        || this.mean.doubleValue() == 0) &&
           (this.stddev == null      || this.stddev.doubleValue() == 0);
  }

  /**
   * Returns this PerfStatValue divided by the argument, field by field.
   */
  public PerfStatValue dividedBy(PerfStatValue dpsv) {
    PerfStatValue psv = new PerfStatValue(this.statspec, this.trimspec);
    if (this.isFlatline() && dpsv.isFlatline()) {
      return null;
    }

    psv.setArchives(this.archives);
    psv.setProductVersion(this.productVersion);
    psv.setIsLargerBetter(this.isLargerBetter); // go with the numerator here
    psv.setSamples(this.samples.intValue());

    if (this.min != null && dpsv.min != null) {
      psv.setMin(divide(this.getMin(), dpsv.getMin()));
    }
    if (this.max != null && dpsv.max != null) {
      psv.setMax(divide(this.getMax(), dpsv.getMax()));
    }
    if (this.maxminusmin != null && dpsv.maxminusmin != null) {
      psv.setMaxMinusMin(divide(this.getMaxMinusMin(), dpsv.getMaxMinusMin()));
    }
    if (this.mean != null && dpsv.mean != null) {
      psv.setMean(divide(this.getMean(), dpsv.getMean()));
    }
    if (this.stddev != null && dpsv.stddev != null) {
      psv.setStddev(divide(this.getStddev(), dpsv.getStddev()));
    }
    return psv;
  }

  private double divide(double numerator, double denominator) {
    double div;
    if (numerator == 0) {
      div = 0.0d;
    } else if (denominator == 0) {
      div = Double.MAX_VALUE;
    } else if (numerator == denominator) {
      div = 1.0d;
    } else {
      div = numerator / denominator;
    }
    if (Log.getLogWriter().finerEnabled()) {
      Log.getLogWriter().finer("divide(" + numerator + ", " + denominator
                              + ") ==> " + div);
    }
    return div;
  }

  public String toString() {
    StringBuffer buf = new StringBuffer();

    //buf.append( "\n" + this.statspec.getId() + "\n" );
    
    if ( this.min != null )
      buf.append( " min=" + format.format( this.min.doubleValue() ) );
    if ( this.max != null )
      buf.append( " max=" + format.format( this.max.doubleValue() ) );
    if ( this.maxminusmin != null )
      buf.append( " max-min=" + format.format( this.maxminusmin.doubleValue() ) );
    if ( this.mean != null )
      buf.append( " mean=" + format.format( this.mean.doubleValue() ) );
    if ( this.stddev != null )
      buf.append( " stddev=" + format.format( this.stddev.doubleValue() ) );

    if (!PerfReporter.brief) {
      //buf.append( " start=" + this.trimspec.getStartStr() );
      //buf.append( " end=" + this.trimspec.getEndStr() );
      buf.append( "\n   " );
      //buf.append( " isLargerBetter=" + this.isLargerBetter );
      buf.append( " samples=" + this.samples );
      buf.append( " archives=" + this.archives );
      //buf.append( " version=" + this.productVersion );
    }

    return buf.toString();
  }
}
