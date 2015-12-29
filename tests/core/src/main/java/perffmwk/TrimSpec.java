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

import com.gemstone.gemfire.internal.LogWriterImpl;
import hydra.Log;
import java.io.*;
import java.text.*;
import java.util.*;

/**
 *  An instance of a trim specification.  It marks the start and end time
 *  for statistics associated with this trim specification.  Reports on
 *  these statistics are trimmed to this specification before being reported. 
 */

public class TrimSpec implements Serializable {

  public static String DEFAULT_TRIM_SPEC_NAME = "default";

  // 2003/09/25 17:21:43.701 PDT
  protected static final DateFormat formatter =
    new SimpleDateFormat(LogWriterImpl.FORMAT);

  /** Logical name of this trim spec. */
  private String name;

  /** Keeps the max start time for this logical trim spec. */
  private long start = -1;
  private String startStr = null;

  /** Keeps the min end time for this logical trim spec. */
  private long end = -1;
  private String endStr = null;

  private long minSamples = Long.MAX_VALUE;
  private long maxSamples = Long.MIN_VALUE;

  //////////////////////////////////////////////////////////////////////////////
  ////    CONSTRUCTORS                                                      ////
  //////////////////////////////////////////////////////////////////////////////

  public TrimSpec( String name ) {
    this.name = name;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    INSTANCE METHODS                                                  ////
  //////////////////////////////////////////////////////////////////////////////

  public String getName() {
    return this.name;
  }
  public long getStart() {
    return this.start;
  }
  public String getStartStr() {
    return this.startStr;
  }
  protected void setStart(String dateStr, Long timestamp) {
    this.start = (timestamp == null) ? getTimestamp(dateStr) : timestamp;
    this.startStr = dateStr;
  }
  public long getEnd() {
    return this.end;
  }
  public String getEndStr() {
    return this.endStr;
  }
  protected void setEnd(String dateStr, Long timestamp) {
    this.end = (timestamp == null) ? getTimestamp(dateStr) : timestamp;
    this.endStr = dateStr;
  }
  /**
   * Gets the timestamp from a date string.  This returns the wrong answer for
   * time zones such as IST and IDT, since the letters have multiple meanings
   * and Java interprets them in unpredictable ways (such usage is deprecated).
   * Logs a warning that mileage may vary.
   *
   * This method is used for backwards compatibility with trim specs generated
   * before timestamps were included.
   */
  private long getTimestamp(String dateStr) {
    Date d;
    try {
      d = formatter.parse(dateStr);
    } catch (ParseException e) {
      throw new StatConfigException("Invalid date format: " + dateStr);
    }
    long t = d.getTime();
    String s = "Converted " + dateStr + " to " + t
             + " using inaccurate and deprecated method DateFormat.parse.";
    Log.getLogWriter().warning(s);
    return t;
  }

  public synchronized void start() {
    start( System.currentTimeMillis() );
  }
  public synchronized void start( long startTime ) {
    // keep only the latest start time
    if ( this.start == -1 || this.start < startTime ) {
      this.start = startTime;
      this.startStr = formatter.format( new Date( this.start ) );
    }
    // complain if a start occurs after the end has been explicitly set
    if ( this.end != -1 && this.start > this.end ) {
      throw new PerfStatException( "Trim intervals do not overlap for " + this.name + ", start (" + this.startStr + ") was called after end (" + this.endStr + ")" );
    }
  }
  public synchronized void startExtended( long startTime ) {
    // keep only the earliest start time
    if ( this.start == -1 || this.start > startTime ) {
      this.start = startTime;
      this.startStr = formatter.format( new Date( this.start ) );
    }
  }
  public synchronized void end() {
    end( System.currentTimeMillis() );
  }
  public synchronized void end( long endTime ) {
    // keep only the earliest end time
    if ( this.end == -1 || this.end > endTime ) {
      this.end = endTime;
      this.endStr = formatter.format( new Date( this.end ) );
    }
  }
  public synchronized void endExtended( long endTime ) {
    // keep only the latest end time
    if ( this.end == -1 || this.end < endTime ) {
      this.end = endTime;
      this.endStr = formatter.format( new Date( this.end ) );
    }
    // complain if a start occurs after the end has been explicitly set
    if ( this.end != -1 && this.start > this.end ) {
      throw new PerfStatException( "Trim intervals do not overlap for " + this.name + ", start (" + this.startStr + ") was called after end (" + this.endStr + ")" );
    }
  }
  public void setSamples( int count ) {
    this.minSamples = Math.min( this.minSamples, count );
    this.maxSamples = Math.max( this.maxSamples, count );
  }
  public String toString() {
    StringBuffer buf = new StringBuffer();
    buf.append( this.name );
    if ( this.startStr != null )
      buf.append( " " + TrimSpecTokens.START + "=" ).append( this.startStr );
    if ( this.endStr != null )
      buf.append( " " + TrimSpecTokens.END + "=" ).append( this.endStr );
    return buf.toString();
  }
  public String toSpecString() {
    StringBuffer buf = new StringBuffer();
    buf.append(TrimSpecTokens.TRIMSPEC + " " + this.name);
    String indent = getIndent(buf.length()
          + TrimSpecTokens.START.length() - TrimSpecTokens.END.length());
    if (this.startStr != null) {
      buf.append(" ").append(TrimSpecTokens.START).append("=")
         .append(this.startStr).append(" (").append(this.start).append(")");
    }
    if (this.startStr != null && this.endStr != null) {
      buf.append("\n").append(indent);
    }
    if (this.endStr != null) {
      buf.append(" ").append(TrimSpecTokens.END).append("=")
         .append(this.endStr).append(" (").append(this.end).append(")");
    }
    return buf.append("\n;").toString();
  }
  private String getIndent(int indent) {
    char[] fill = new char[indent];
    Arrays.fill(fill, ' ');
    return String.valueOf(fill);
  }
  public String toReportString() {
    StringBuffer buf = new StringBuffer();
    buf.append( this.name );
    buf.append( " ==> " );
    buf.append( TrimSpecTokens.START + "=" + this.startStr + " " );
    buf.append( TrimSpecTokens.END + "=" + this.endStr + " ");
    if ( this.minSamples == Long.MAX_VALUE ) { // neither are set
      buf.append( "samples=unused" );
    } else { // both are set
      if ( this.minSamples == this.maxSamples ) {
	buf.append( "samples=" + this.minSamples );
      } else {
	buf.append( "samples=" + this.minSamples + "-" + this.maxSamples );
      }
    }
    return buf.toString();
  }
}
