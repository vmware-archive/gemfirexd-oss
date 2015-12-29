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

import com.gemstone.gemfire.*;

import java.io.Serializable;
import java.util.*;
import java.util.regex.*;

/** 
 *
 * Holds the components of an individual statistics specification id.
 *
 */

public class StatSpecId implements Serializable {

  /** The logical name of the system where this stat is stored */
  private String systemName;

  /** The name of the statistics type */
  private String typeName;

  /** The name of the statistics instance */
  private String instanceName;

  /** The name of the statistic */
  private String statName;

  /** The system name pattern */
  private Pattern systemNamePattern;

  /** The instance name pattern */
  private Pattern instanceNamePattern;

  //////////////////////////////////////////////////////////////////////////// 
  ////    CONSTRUCTORS                                                    ////
  //////////////////////////////////////////////////////////////////////////// 

  public StatSpecId() {
  }
  public StatSpecId( PerformanceStatistics statInst, StatisticDescriptor statDesc ) {
    this.systemName = "*";
    this.typeName = statInst.statistics().getType().getName();
    this.instanceName = "*";
    this.statName = statDesc.getName();
  }

  //////////////////////////////////////////////////////////////////////////// 
  ////    ACCESSORS                                                       ////
  //////////////////////////////////////////////////////////////////////////// 

  public String getSystemName() {
    return this.systemName;
  }
  public void setSystemName( String systemName ) {
    this.systemName = systemName;
  }
  public String getTypeName() {
    return this.typeName;
  }
  public void setTypeName( String typeName ) {
    this.typeName = typeName;
  }
  public String getInstanceName() {
    return this.instanceName;
  }
  public void setInstanceName( String instanceName ) {
    this.instanceName = instanceName;
  }
  public String getStatName() {
    return this.statName;
  }
  public void setStatName( String statName ) {
    this.statName = statName;
  }

  //////////////////////////////////////////////////////////////////////////// 
  ////    PRINTING                                                        ////
  //////////////////////////////////////////////////////////////////////////// 

  public String toString() {
    return this.systemName + " " +
           this.typeName + " " +
           this.instanceName + " " +
           this.statName;
  }

  //////////////////////////////////////////////////////////////////////////// 
  ////    NAMING                                                          ////
  //////////////////////////////////////////////////////////////////////////// 

  protected Pattern getSystemPattern() {
    if ( this.systemNamePattern == null ) {
      this.systemNamePattern = patternFor( this.systemName );
    }
    return this.systemNamePattern;
  }
  protected Pattern getInstancePattern() {
    if ( this.instanceNamePattern == null ) {
      this.instanceNamePattern = patternFor( this.instanceName );
    }
    return this.instanceNamePattern;
  }
  private Pattern patternFor( String s ) {
    String result = new String();
    StringTokenizer tokenizer = new StringTokenizer( s, "*", true );
    while ( tokenizer.hasMoreTokens() ) {
      String token = tokenizer.nextToken();
      if ( token.equals( "*" ) ) {
        result += ".";
      }
      result += token;
    }
    return Pattern.compile( result );
  }
}
