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

import java.io.Serializable;

/** 
 * Holds an individual statistics specification expression.
 */
public class Expr extends StatSpec implements Serializable {

  /** The numerator stat spec */
  private StatSpec numerator;

  /** The denominator stat spec */
  private StatSpec denominator;

  /** The stat config */
  protected StatConfig statconfig;

  //////////////////////////////////////////////////////////////////////////// 
  ////    CONSTRUCTORS                                                    ////
  //////////////////////////////////////////////////////////////////////////// 

  protected Expr(String name) {
    super(name);
  }

  //////////////////////////////////////////////////////////////////////////// 
  ////    ACCESSORS                                                       ////
  //////////////////////////////////////////////////////////////////////////// 

  public void setNumerator(StatSpec numerator) {
    this.numerator = numerator;
  }
  public void setDenominator(StatSpec denominator) {
    this.denominator = denominator;
  }
  public StatSpec getNumerator() {
    return this.numerator;
  }
  public StatSpec getDenominator() {
    return this.denominator;
  }
  public void setStatConfig( StatConfig statconfig ) {
    this.statconfig = statconfig;
  }

  //////////////////////////////////////////////////////////////////////////// 
  ////    PRINTING                                                        ////
  //////////////////////////////////////////////////////////////////////////// 

  public String toString() {
    StringBuffer buf = new StringBuffer();
    buf.append(this.name)
       .append(" ").append(StatSpecTokens.EQUALS)
       .append(" ").append(this.numerator.getName())
       .append(" ").append(StatSpecTokens.DIV)
       .append(" ").append(this.denominator.getName())
       .append(" ").append(StatSpecTokens.OP_TYPES + "=" + getOpsAsString());
    return buf.toString();
  }
  public String toSpecString() {
    return StatSpecTokens.EXPR + " " + toString() + "\n;";
  }
}
