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
package parReg.colocation;

import java.io.Serializable;

public class Month implements Serializable {

  private int month;
  
  private String quarter;

  Month(int month) {
    this.month = month;
    switch (month) {
      case 1:
      case 2:
      case 3:
        this.quarter = "Quarter1";
        break;
      case 4:
      case 5:
      case 6:
        this.quarter = "Quarter2";
        break;
      case 7:
      case 8:
      case 9:
        this.quarter = "Quarter3";
        break;
      case 10:
      case 11:
      case 12:
        this.quarter = "Quarter4";
        break;
    }
  }

  public String toString() {
    switch (month) {
      case 1:
        return "January";
      case 2:
        return "February";
      case 3:
        return "March";
      case 4:
        return "April";
      case 5:
        return "May";
      case 6:
        return "June";
      case 7:
        return "July";
      case 8:
        return "August";
      case 9:
        return "September";
      case 10:
        return "October";
      case 11:
        return "November";
      case 12:
        return "December";
      default:
        return "No month like this";
    }
  }

  public final static Month JAN = new Month(1), FEB = new Month(2),
      MAR = new Month(3), APR = new Month(4), MAY = new Month(5),
      JUN = new Month(6), JUL = new Month(7), AUG = new Month(8),
      SEP = new Month(9), OCT = new Month(10), NOV = new Month(11),
      DEC = new Month(12);

  public final static Month months[] = { JAN, FEB, MAR, APR, MAY, JUN, JUL,
      AUG, SEP, OCT, NOV, DEC };

  public int hashCode() {
    return month;
  }
  
  public String getQuarter() {
    return this.quarter;
  }
  
  public boolean equals(Object obj) {
    if (obj instanceof Month) {
      if (this.month == ((Month)obj).month) {
        return true;
      }
      else {
        return false;
      }
    }
    else {
      return false;
    }
  }

}
