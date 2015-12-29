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
/**
 * 
 */
package gfxdperf.tpch;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author lynng
 *
 */
public interface ResultRow {
  
  /** Initialize this instance of ResultRow with the values of the current
   *  row of the ResultSet.
   * 
   * @param rs The ResultSet, which points to a current row.
   * @throws SQLException Thrown if any exceptions are encountered extracting
   *                      the values from the current row.
   */
  public void init(ResultSet rs) throws SQLException;
  
  /** Returns a human readable String representation of the values and column names
   *  of this ResultRow.
   */
  public String toString();
  
  /** Returns a String containing the String representation of the values of the row
   *  separated by "|". This is in the same format as used by the answer files for TPC-H.
   */
  public String toAnswerLineFormat();

}
