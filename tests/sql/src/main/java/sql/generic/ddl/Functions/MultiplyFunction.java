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
package sql.generic.ddl.Functions;

public class MultiplyFunction extends FunctionTest {

  @Override
  protected String getFunctionDdl() {
    // TODO Auto-generated method stub
    return "create function trade.multiply(DP1 Decimal (30, 20) ) "
        + // comment out (30, 20)
        "RETURNS DECIMAL (30, 20) "
        + // comment out scale and precision (30, 20) to reproduce the bug
        "PARAMETER STYLE JAVA " + "LANGUAGE JAVA " + "NO SQL "
        + "EXTERNAL NAME 'sql.generic.ddl.Functions.FunctionBody.multiply'";
  }

  @Override
  protected String getFunctionName() {
    // TODO Auto-generated method stub
    return "trade.multiply";
  }

}
