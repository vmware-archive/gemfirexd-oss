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

import java.util.*;

/**
 * Represents the results of comparing statistic values of a number
 * of test runs to a "base" test run.  An instance of this class
 * basically encodes a "table" of data.  Each row of the table
 * contains statistic values.
 */
public interface TestComparison {

  /** The name of the statistic specification for each data "row" */
  public List getStatSpecColumn();

  /** The name of the operation for each data "row" */
  public List getOpColumn();

  /** Data for the test directories compared */
  public List[] getValColumns();

  /** Printing */
  public String toString();
}
