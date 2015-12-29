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
package cacheperf.comparisons.gemfirexd.useCase1.src.matcher;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cacheperf.comparisons.gemfirexd.useCase1.src.matcher.model.MatchingInfo;
import cacheperf.comparisons.gemfirexd.useCase1.src.matcher.model.MatchResultSet;
import com.pivotal.gemfirexd.procedure.ProcedureExecutionContext;

public interface MatchExecutor {

  MatchResultSet match(
      List<Object> prevResultList,
      Map<String,Map<String,String>> backOfficeMsg,
      Set<MatchingInfo> matchingKeySet,
      ProcedureExecutionContext pCtx
  ) throws SQLException;
}
