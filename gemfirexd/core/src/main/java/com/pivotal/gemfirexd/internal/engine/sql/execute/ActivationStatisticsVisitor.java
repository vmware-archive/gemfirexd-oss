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
package com.pivotal.gemfirexd.internal.engine.sql.execute;

import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.impl.sql.execute.BaseActivation;
import com.pivotal.gemfirexd.internal.impl.sql.execute.ConstantActionActivation;
import com.pivotal.gemfirexd.internal.impl.sql.execute.ResultSetStatisticsVisitor;

/**
 * Accepted by the Activation classes to yield statistics descriptors.
 * 
 * @author soubhikc
 * @see ResultSetStatisticsVisitor
 * 
 */
public interface ActivationStatisticsVisitor extends GfxdStatisticsVisitor {

  public void visit(
      Activation ac,
      int donotHonor);

  public void visit(
      BaseActivation ac,
      int donotHonor);

  public void visit(
      AbstractGemFireActivation ac,
      int donotHonor);

  public void visit(
      AbstractGemFireDistributionActivation ac,
      int donotHonor);

  public void visit(
      GemFireSelectActivation ac);

  public void visit(
      GemFireUpdateActivation ac);

  public void visit(
      GemFireDeleteActivation ac);

  public void visit(
      GemFireSelectDistributionActivation ac);

  public void visit(
      GemFireUpdateDistributionActivation ac);

  public void visit(
      GemFireDeleteDistributionActivation ac);

  public void visit(
      ConstantActionActivation ac);

  public void reset();

}
