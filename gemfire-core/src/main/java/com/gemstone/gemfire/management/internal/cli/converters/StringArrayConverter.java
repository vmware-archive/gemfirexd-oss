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
package com.gemstone.gemfire.management.internal.cli.converters;

import java.util.List;

import org.springframework.shell.core.Completion;
import org.springframework.shell.core.MethodTarget;

import com.gemstone.gemfire.management.cli.ConverterHint;
import com.gemstone.gemfire.management.internal.cli.MultipleValueAdapter;

/**
 * @author Nikhil Jadhav
 *
 * @since 7.0
 * 
 * 
 */
public class StringArrayConverter extends MultipleValueAdapter<String[]>{

  @Override
  public String[] convertFromText(String[] value, Class<?> targetType,
      String context) {
    return value;
  }

  @Override
  public boolean getAllPossibleValues(List<Completion> completions,
      Class<?> targetType, String[] existingData, String context,
      MethodTarget target) {
    return false;
  }

  @Override
  public boolean supports(Class<?> type, String optionContext) {
    if (String[].class.isAssignableFrom(type)
        && !optionContext.equals(ConverterHint.DIRS)) {
      return true;
    } else {
      return false;
    }
  }

}
