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
package com.gemstone.gemfire.management.internal.cli;

import java.util.List;

import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;

/**
 * Extends {@link Converter} to add multiple value support
 * 
 * @author Nikhil Jadhav
 * 
 * @param <T>
 */
public interface MultipleValueConverter<T> extends Converter<T> {

  /**
   * Similar to {@link Converter#convertFromText(String, Class, String)} but
   * with support for multiple values
   * 
   * @param value
   * @param targetType
   * @param context
   * @return required Data 
   */
  T convertFromText(String[] value, Class<?> targetType, String context);

  /**
   * Similar to
   * {@link Converter#getAllPossibleValues(List, Class, String, String, MethodTarget)}
   * but with support for multiple values
   * 
   * @param completions
   * @param targetType
   * @param existingData
   * @param context
   * @param target
   * @return required Data
   */
  boolean getAllPossibleValues(List<Completion> completions,
      Class<?> targetType, String[] existingData, String context,
      MethodTarget target);
}
