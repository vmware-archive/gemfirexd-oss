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

package com.gemstone.gemfire.management.internal.cli.commands;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;
import com.gemstone.gemfire.management.internal.cli.util.MemberNotFoundException;

import org.springframework.shell.core.CommandMarker;

/**
 * The AbstractCommandsSupport class is an abstract base class encapsulating common functionality for implementing
 * command classes with command for the GemFire shell (gfsh).
 * </p>
 * @author John Blum
 * @see org.springframework.shell.core.CommandMarker
 * @since 7.0
 */
@SuppressWarnings("unused")
public abstract class AbstractCommandsSupport implements CommandMarker {

  protected static void assertArgument(final boolean legal, final String message, final Object... args) {
    if (!legal) {
      throw new IllegalArgumentException(String.format(message, args));
    }
  }

  protected static void assertNotNull(final Object obj, final String message, final Object... args) {
    if (obj == null) {
      throw new NullPointerException(String.format(message, args));
    }
  }

  protected static void assertState(final boolean valid, final String message, final Object... args) {
    if (!valid) {
      throw new IllegalStateException(String.format(message, args));
    }
  }

  protected static String toString(final Boolean condition, final String trueValue, final String falseValue) {
    return (Boolean.TRUE.equals(condition) ? StringUtils.defaultIfBlank(trueValue, "true")
      : StringUtils.defaultIfBlank(falseValue, "false"));
  }

  protected static String toString(final Throwable t, final boolean printStackTrace) {
    String message = t.getMessage();

    if (printStackTrace) {
      final StringWriter writer = new StringWriter();
      t.printStackTrace(new PrintWriter(writer));
      message = writer.toString();
    }

    return message;
  }

  protected boolean isConnectedAndReady() {
    return (getGfsh() != null && getGfsh().isConnectedAndReady());
  }

  protected boolean isDebugging() {
    return (getGfsh() != null && getGfsh().getDebug());
  }

  protected boolean isLogging() {
    return (getGfsh() != null);
  }

  protected Cache getCache() {
    return CacheFactory.getAnyInstance();
  }

  protected Gfsh getGfsh() {
    return Gfsh.getCurrentInstance();
  }

  protected DistributedMember getMember(final Cache cache, final String memberName) {
    for (final DistributedMember member : getMembers(cache)) {
      if (memberName.equalsIgnoreCase(member.getName()) || memberName.equalsIgnoreCase(member.getId())) {
        return member;
      }
    }

    throw new MemberNotFoundException(CliStrings.format(CliStrings.MEMBER_NOT_FOUND_ERROR_MESSAGE, memberName));
  }

  /**
   * Gets all members in the GemFire distributed system/cache.
   * </p>
   * @param cache the GemFire cache.
   * @return all members in the GemFire distributed system/cache.
   * @see com.gemstone.gemfire.management.internal.cli.CliUtil#getAllMembers(com.gemstone.gemfire.cache.Cache)
   * @deprecated use CliUtil.getAllMembers(com.gemstone.gemfire.cache.Cache) instead
   */
  @Deprecated
  protected Set<DistributedMember> getMembers(final Cache cache) {
    final Set<DistributedMember> members = new HashSet<DistributedMember>(cache.getMembers());
    members.add(cache.getDistributedSystem().getDistributedMember());
    return members;
  }

  protected Execution getMembersFunctionExecutor(final Set<DistributedMember> members) {
    return FunctionService.onMembers(members);
  }

  @SuppressWarnings("unchecked")
  protected <T extends Function> T register(T function) {
    if (FunctionService.isRegistered(function.getId())) {
      function = (T) FunctionService.getFunction(function.getId());
    }
    else {
      FunctionService.registerFunction(function);
    }

    return function;
  }

}
