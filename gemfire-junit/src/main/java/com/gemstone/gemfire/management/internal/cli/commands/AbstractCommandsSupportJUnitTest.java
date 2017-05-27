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

import static org.junit.Assert.*;
import static org.junit.runners.MethodSorters.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.util.CollectionUtils;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.util.MemberNotFoundException;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;

/**
 * The AbstractCommandsSupportJUnitTest class is a test suite of test cases testing the contract and functionality
 * of the AbstractCommandsSupport class for implementing GemFire shell (Gfsh) commands.
 * </p>
 * @author John Blum
 * @see com.gemstone.gemfire.management.internal.cli.commands.AbstractCommandsSupport
 * @see org.jmock.Expectations
 * @see org.jmock.Mockery
 * @see org.jmock.lib.legacy.ClassImposteriser
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since 7.0
 */
@FixMethodOrder(NAME_ASCENDING)
public class AbstractCommandsSupportJUnitTest {

  private Mockery mockContext;

  @Before
  public void setup() {
    mockContext = new Mockery() {{
      setImposteriser(ClassImposteriser.INSTANCE);
    }};
  }

  @After
  public void tearDown() {
    mockContext.assertIsSatisfied();
    mockContext = null;
  }

  protected AbstractCommandsSupport createAbstractCommandsSupport(final Cache cache) {
    return new TestCommands(cache);
  }

  protected DistributedMember createMockMember(final String memberId, final String memberName) {
    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember " + memberId);

    mockContext.checking(new Expectations() {{
      allowing(mockMember).getName();
      will(returnValue(memberName));
      allowing(mockMember).getId();
      will(returnValue(memberId));
    }});

    return mockMember;
  }

  @Test
  public void test000AssertArgumentIsLegal() {
    AbstractCommandsSupport.assertArgument(true, "");
  }

  @Test(expected = IllegalArgumentException.class)
  public void test001AssertArgumentIsIllegal() {
    try {
      AbstractCommandsSupport.assertArgument(false, "The actual argument is %1$s!", "illegal");
    }
    catch (IllegalArgumentException expected) {
      assertEquals("The actual argument is illegal!", expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void test002AssetNotNullWithNonNullObject() {
    AbstractCommandsSupport.assertNotNull(new Object(), "");
  }

  @Test(expected = NullPointerException.class)
  public void test003AssertNotNullWithNullObject() {
    try {
      AbstractCommandsSupport.assertNotNull(null, "This is an %1$s message!", "expected");
    }
    catch (NullPointerException expected) {
      assertEquals("This is an expected message!", expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void test004AssertStateIsValid() {
    AbstractCommandsSupport.assertState(true, "");
  }

  @Test(expected = IllegalStateException.class)
  public void test005AssertStateIsInvalid() {
    try {
      AbstractCommandsSupport.assertState(false, "The actual state is %1$s!", "invalid");
    }
    catch (IllegalStateException expected) {
      assertEquals("The actual state is invalid!", expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void test007GetMemberWithMatchingMemberId() {
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");

    final DistributedSystem mockDistributedSystem = mockContext.mock(DistributedSystem.class, "DistributedSystem");

    final DistributedMember mockMemberSelf = createMockMember("S", "Self");
    final DistributedMember mockMemberOne = createMockMember("1", "One");
    final DistributedMember mockMemberTwo = createMockMember("2", "Two");

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getMembers();
      will(returnValue(CollectionUtils.asSet(mockMemberOne, mockMemberTwo)));
      oneOf(mockCache).getDistributedSystem();
      will(returnValue(mockDistributedSystem));
      oneOf(mockDistributedSystem).getDistributedMember();
      will(returnValue(mockMemberSelf));
    }});

    final AbstractCommandsSupport commands = createAbstractCommandsSupport(mockCache);

    assertSame(mockMemberTwo, commands.getMember(mockCache, "2"));
  }

  @Test
  public void test008GetMemberWithMatchingMemberName() {
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");

    final DistributedSystem mockDistributedSystem = mockContext.mock(DistributedSystem.class, "DistributedSystem");

    final DistributedMember mockMemberSelf = createMockMember("S", "Self");
    final DistributedMember mockMemberOne = createMockMember("1", "One");
    final DistributedMember mockMemberTwo = createMockMember("2", "Two");

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getMembers();
      will(returnValue(CollectionUtils.asSet(mockMemberOne, mockMemberTwo)));
      oneOf(mockCache).getDistributedSystem();
      will(returnValue(mockDistributedSystem));
      oneOf(mockDistributedSystem).getDistributedMember();
      will(returnValue(mockMemberSelf));
    }});

    final AbstractCommandsSupport commands = createAbstractCommandsSupport(mockCache);

    assertSame(mockMemberOne, commands.getMember(mockCache, "One"));
  }

  @Test
  public void test009GetMemberWithMatchingMemberNameCaseInsensitive() {
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");

    final DistributedSystem mockDistributedSystem = mockContext.mock(DistributedSystem.class, "DistributedSystem");

    final DistributedMember mockMemberSelf = createMockMember("S", "Self");
    final DistributedMember mockMemberOne = createMockMember("1", "One");
    final DistributedMember mockMemberTwo = createMockMember("2", "Two");

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getMembers();
      will(returnValue(CollectionUtils.asSet(mockMemberOne, mockMemberTwo)));
      oneOf(mockCache).getDistributedSystem();
      will(returnValue(mockDistributedSystem));
      oneOf(mockDistributedSystem).getDistributedMember();
      will(returnValue(mockMemberSelf));
    }});

    final AbstractCommandsSupport commands = createAbstractCommandsSupport(mockCache);

    assertSame(mockMemberSelf, commands.getMember(mockCache, "self"));
  }

  @Test(expected = MemberNotFoundException.class)
  public void test010GetMemberThrowsMemberNotFoundException() {
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");

    final DistributedSystem mockDistributedSystem = mockContext.mock(DistributedSystem.class, "DistributedSystem");

    final DistributedMember mockMemberSelf = createMockMember("S", "Self");
    final DistributedMember mockMemberOne = createMockMember("1", "One");
    final DistributedMember mockMemberTwo = createMockMember("2", "Two");

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getMembers();
      will(returnValue(CollectionUtils.asSet(mockMemberOne, mockMemberTwo)));
      oneOf(mockCache).getDistributedSystem();
      will(returnValue(mockDistributedSystem));
      oneOf(mockDistributedSystem).getDistributedMember();
      will(returnValue(mockMemberSelf));
    }});

    final AbstractCommandsSupport commands = createAbstractCommandsSupport(mockCache);

    try {
      commands.getMember(mockCache, "zero");
    }
    catch (MemberNotFoundException expected) {
      assertEquals(CliStrings.format(CliStrings.MEMBER_NOT_FOUND_ERROR_MESSAGE, "zero"), expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void test011GetMembers() {
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");

    final DistributedSystem mockDistributedSystem = mockContext.mock(DistributedSystem.class, "DistributedSystem");

    final DistributedMember mockMemberSelf = createMockMember("S", "Self");
    final DistributedMember mockMemberOne = createMockMember("1", "One");
    final DistributedMember mockMemberTwo = createMockMember("2", "Two");

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getMembers();
      will(returnValue(CollectionUtils.asSet(mockMemberOne, mockMemberTwo)));
      oneOf(mockCache).getDistributedSystem();
      will(returnValue(mockDistributedSystem));
      oneOf(mockDistributedSystem).getDistributedMember();
      will(returnValue(mockMemberSelf));
    }});

    final AbstractCommandsSupport commands = createAbstractCommandsSupport(mockCache);

    final Set<DistributedMember> expectedMembers = CollectionUtils.asSet(mockMemberOne, mockMemberTwo, mockMemberSelf);
    final Set<DistributedMember> actualMembers = commands.getMembers(mockCache);

    assertNotNull(actualMembers);
    assertEquals(expectedMembers.size(), actualMembers.size());
    assertTrue(actualMembers.containsAll(expectedMembers));
  }

  @Test
  public void test012GetMembersContainsOnlySelf() {
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");

    final DistributedSystem mockDistributedSystem = mockContext.mock(DistributedSystem.class, "DistributedSystem");
    final DistributedMember mockMemberSelf = createMockMember("S", "Self");

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getMembers();
      will(returnValue(Collections.emptySet()));
      oneOf(mockCache).getDistributedSystem();
      will(returnValue(mockDistributedSystem));
      oneOf(mockDistributedSystem).getDistributedMember();
      will(returnValue(mockMemberSelf));
    }});

    final AbstractCommandsSupport commands = createAbstractCommandsSupport(mockCache);

    final Set<DistributedMember> expectedMembers = CollectionUtils.asSet(mockMemberSelf);
    final Set<DistributedMember> actualMembers = commands.getMembers(mockCache);

    assertNotNull(actualMembers);
    assertEquals(expectedMembers.size(), actualMembers.size());
    assertTrue(actualMembers.containsAll(expectedMembers));
  }

  @Test
  public void test013Register() {
    final Function mockFunction = mockContext.mock(Function.class, "Function");

    mockContext.checking(new Expectations() {{
      exactly(3).of(mockFunction).getId();
      will(returnValue("mockId"));
      oneOf(mockFunction).isHA();
      will(returnValue(true));
      oneOf(mockFunction).hasResult();
      will(returnValue(true));
    }});

    final AbstractCommandsSupport commands = createAbstractCommandsSupport(mockContext.mock(Cache.class));

    assertFalse(FunctionService.isRegistered("mockId"));
    assertSame(mockFunction, commands.register(mockFunction));
    assertTrue(FunctionService.isRegistered("mockId"));
  }

  @Test
  public void test014RegisteredAlready() {
    final Function registeredFunction = mockContext.mock(Function.class, "Registered Function");
    final Function unregisteredFunction = mockContext.mock(Function.class, "Unregistered Function");

    mockContext.checking(new Expectations() {{
      exactly(2).of(registeredFunction).getId();
      will(returnValue("mockId"));
      oneOf(registeredFunction).isHA();
      will(returnValue(false));
      exactly(2).of(unregisteredFunction).getId();
      will(returnValue("mockId"));
    }});

    final AbstractCommandsSupport commands = createAbstractCommandsSupport(mockContext.mock(Cache.class));

    FunctionService.registerFunction(registeredFunction);

    assertTrue(FunctionService.isRegistered("mockId"));
    assertSame(registeredFunction, commands.register(unregisteredFunction));
    assertTrue(FunctionService.isRegistered("mockId"));
  }

  @Test
  public void test015ToStringOnBoolean() {
    assertEquals("false", AbstractCommandsSupport.toString(null, null, null));
    assertEquals("true", AbstractCommandsSupport.toString(true, null, null));
    assertEquals("true", AbstractCommandsSupport.toString(Boolean.TRUE, null, null));
    assertEquals("false", AbstractCommandsSupport.toString(false, null, null));
    assertEquals("false", AbstractCommandsSupport.toString(Boolean.FALSE, null, null));
    assertEquals("false", AbstractCommandsSupport.toString(true, "false", "true"));
    assertEquals("true", AbstractCommandsSupport.toString(false, "false", "true"));
    assertEquals("Yes", AbstractCommandsSupport.toString(true, "Yes", "No"));
    assertEquals("Yes", AbstractCommandsSupport.toString(false, "No", "Yes"));
    assertEquals("TRUE", AbstractCommandsSupport.toString(Boolean.TRUE, "TRUE", "FALSE"));
    assertEquals("FALSE", AbstractCommandsSupport.toString(Boolean.FALSE, "TRUE", "FALSE"));
  }

  @Test
  public void test016ToStringOnThrowable() {
    assertEquals("test", AbstractCommandsSupport.toString(new Throwable("test"), false));
  }

  @Test
  public void test017ToStringOnThrowablePrintingStackTrace() {
    final StringWriter writer = new StringWriter();
    final Throwable t = new Throwable("test");

    t.printStackTrace(new PrintWriter(writer));

    assertEquals(writer.toString(), AbstractCommandsSupport.toString(t, true));
  }

  protected static class TestCommands extends AbstractCommandsSupport {

    private final Cache cache;

    protected TestCommands(final Cache cache) {
      assert cache != null : "The Cache cannot be null!";
      this.cache = cache;
    }

    @Override
    protected Cache getCache() {
      return this.cache;
    }
  }

}
