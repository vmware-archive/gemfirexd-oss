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
package com.gemstone.gemfire.security;

/**
 * this class contains test methods that used to be in its superclass but
 * that test started taking too long and caused dunit runs to hang
 */
public class ClientAuthenticationPart2DUnitTest extends
    ClientAuthenticationDUnitTest {

  /** constructor */
  public ClientAuthenticationPart2DUnitTest(String name) {
    super(name);
  }

  // override inherited tests so they aren't executed again
  
  @Override
  public void testValidCredentials() {  }
  @Override
  public void testNoCredentials() {  }
  @Override
  public void testInvalidCredentials() {  }
  @Override
  public void testInvalidAuthInit() {  }
  @Override
  public void testNoAuthInitWithCredentials() {  }
  @Override
  public void testInvalidAuthenticator() {  }
  @Override
  public void testNoAuthenticatorWithCredentials() {  }
  @Override
  public void testCredentialsWithFailover() {  }
  @Override
  public void testCredentialsForNotifications() {  }
  //@Override
  public void testValidCredentialsForMultipleUsers() {  }


  
  
  
  public void DISABLED_Bug48433_testNoCredentialsForMultipleUsers() {
    itestNoCredentials(Boolean.TRUE);
  }
  public void testInvalidCredentialsForMultipleUsers() {
    itestInvalidCredentials(Boolean.TRUE);
  }
  public void testInvalidAuthInitForMultipleUsers() {
    itestInvalidAuthInit(Boolean.TRUE);
  }
  public void testNoAuthInitWithCredentialsForMultipleUsers() {
    itestNoAuthInitWithCredentials(Boolean.TRUE);
  }
  public void testInvalidAuthenitcatorForMultipleUsers() {
    itestInvalidAuthenticator(Boolean.TRUE);
  }
  public void testNoAuthenticatorWithCredentialsForMultipleUsers() {
    itestNoAuthenticatorWithCredentials(Boolean.TRUE);
  }
  public void testCredentialsWithFailoverForMultipleUsers() {
    itestCredentialsWithFailover(Boolean.TRUE);
  }
  public void __testCredentialsForNotificationsForMultipleUsers() {
    itestCredentialsForNotifications(Boolean.TRUE);
  }

}
