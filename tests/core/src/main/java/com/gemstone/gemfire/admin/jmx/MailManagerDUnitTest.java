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
package com.gemstone.gemfire.admin.jmx;

import com.gemstone.gemfire.admin.AdminDUnitTestCase;
import com.gemstone.gemfire.admin.jmx.internal.MailManager;
import com.gemstone.gemfire.cache.Cache;

/**
 * tesing functionality of mail manager
 * 
 * @author mjha
 * 
 */
public class MailManagerDUnitTest extends AdminDUnitTestCase {
  public static final long REFRESH_INTERVAL = 10000;

  public static final int MEM_VM = 0;

  protected static Cache cache = null;

  /**
   * The constructor.
   * 
   * @param name
   */
  public MailManagerDUnitTest(String name) {
    super(name);
  }

  /**
   * Testing the singleton behavior of stat alert manager
   * 
   * @throws Exception
   */
  public void testMailManager() throws Exception {
    MailManager mailManager = new MailManager("mailsrv1.gemstone.com",
        "temp@gemstone.com");

    assertEquals(" 'mail from' id of mail manager is not correct",
        "temp@gemstone.com", mailManager.getMailFromAddress());
    assertEquals(" 'mail host' of mail manager is not correct",
        "mailsrv1.gemstone.com", mailManager.getMailHost());

    mailManager.setMailHost("mailsrv2.gemstone.com");
    assertEquals(" 'mail host' of mail manager is not correct",
        "mailsrv2.gemstone.com", mailManager.getMailHost());

    mailManager.setMailFromAddress("temp1@gemstone.com");
    assertEquals(" 'mail from' of mail manager is not correct",
        "temp1@gemstone.com", mailManager.getMailFromAddress());

    mailManager.addMailToAddress("xyz@gemstone.com");
    mailManager.addMailToAddress("xyz1@gemstone.com");
    mailManager.addMailToAddress("xyz2@gemstone.com");
    assertEquals("No of 'To list' of mail manager is supposed to be 3 ", 3,
        mailManager.getAllToAddresses().length);

    mailManager.removeMailToAddress("xyz1@gemstone.com");
    mailManager.removeMailToAddress("xyz2@gemstone.com");

    assertEquals(" 'mail from' of mail manager is not correct",
        "xyz@gemstone.com", mailManager.getAllToAddresses()[0]);
    assertEquals("No of 'To list' of mail manager is supposed to be 1 ", 1,
        mailManager.getAllToAddresses().length);

    mailManager.addMailToAddress("xyz1@gemstone.com");
    mailManager.addMailToAddress("xyz1@gemstone.com");
    assertEquals("No of 'To list' of mail manager is supposed to be 2 ", 2,
        mailManager.getAllToAddresses().length);

    mailManager.addMailToAddress("xyz2@gemstone.com");

    mailManager.removeAllMailToAddresses();
    assertEquals("No of 'To list' of mail manager is supposed to be 0 ", 0,
        mailManager.getAllToAddresses().length);

    assertNotNull("to string method should not return null ", mailManager
        .toString());
  }

}
