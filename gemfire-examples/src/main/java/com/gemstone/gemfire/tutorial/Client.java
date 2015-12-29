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
package com.gemstone.gemfire.tutorial;

import java.io.IOException;

import com.gemstone.gemfire.tutorial.storage.GemfireDAO;

/**
 * Main method that connects to the GemFire distributed system as a client and
 * launches a command line user interface for the social networking application.
 * 
 * @author GemStone Systems, Inc.
 */
public class Client {

  public static void main(String[] args) throws IOException {
    GemfireDAO dao = new GemfireDAO();
    dao.initClient();
    TextUI ui= new TextUI(dao, System.in, System.out);
    ui.processCommands();
  }

}
