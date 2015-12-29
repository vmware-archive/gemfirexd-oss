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
package container.app.phonedirectory.remoting.rmi;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Scanner;

import container.app.lang.support.DestroyVisitor;
import container.app.phonedirectory.service.PhoneDirectoryService;
import container.app.phonedirectory.service.factory.PhoneDirectoryServiceFactory;
import container.app.remoting.rmi.AbstractRmiServiceExporter;
import container.app.util.StringUtils;
import container.app.util.Sys;

public class PhoneDirectoryServiceExporter extends AbstractRmiServiceExporter {

  public static final String SERVICE_NAME = "phoneDirectoryService";
  
  private volatile PhoneDirectoryService service;

  @Override
  protected int getRegistryPort() {
    return Integer.getInteger("gemfire.container-tests.rmi-registry.port", super.getRegistryPort());
  }

  @Override
  protected String getServiceNameForBinding() {
    return SERVICE_NAME;
  }

  @Override
  protected Remote exportRemoteObject() throws RemoteException {
    this.service = PhoneDirectoryServiceFactory.getPhoneDirectoryService();
    return UnicastRemoteObject.exportObject(this.service, 0);
  }

  protected void promptForUserInput() throws IOException {
    Sys.out("Enter 'exit' to stop the Phone Directory Service and 'show' to list available services!");

    final Scanner in = new Scanner(System.in);
    String input = null;

    do {
      input = StringUtils.trim(in.nextLine());
      if ("show".equalsIgnoreCase(input)) {
        showAvailableServices();
      }
      else {
        Sys.out("({0}) is not a valid command; Please enter 'exit' or 'show'.", input);
      }
    }
    while (!"exit".equalsIgnoreCase(input));
  }

  @Override
  public void stop() throws Exception {
    this.service.accept(new DestroyVisitor());
    this.service = null;
    super.stop();
  }

  /**
   * @param args
   */
  public static void main(final String[] args) throws Exception {
    PhoneDirectoryServiceExporter exporter = null;

    try {
      exporter = new PhoneDirectoryServiceExporter();
      exporter.run();
      exporter.promptForUserInput();
    }
    catch (Exception e) {
      Sys.err(e);
      //throw e;
    }
    finally {
      if (exporter != null) {
        Sys.out("Stopping the Phone Directory Service...");
        exporter.stop();
      }
    }
  }

}
