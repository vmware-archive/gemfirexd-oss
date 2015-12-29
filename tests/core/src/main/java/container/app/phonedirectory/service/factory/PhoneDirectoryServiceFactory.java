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
package container.app.phonedirectory.service.factory;

import container.app.phonedirectory.service.PhoneDirectoryService;
import container.app.phonedirectory.service.provider.PhoneDirectoryServiceImpl;

public abstract class PhoneDirectoryServiceFactory {

  private static PhoneDirectoryServiceFactory serviceFactory;

  public static synchronized PhoneDirectoryServiceFactory getInstance() {
    if (serviceFactory == null) {
      serviceFactory = new DefaultPhoneDirectoryServiceFactory();
    }

    return serviceFactory;
  }

  public static PhoneDirectoryService getPhoneDirectoryService() {
    return getInstance().createPhoneDirectoryService();
  }

  public abstract PhoneDirectoryService createPhoneDirectoryService();

  protected static class DefaultPhoneDirectoryServiceFactory extends PhoneDirectoryServiceFactory {

    // Note, we could make the PhoneDirectoryService bean a singleton as it is stateless, but we must be careful
    // to cleanup our strong references in the context of RMI so our objects/beans are properly garbage collected.
    @Override
    public PhoneDirectoryService createPhoneDirectoryService() {
      return new PhoneDirectoryServiceImpl();
    }
  }

}
