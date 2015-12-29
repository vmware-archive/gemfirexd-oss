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
package container.app.phonedirectory.dao.factory;

import container.app.phonedirectory.dao.PhoneDirectoryDao;
import container.app.phonedirectory.dao.provider.CachingPhoneDirectoryDao;

public abstract class PhoneDirectoryDaoFactory {

  private static PhoneDirectoryDaoFactory daoFactory;

  public static synchronized PhoneDirectoryDaoFactory getInstance() {
    if (daoFactory == null) {
      daoFactory = new DefaultPhoneDirectoryDaoFactory();
    }

    return daoFactory;
  }

  public static PhoneDirectoryDao getPhoneDirectoryDao() {
    return getInstance().createPhoneDirectoryDao();
  }
  
  public static PhoneDirectoryDao getPhoneDirectoryDao(final boolean lazyInit) {
    return getInstance().createPhoneDirectoryDao(lazyInit);
  }

  public PhoneDirectoryDao createPhoneDirectoryDao() {
    return createPhoneDirectoryDao(false);
  }

  public abstract PhoneDirectoryDao createPhoneDirectoryDao(boolean lazyInit);

  protected static class DefaultPhoneDirectoryDaoFactory extends PhoneDirectoryDaoFactory {

    // Note, we could make the PhoneDirectoryDao bean a singleton as it is stateless, but we must be careful
    // to cleanup our strong references in the context of RMI so our objects/beans are properly garbage collected.
    @Override
    public PhoneDirectoryDao createPhoneDirectoryDao(final boolean lazyInit) {
      return new CachingPhoneDirectoryDao(lazyInit);
    }
  }

}
