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
package container.app.phonedirectory.osgi;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;

import container.app.phonedirectory.remoting.rmi.PhoneDirectoryServiceExporter;
import container.app.util.Assert;
import container.app.util.ComposableFilter;
import container.app.util.Filter;
import container.app.util.Sys;

public class PhoneDirectoryServiceActivator extends PhoneDirectoryServiceExporter implements BundleActivator {

  protected Object getBundleName(final BundleContext context) {
    Assert.notNull(context, "The bundle context was null!");
    return context.getBundle().getHeaders().get(Constants.BUNDLE_NAME);
  }

  @Override
  protected Filter<Object> getSystemPropertyFilter() {
    final SystemPropertyFilter filter = new SystemPropertyFilter();
    // Note, use 'null' in place of 'filter' to show all System properties.
    return ComposableFilter.composeAnd(filter, super.getSystemPropertyFilter());
  }

  public void start(final BundleContext context) throws Exception {
    Sys.out(getBundleName(context) + " is starting...");
    run();
    showAvailableServices();
    showSystemState(getSystemPropertyFilter());
  }

  public void stop(final BundleContext context) throws Exception {
    Sys.out(getBundleName(context) + " is stopping...");
    stop();
  }

}
