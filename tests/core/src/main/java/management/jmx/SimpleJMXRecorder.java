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
package management.jmx;

import java.util.ArrayList;
import java.util.List;

import javax.management.ObjectName;

public class SimpleJMXRecorder implements JMXEventRecorder {

  private List<JMXEvent> eventList = new ArrayList<JMXEvent>();

  @Override
  public synchronized void addEvent(JMXEvent e) {
    eventList.add(e);
  }

  public void addJmxOp(ObjectName name, String op, Object returnValue) {
    JMXEvent e = new JMXOperation(name, op, returnValue, null, null);
    this.addEvent(e);
  }

  @Override
  public List<JMXEvent> getRecordedEvents() {
    return eventList;
  }

}
