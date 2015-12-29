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
package com.gemstone.gemfire.pdx;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.util.Gateway;
import com.gemstone.gemfire.cache.util.GatewayEvent;
import com.gemstone.gemfire.cache.util.GatewayEventListener;
import com.gemstone.gemfire.cache.util.GatewayHub;
import com.gemstone.gemfire.cache.util.GatewayQueueAttributes;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.NullDataOutputStream;

import dunit.Host;
import dunit.SerializableRunnable;
import dunit.VM;

public class PdxGatewayEventListenerDUnitTest extends CacheTestCase {


  public PdxGatewayEventListenerDUnitTest(String name) {
    super(name);
  }
  
  public void testPdxTypesNotPassedToGateway() throws IOException {
    
    Properties props = new Properties();
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, "1");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    getSystem(props);
    Cache cache =getCache();
    GatewayHub hub = cache.addGatewayHub("Test Hub", -1);
    Gateway gateway = hub.addGateway("test gateway");
    gateway.setQueueAttributes(new GatewayQueueAttributes(".", 100, 1, 1, false, false, true, 15));
    final TestGatewayListener listener = new TestGatewayListener();
    gateway.addListener(listener);
    hub.start();
    gateway.start();
    Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).setEnableGateway(true)
        .setGatewayHubId("Test Hub")
    .create("region");
    
    DataSerializer.writeObject(new TestPdx("a"), new NullDataOutputStream());

    region.put(new TestPdx("b"), new TestPdx("c"));
    region.put(new TestPdx("a"), new TestPdx("d"));
    
    waitForCriterion(new WaitCriterion() {
      
      public boolean done() {
        return listener.seenEvents.size() >= 2;
      }
      
      public String description() {
        return "Waiting for " + listener.seenEvents.size() + " to be 2";
      }
    }, 1000, 50, true);
    
    List<GatewayEvent> events = listener.seenEvents;
    assertEquals(2, events.size());
    assertEquals(new TestPdx("b"), events.get(0).getKey());
    assertEquals(new TestPdx("c"), events.get(0).getDeserializedValue());
    assertEquals(new TestPdx("a"), events.get(1).getKey());
    assertEquals(new TestPdx("d"), events.get(1).getDeserializedValue());
  }
  
  public void xtestPdxDeserializedInRemoteListener() throws IOException {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    
    final int mcastPort = AvailablePortHelper.getRandomAvailableUDPPort();
    final Properties props = new Properties();
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, "1");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    props.setProperty("mcast-port", Integer.toString(mcastPort));
    getSystem(props);
    Cache cache =getCache();
    GatewayHub hub = cache.addGatewayHub("Test Hub", -1);
    Gateway gateway = hub.addGateway("test gateway");
    gateway.setQueueAttributes(new GatewayQueueAttributes(".", 100, 1, 1, false, false, true, 15));
    final TestGatewayListener listener = new TestGatewayListener();
    gateway.addListener(listener);
    hub.start();
    gateway.start();
    Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).setEnableGateway(true)
        .setGatewayHubId("Test Hub")
    .create("region");
    
    
    vm0.invoke(new SerializableRunnable() {
      
      public void run() {
        getSystem(props);
        Cache cache =getCache();
        Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).setEnableGateway(true)
            .setGatewayHubId("Test Hub")
        .create("region");
        region.put(new TestPdx("b"), new TestPdx("c"));
        region.put(new TestPdx("a"), new TestPdx("d"));
      }
    });
    
    waitForCriterion(new WaitCriterion() {
      
      public boolean done() {
        return listener.seenEvents.size() >= 2;
      }
      
      public String description() {
        return "Waiting for " + listener.seenEvents.size() + " to be 2";
      }
    }, 1000, 50, true);
    
    List<GatewayEvent> events = listener.seenEvents;
    assertEquals(2, events.size());
    assertEquals(new TestPdx("b"), events.get(0).getKey());
    assertEquals(new TestPdx("c"), events.get(0).getDeserializedValue());
    assertEquals(new TestPdx("a"), events.get(1).getKey());
    assertEquals(new TestPdx("d"), events.get(1).getDeserializedValue());
  }
  
  private static final class TestGatewayListener implements GatewayEventListener {
    private List<GatewayEvent> seenEvents = Collections.synchronizedList(new ArrayList<GatewayEvent>());
    
    public void close() {
      
    }

    public boolean processEvents(List<GatewayEvent> events) {
      seenEvents.addAll(events);
      return true;
    }
  }
  
  public static final class TestPdx  implements PdxSerializable {
    private String value;
    
    public TestPdx() {
      
    }
    
    public TestPdx(String value) {
      super();
      this.value = value;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((value == null) ? 0 : value.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      TestPdx other = (TestPdx) obj;
      if (value == null) {
        if (other.value != null)
          return false;
      } else if (!value.equals(other.value))
        return false;
      return true;
    }

    public void toData(PdxWriter writer) {
      writer.writeString("value", value);
      
    }

    public void fromData(PdxReader reader) {
      value = reader.readString("value");
      
    }
  }

}
