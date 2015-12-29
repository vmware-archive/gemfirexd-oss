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
package quickstart;

/**
 * Tests the functionality of the PushProducer/Consumer quickstart
 * example.
 *
 * @author Kirk Lund
 * @since 4.1.1
 */
public class PushTest extends QuickstartTestCase {

  protected ProcessWrapper consumer;
  protected ProcessWrapper producer;
    
  public PushTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    if (this.consumer != null) this.consumer.destroy();
    if (this.producer != null) this.producer.destroy();
  }
  
  public void testPush() throws Exception {
    // start up PushConsumer
    getLogWriter().info("[testPush] start up PushConsumer");
    this.consumer = new ProcessWrapper(PushConsumer.class);
    this.consumer.execute(createProperties());
    this.consumer.waitForOutputToMatch("Please start the PushProducer\\.");
    
    // start up PushProducer
    getLogWriter().info("[testPush] start up PushProducer");
    this.producer = new ProcessWrapper(PushProducer.class);
    this.producer.execute(createProperties());
    this.producer.waitForOutputToMatch("Please press Enter in the PushConsumer\\.");

    // complete consumer
    this.consumer.sendInput();
    getLogWriter().info("[testPush] joining to PushConsumer");
    this.consumer.waitFor();
    printProcessOutput(this.consumer, "CONSUMER");
    
    // complete producer
    getLogWriter().info("[testPush] joining to PushProducer");
    this.producer.waitFor();
    printProcessOutput(this.producer, "PRODUCER");
    
    // validate output from producer
    assertOutputMatchesGoldenFile(this.producer.getOutput(), "PushProducer.txt");
    
    // validate output from consumer
    assertOutputMatchesGoldenFile(this.consumer.getOutput(), "PushConsumer.txt");
  }
}
