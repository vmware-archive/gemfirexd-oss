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

import java.util.Properties;

/**
 * Tests the functionality of the BenchmarkAckProducer/Consumer quickstart
 * example.
 *
 * @author Kirk Lund
 * @since 4.1.1
 */
public class BenchmarkAckTest extends QuickstartTestCase {

  protected ProcessWrapper consumer;
  protected ProcessWrapper producer;
    
  public BenchmarkAckTest(String name) {
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

  @Override
  protected Properties createProperties() {
    Properties properties = super.createProperties();
    properties.setProperty("benchmark.operations-per-sample", "5");
    return properties;
  }
  
  public void testBenchmarkAck() throws Exception {
    
    // start up BenchmarkAckConsumer
    getLogWriter().info("[testBenchmarkAck] start up BenchmarkAckConsumer");
    this.consumer = new ProcessWrapper(BenchmarkAckConsumer.class);
    this.consumer.execute(createProperties());
    
    this.consumer.waitForOutputToMatch("^Please start the BenchmarkAckProducer.*$");
    
    // start up BenchmarkAckProducer
    getLogWriter().info("[testBenchmarkAck] start up BenchmarkAckProducer");
    this.producer = new ProcessWrapper(BenchmarkAckProducer.class);
    this.producer.execute(createProperties());
    
    // complete producer            
    getLogWriter().info("[testBenchmarkAck] joining to BenchmarkAckProducer");
    this.producer.waitFor();
    printProcessOutput(this.producer, "PRODUCER");
    
    // complete consumer
    this.consumer.sendInput();
    getLogWriter().info("[testBenchmarkAck] joining to BenchmarkAckConsumer");
    this.consumer.waitFor();
    printProcessOutput(this.consumer, "CONSUMER");
    
    // validate output from producer
    assertOutputMatchesGoldenFile(this.producer.getOutput(), "BenchmarkAckProducer.txt");
    
    // validate output from consumer
    assertOutputMatchesGoldenFile(this.consumer.getOutput(), "BenchmarkAckConsumer.txt");
  }
}
