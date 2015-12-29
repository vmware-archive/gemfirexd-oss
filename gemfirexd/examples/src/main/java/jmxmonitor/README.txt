-- Overivew

This example illustrates how to use JMX APIs to retrieve and monitor GemFireXD statistics, and to raise notifications when certain conditions 
are met.

-- Design Overview

   The example is designed and implemented with a plug-in architecture. The classes that retrieve statistics and raise notifications are
   implemented as modules that can be plugged into the JMX monitor example using an XML configuration file. The example provides modules
   that demonstrate how to retrieve/monitor memory and transaction statistics from the GemFireXD ResourceManger and CachePerfStats MBeans. 
   
   You can use these modules as a basis for building your own plug-ins to monitor additional statistics.
   
-- Example Components

   1. examples.jmxmonitor Package: 
   	o JMXGemFireXDMonitor.java: A JMX monitor drive class that calls JMX APIs and GemFireXD MBeans (Agent, Distributed System).
   	o JMXGemFireXDMonitorException: An exception class used in this example. Contains no code related to JMX and GemFireXD MBeans.
   	o MonitorConfigHelper.java: A regular XML parser class for jmx-monitor.xml, Contains no code related to JMX and GemFireXD MBeans.
   	o jmx-monitor.xml: A configuration file for the JMX monitor and its module classes.
   	
   2. examples.jmxmonitor.jmxstats Package:
   	o GemFireXDStats.java: An interface that a customized stats class should implement.
   	o AbstractGemFireXDStats.java: An abstract class that implements GemFireXDStats.java with some common methods implemented by default.
   	o ResourceManagerStats.java: A plug-in module to retrieve and monitor the heap statistics from ResourceManagerStats MBean. 
   	o TransactionStats.java: A plug-in module to retrieve and monitor the transaction statistics from CachePerfStats MBean.
   	
   3. examples.jmxmonitor.notifier Package
	o StatsNotifier.java: An interface that a customized notifier class should implement.
   	o ConsoleStatsNotifer.java: A plug-in module to send the notifications using the console.
   	
-- Example Dependencies

   Apart from the JDK, the example code has no dependencies on third-party software or specific GemFireXD APIs.
   
   
-- Building the Example

   1. Change to the directory that contains the examples.
   
   2. Compile the example code using:
   
          javac examples/jmxmonitor/*.java examples/jmxmonitor/*/*.java
          
   3. Make sure that all Java files are compiled, especially those under examples/jmxmonitor/jmxstats/ and examples/jmxmonitor/notifier/.


-- Running the Example

   1. Follow the GemFireXD User Guide to start the JMX agent, which should automatically connect to GemFireXD distributed system. This can be achieved
      by specifying the agent property auto-connect=true. 
      
   2. In jmx-monitor.xml, configure the monitor settings (for example, the agent-host, agent-port, run mode and so forth). Also specify the statistics
      and notification module classes to use and their properties (for example threshold, history size and so forth)
      
   3. Run the monitor using command:
   
          java examples/jmxmonitor/JMXGemFireXDMonitor
          
      The monitor can run once, or run continuously, polling the stats at the poll-interval configured in the XML file.
      
   4. The retrieved statistics are shown similar to the following output, (The Warn message was the notification from ConsoleStatsNotifier.java):
   
	Getting statistic resources for GemFire.CacheVm:id=vmc-ssrc-rh98(20269)<v2>-8439/49266,type=CacheVm
	Warn: ============
	MEMBER vmc-ssrc-rh98(20269)<v2>-8439/49266:
		TxCommitLatency is 2.27381574E8, and exceeds the threshold 10.0ms
	==================
	[2012-03-21 16:50:20.490] Statistic resources for member vmc-ssrc-rh98(20269)<v2>-8439/49266:
				===TransactionStats===
				txCommits->3
				txRollbacks->4
				txSuccessLifeTime->682144722ms
				txRollbackLifeTime->2143929609ms

				===ResourceManagerStats===
				tenuredHeapUsed->26956728
				criticalThreshold->101213798
				heapCriticalEvents->0



