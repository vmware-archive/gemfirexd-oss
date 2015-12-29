management.jmx.JMXPrms-mbeanSpec += "{
	'class' : 'com.gemstone.gemfire.management.GatewaySenderMXBean',
	'templateObjectName' : 'GemFire:service=GatewaySender',
	'type' : 'gatewaySender',	
	'tests' : [
		'checkSenderConfig',
		'checkSenderRuntime',
		'checkSenderStatistics',
		'startStopSender',
		'pauseResumeSender'		
	]
}"; 