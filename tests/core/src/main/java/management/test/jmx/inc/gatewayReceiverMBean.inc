management.jmx.JMXPrms-mbeanSpec += "{
	'class' : 'com.gemstone.gemfire.management.GatewayReceiverMXBean',
	'templateObjectName' : 'GemFire:service=GatewayReceiver',
	'type' : 'gatewayReceiver',	
	'tests' : [
		'checkReceiverConfig',
		'checkReceiverStatistics',
		'startStopReceiver'
	]
}"; 