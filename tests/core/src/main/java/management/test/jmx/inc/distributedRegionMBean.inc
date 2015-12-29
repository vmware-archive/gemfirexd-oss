management.jmx.JMXPrms-mbeanSpec += "{
	'class' : 'com.gemstone.gemfire.management.DistributedRegionMXBean',
	'templateObjectName' : 'GemFire:service=Region,name=(.*?),type=Distributed',
	'type' : 'distributedRegion',	
	'tests' : [		
		'checkRegionStatistics',
		'checkRegionRuntimeAttributes',
		'checkRegionConfigAttributes'
	]	
}";