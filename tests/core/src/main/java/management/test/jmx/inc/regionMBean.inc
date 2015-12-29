management.jmx.JMXPrms-mbeanSpec += "{
	'class' : 'com.gemstone.gemfire.management.RegionMXBean',
	'templateObjectName' : 'GemFire:service=Region',
	'type' : 'region',	
	'tests' : [
		'createRegion',
		'destroyRegion',
		'checkRegionStatistics',
		'checkRegionRuntimeAttributes',
		'checkRegionConfigAttributes'
	]	
}";