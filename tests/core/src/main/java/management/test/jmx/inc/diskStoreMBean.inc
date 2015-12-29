management.jmx.JMXPrms-mbeanSpec += "{
	'class' : 'com.gemstone.gemfire.management.DiskStoreMXBean',
	'templateObjectName' : 'GemFire:service=DiskStore',
	'type' : 'diskStore',	
	'tests' : [
		'checkDiskConfig',
		'checkDiskCounters',
		'checkDiskStatistics',
		'forceRoll',
		'forceCompaction',
		'flush'
	]
}";