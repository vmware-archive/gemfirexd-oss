management.jmx.JMXPrms-mbeanSpec += "{
	'class' : 'com.gemstone.gemfire.management.MemberMXBean',
	'templateObjectName' : 'GemFire:type=Member',
	'type' : 'member',	
	'tests' : [
		'createManager', 
		'listDiskStores', 
		'fetchLicense', 
		'fetchLog', 
		'checkGemfireConfig', 
		'checkOtherConfig', 
		'checkRegionCounters',
		'fetchJVMMetrics',
		'fetchOSMetrics',
		'processCommand',
		'checkRegionLatencyCounters',
		'checkRegionRateCounters',
		'checkWANResources',
		'checkGemfireRateCounters',
		'checkFunctionCounters',
		'checkGemfireTxCounters',
		'checkGemfireDiskCounters',
		'checkGemfireLockCounters',
		'checkGemfireLruCounters',
		'checkGIICounters',
		'checkGemfireSerializationCounters',
		'checkPlatformCounters'		
	]	
}"; 