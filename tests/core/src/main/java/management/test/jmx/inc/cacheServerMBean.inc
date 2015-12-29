management.jmx.JMXPrms-mbeanSpec += "{
	'class' : 'com.gemstone.gemfire.management.CacheServerMXBean',
	'templateObjectName' : 'GemFire:service=CacheServer',
	'type' : 'cacheServer',	
	'tests' : [
		'startAndStopCacheServerWtihConfigCheck',
		'checkCacheServerStatistics',
		'checkCacheServerRuntimeData',
		'checkIndexAttributes',
		'checkCQAttributes',
		'checkClientAttributes',
		'fetchClientStats',
		'fetchClientIds',
		'closeContinuousQuery',
		'closeAllContinuousQuery',
		'stopContinuousQuery',
		'executeContinuousQuery',
		'removeIndex',
		'doClientHA'
	]	
}";