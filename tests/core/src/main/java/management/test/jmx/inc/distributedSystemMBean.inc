management.jmx.JMXPrms-mbeanSpec += "{
	'class' : 'com.gemstone.gemfire.management.DistributedSystemMXBean',
	'templateObjectName' : 'GemFire:service=System,type=Distributed',
	'type' : 'distributedSystem',	
	'tests' : [
		'checkDSRuntime',
		'checkDSRuntimeMetrics',
		'checkStatistics',
		'checkDiskStoreStatistics',
		'backupAllMembers',
		'fetchMemberConfiguration',
		'fetchMemberUpTime',
		'fetchJVMMetrics',
		'fetchOSMetrics',
		'fetchDiskMetrics',
		'fetchNetworkMetric',
		'shutDownAllMembers',
		'revokeMissingDiskStores',
		'listMissingDiskStores',
	]	
}";