management.jmx.JMXPrms-mbeanSpec += "{
	'class' : 'com.gemstone.gemfire.management.LockServiceMXBean',
	'templateObjectName' : 'GemFire:service=LockService',
	'type' : 'lockService',	
	'tests' : [
		'checkDLockConfig',
		'checkDLockCounters',
		'checkDLockStatistics',
		'becomeLockGrantor',
		'listThreadsHoldingLock',
		'listHeldLocks',
		'createAndDestroyDLock',			
	]
}";