management.jmx.JMXPrms-mbeanSpec += "{
	'class' : 'com.gemstone.gemfire.management.DistributedLockServiceMXBean',
	'templateObjectName' : 'GemFire:service=LockService,name=(.*?),type=Distributed',
	'type' : 'distributedLockService',	
	'tests' : [
		'checkDLockConfig',
		'checkDLockCounters',
		'listThreadsHoldingLock',
		'listHeldLocks'
	]
}";