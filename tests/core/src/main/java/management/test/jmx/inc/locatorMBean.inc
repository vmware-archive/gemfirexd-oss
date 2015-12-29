management.jmx.JMXPrms-mbeanSpec += "{
	'class' : 'com.gemstone.gemfire.management.LocatorMXBean',
	'templateObjectName' : 'GemFire:service=Locator',
	'type' : 'locator',	
	'tests' : [
		'checkLocatorConfig',
		'checkLocatorCounters',
		'startAndStopLocator'
	]
}";