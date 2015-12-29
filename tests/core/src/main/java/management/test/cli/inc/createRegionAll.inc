management.cli.GfshPrms-commandSpec += "{
    'command': 'create region',
    'executor': 'management.operations.ops.cli.executors.CreateRegionExecutor',
    'arguments' : ['region'],
    'modes' : [		
		{
			'name' : 'short-cut-all',
			'mandatoryOptions': [],
			'options': [],
			'arguments' : ['region-shortcut']
		}
    ],
    'mandatoryOptions': [],
    'options': []
}";

//		{
//			'name' : 'short-cut-override-disk',
//			'mandatoryOptions': ['disk-store', 'disk-synchronous', 'statistics-enabled'],
//			'options': [],
//			'arguments' : ['region-shortcut']
//		},
//		{
//			'name' : 'short-cut-all',
//			'mandatoryOptions': [],
//			'options': [],
//			'arguments' : ['region-shortcut']
//		},
//		{
//			'name' : 'short-cut-group',
//			'mandatoryOptions': ['group'],
//			'options': [],
//			'arguments' : ['region-shortcut']
//		},		
//		{
//			'name' : 'short-cut-all-skip-if-exists',
//			'mandatoryOptions': ['skip-if-exists'],
//			'options': [],
//			'arguments' : ['region-shortcut']
//		},
//		{
//			'name' : 'short-cut-group-skip-if-exists',
//			'mandatoryOptions': ['group', 'skip-if-exists'],
//			'options': [],
//			'arguments' : ['region-shortcut']
//		},
//		{
//			'name' : 'short-cut-override-disk',
//			'mandatoryOptions': ['disk-store', 'disk-synchronous', 'statistics-enabled'],
//			'options': [],
//			'arguments' : ['region-shortcut']
//		},
//		{
//			'name' : 'short-cut-override-key-value-constraint',
//			'mandatoryOptions': ['key-constraint', 'value-constraint'],
//			'options': [],
//			'arguments' : ['region-shortcut']
//		},
//		{
//			'name' : 'short-cut-override-expiration',
//			'mandatoryOptions': ['entry expiration','entry-expiration-idle-time','entry-expiration-time-to-live', 'statistics-enabled'],
//			'options': [],
//			'arguments' : ['region-shortcut']
//		},
//		{
//			'name' : 'short-cut-region-override-region-expiration',
//			'mandatoryOptions': ['region-expiration-time-to-live', 'region-expiration-idle-time', 'statistics-enabled'],
//			'options': [],
//			'arguments' : ['region-shortcut']
//		},
//		{
//			'name' : 'short-cut-override-conflation',
//			'mandatoryOptions': ['enable-async-conflation' ,'enable-subscription-conflation'],
//			'options': [],
//			'arguments' : ['region-shortcut']
//		},
//		{
//			'name' : 'short-cut-override-listeners',
//			'mandatoryOptions': ['cache-listener' ,'cache-loader', 'cache-writer'],
//			'options': [],
//			'arguments' : ['region-shortcut']
//		},
//		{
//			'name' : 'short-cut-override-concurrency-enabled',
//			'mandatoryOptions': ['concurrency-checks-enabled'],
//			'options': [],
//			'arguments' : ['region-shortcut']
//		},
//		{
//			'name' : 'short-cut-override-concurrency-level',
//			'mandatoryOptions': ['concurrency-level'],
//			'options': [],
//			'arguments' : ['region-shortcut']
//		},
//		{
//			'name' : 'short-cut-override-cloning-enabled',
//			'mandatoryOptions': ['cloning-enabled'],
//			'options': [],
//			'arguments' : ['region-shortcut']
//		},
//		{
//			'name' : 'from-other-region-all',
//			'mandatoryOptions': ['use-attributes-from'],
//			'options': []
//		},
//		{
//			'name' : 'from-other-region-group',
//			'mandatoryOptions': ['use-attributes-from', 'group'],
//			'options': []
//		},
//		{
//			'name' : 'from-other-region-skip-if-exists',
//			'mandatoryOptions': ['use-attributes-from', 'skip-if-exists'],
//			'options': []
//		},
//		{
//			'name' : 'from-other-region-group-skip-if-exists',
//			'mandatoryOptions': ['use-attributes-from', 'group', 'skip-if-exists'],
//			'options': []
//		},
//		{
//			'name' : 'from-other-region-override-disk',
//			'mandatoryOptions': ['use-attributes-from', 'disk-store', 'disk-synchronous', 'statistics-enabled'],
//			'options': []
//		},
//		{
//			'name' : 'from-other-region-override-key-value-constraint',
//			'mandatoryOptions': ['use-attributes-from', 'key-constraint', 'value-constraint'],
//			'options': []
//		},
//		{
//			'name' : 'from-other-region-override-expiration',
//			'mandatoryOptions': ['use-attributes-from', 'entry-expiration-idle-time','entry-expiration-time-to-live', 'statistics-enabled'],
//			'options': []
//		},
//		{
//			'name' : 'from-other-region-override-region-expiration',
//			'mandatoryOptions': ['use-attributes-from', 'region-expiration-time-to-live', 'region-expiration-idle-time', 'statistics-enabled'],
//			'options': []
//		},
//		{
//			'name' : 'from-other-region-override-conflation',
//			'mandatoryOptions': ['use-attributes-from', 'enable-async-conflation' ,'enable-subscription-conflation'],
//			'options': []
//		},
//		{
//			'name' : 'from-other-region-override-listeners',
//			'mandatoryOptions': ['use-attributes-from', 'cache-listener' ,'cache-loader', 'cache-writer'],
//			'options': []
//		},
//		{
//			'name' : 'from-other-region-override-concurrency-enabled',
//			'mandatoryOptions': ['use-attributes-from', 'concurrency-checks-enabled'],
//			'options': []
//		},
//		{
//			'name' : 'from-other-region-override-concurrency-level',
//			'mandatoryOptions': ['use-attributes-from', 'concurrency-level'],
//			'options': []
//		},
//		{
//			'name' : 'from-other-region-override-cloning-enabled',
//			'mandatoryOptions': ['use-attributes-from', 'cloning-enabled'],
//			'options': []
//		}	