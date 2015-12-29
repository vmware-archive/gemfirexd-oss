management.cli.GfshPrms-commandSpec += "{
    'command': 'create region',
    'executor': 'management.operations.ops.cli.executors.CreateRegionExecutor',
    'arguments' : [],
    'modes' : [		
		{
			'name' : 'short-cut-override-disk',
			'mandatoryOptions': ['disk-store', 'enable-synchronous-disk', 'enable-statistics'],
			'options': [],
			'arguments' : []
		},
		{
			'name' : 'short-cut-override-key-value-constraint',
			'mandatoryOptions': ['key-constraint', 'value-constraint'],
			'options': [],
			'arguments' : []
		},
		{
			'name' : 'short-cut-override-expiration',
			'mandatoryOptions': ['entry-idle-time-expiration','entry-time-to-live-expiration', 'enable-statistics'],
			'options': [],
			'arguments' : []
		},
		{
			'name' : 'short-cut-region-override-region-expiration',
			'mandatoryOptions': ['region-idle-time-expiration', 'region-time-to-live-expiration', 'enable-statistics'],
			'options': [],
			'arguments' : []
		},
		{
			'name' : 'short-cut-override-conflation',
			'mandatoryOptions': ['enable-async-conflation' ,'enable-subscription-conflation'],
			'options': [],
			'arguments' : []
		},
		{
			'name' : 'short-cut-override-listeners',
			'mandatoryOptions': ['cache-listener' ,'cache-loader', 'cache-writer'],
			'options': [],
			'arguments' : []
		},
		{
			'name' : 'short-cut-override-concurrency-enabled',
			'mandatoryOptions': ['enable-concurrency-checks'],
			'options': [],
			'arguments' : []
		},
		{
			'name' : 'short-cut-override-concurrency-level',
			'mandatoryOptions': ['concurrency-level'],
			'options': [],
			'arguments' : []
		},
		{
			'name' : 'short-cut-override-cloning-enabled',
			'mandatoryOptions': ['enable-cloning'],
			'options': [],
			'arguments' : []
		},
		{
			'name' : 'short-cut-override-pr',
			'mandatoryOptions': ['local-max-memory','recovery-delay','redundant-copies','startup-recovery-delay','total-max-memory','total-num-buckets'],
			'options': [],
			'arguments' : []		
		},
		{
			'name' : 'short-cut-override-pr-colocated-with',
			'mandatoryOptions': ['colocated-with','local-max-memory','recovery-delay','redundant-copies','startup-recovery-delay','total-max-memory','total-num-buckets'],
			'options': [],
			'arguments' : []		
		}		
    ],
    'mandatoryOptions': ['name','type'],
    'options': []
}";

