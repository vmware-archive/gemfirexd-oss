management.cli.GfshPrms-commandSpec += "{
    'command': 'create region',
    'executor': 'management.operations.ops.cli.executors.CreateRegionExecutor',
    'arguments' : [],
    'modes' : [		
		{
			'name' : 'from-other-region-override-disk',
			'mandatoryOptions': ['disk-store', 'enable-synchronous-disk', 'enable-statistics'],
			'options': []
		},
		{
			'name' : 'from-other-region-override-key-value-constraint',
			'mandatoryOptions': ['key-constraint', 'value-constraint'],
			'options': []
		},
		{
			'name' : 'from-other-region-override-expiration',
			'mandatoryOptions': ['entry-idle-time-expiration','entry-time-to-live-expiration', 'enable-statistics'],
			'options': []
		},
		{
			'name' : 'from-other-region-override-region-expiration',
			'mandatoryOptions': ['region-idle-time-expiration', 'region-time-to-live-expiration', 'enable-statistics'],
			'options': []
		},
		{
			'name' : 'from-other-region-override-conflation',
			'mandatoryOptions': ['enable-async-conflation' ,'enable-subscription-conflation'],
			'options': []
		},
		{
			'name' : 'from-other-region-override-listeners',
			'mandatoryOptions': ['cache-listener' ,'cache-loader', 'cache-writer'],
			'options': []
		},
		{
			'name' : 'from-other-region-override-concurrency-enabled',
			'mandatoryOptions': ['enable-concurrency-checks'],
			'options': []
		},
		{
			'name' : 'from-other-region-override-concurrency-level',
			'mandatoryOptions': ['concurrency-level'],
			'options': []
		},
		{
			'name' : 'from-other-region-override-cloning-enabled',
			'mandatoryOptions': ['enable-cloning'],
			'options': []
		},
		{
			'name' : 'from-other-override-pr',
			'mandatoryOptions': ['local-max-memory','recovery-delay','redundant-copies','startup-recovery-delay','total-max-memory','total-num-buckets'],
			'options': [],
			'arguments' : []		
		},
		{
			'name' : 'from-other-override-pr-colocated-with',
			'mandatoryOptions': ['colocated-with','local-max-memory','recovery-delay','redundant-copies','startup-recovery-delay','total-max-memory','total-num-buckets'],
			'options': [],
			'arguments' : []		
		}	
    ],
    'mandatoryOptions': ['template-region', 'name'],
    'options': []
}";