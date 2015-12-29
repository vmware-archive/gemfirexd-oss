management.cli.GfshPrms-commandSpec += "{
    'command': 'create region',
    'executor': 'management.operations.ops.cli.executors.CreateRegionExecutor',
    'arguments' : [],
    'modes' : [		
		{
			'name' : 'short-cut-all',
			'mandatoryOptions': [],
			'options': [],
			'arguments' : []
		},
		{
			'name' : 'short-cut-group',
			'mandatoryOptions': ['group'],
			'options': [],
			'arguments' : []
		},		
		{
			'name' : 'short-cut-all-skip-if-exists',
			'mandatoryOptions': ['skip-if-exists'],
			'options': [],
			'arguments' : []
		},
		{
			'name' : 'short-cut-group-skip-if-exists',
			'mandatoryOptions': ['group', 'skip-if-exists'],
			'options': [],
			'arguments' : []
		}
    ],
    'mandatoryOptions': ['name', 'type'],
    'options': []
}";