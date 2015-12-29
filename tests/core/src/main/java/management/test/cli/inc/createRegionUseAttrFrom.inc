management.cli.GfshPrms-commandSpec += "{
    'command': 'create region',
    'executor': 'management.operations.ops.cli.executors.CreateRegionExecutor',
    'arguments' : [],    
    'modes' : [		
		{
			'name' : 'from-other-region-all',
			'mandatoryOptions': [],
			'options': []
		},
		{
			'name' : 'from-other-region-group',
			'mandatoryOptions': ['group'],
			'options': []
		},
		{
			'name' : 'from-other-region-skip-if-exists',
			'mandatoryOptions': ['skip-if-exists'],
			'options': []
		},
		{
			'name' : 'from-other-region-group-skip-if-exists',
			'mandatoryOptions': ['group', 'skip-if-exists'],
			'options': []
		}				  
    ],
    'mandatoryOptions': ['template-region', 'name'],
    'options': []
}";



// TODO : Difficult to validate following combinations
//		{
//			'name' : 'from-other-region-skip-if-exists',
//			'mandatoryOptions': ['use-attributes-from', 'skip-if-exists'],
//			'options': []
//		},
//		{
//			'name' : 'from-other-region-group-skip-if-exists',
//			'mandatoryOptions': ['use-attributes-from', 'group', 'skip-if-exists'],
//			'options': []
//		}
//