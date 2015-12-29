management.cli.GfshPrms-commandSpec += "{
    'command': 'list gateways',
    'executor': 'management.operations.ops.cli.executors.wan.ListGWExecutor',
    'arguments' : [],
    'modes' : [
		{
			'name' : 'group',
			'mandatoryOptions': [ 'group'],
			'options': []
		},
		{
			'name' : 'all',
			'mandatoryOptions': [],
			'options': []
		}
    ],
    'mandatoryOptions': [],
    'options': [    	
    ]
}";

management.cli.GfshPrms-commandSpec += "{
    'command': 'start gateway-sender',
    'executor': 'management.operations.ops.cli.executors.wan.StartGWSenderExecutor',
    'arguments' : [],
    'modes' : [
		{
			'name' : 'member',
			'mandatoryOptions': [ 'member', 'id'],
			'options': []
		},
		{
			'name' : 'group',
			'mandatoryOptions': [ 'group', 'id'],
			'options': []
		},
		{
			'name' : 'all',
			'mandatoryOptions': ['id'],
			'options': []
		}
    ],
    'mandatoryOptions': [],
    'options': [
    ]
}";


management.cli.GfshPrms-commandSpec += "{
    'command': 'stop gateway-sender',
    'executor': 'management.operations.ops.cli.executors.wan.StopGWSenderExecutor',
    'arguments' : [],
    'modes' : [
		{
			'name' : 'member',
			'mandatoryOptions': [ 'member' , 'id'],
			'options': []
		},
		{
			'name' : 'group',
			'mandatoryOptions': [ 'group' , 'id'],
			'options': []
		},
		{
			'name' : 'all',
			'mandatoryOptions': ['id'],
			'options': []
		}    
    ],
    'mandatoryOptions': [],
    'options': [
    ]
}";

management.cli.GfshPrms-commandSpec += "{
    'command': 'pause gateway-sender',
    'executor': 'management.operations.ops.cli.executors.wan.PauseGWSenderExecutor',
    'arguments' : [],
    'modes' : [
		{
			'name' : 'member',
			'mandatoryOptions': [ 'member' , 'id'],
			'options': []
		},
		{
			'name' : 'group',
			'mandatoryOptions': [ 'group' , 'id'],
			'options': []
		},
		{
			'name' : 'all',
			'mandatoryOptions': ['id'],
			'options': []
		}    
    ],
    'mandatoryOptions': [],
    'options': [
    ]
}";

management.cli.GfshPrms-commandSpec += "{
    'command': 'resume gateway-sender',
    'executor': 'management.operations.ops.cli.executors.wan.ResumeGWSenderExecutor',
    'arguments' : [],
    'modes' : [
		{
			'name' : 'member',
			'mandatoryOptions': [ 'member' , 'id'],
			'options': []
		},
		{
			'name' : 'group',
			'mandatoryOptions': [ 'group' , 'id'],
			'options': []
		},
		{
			'name' : 'all',
			'mandatoryOptions': ['id'],
			'options': []
		}    
    ],
    'mandatoryOptions': [],
    'options': [
    ]
}";

management.cli.GfshPrms-commandSpec += "{
    'command': 'status gateway-sender',
    'executor': 'management.operations.ops.cli.executors.wan.StatusGWSenderExecutor',
    'arguments' : [],
    'modes' : [
		{
			'name' : 'member',
			'mandatoryOptions': [ 'member' , 'id'],
			'options': []
		},
		{
			'name' : 'group',
			'mandatoryOptions': [ 'group' , 'id'],
			'options': []
		},
		{
			'name' : 'all',
			'mandatoryOptions': ['id'],
			'options': []
		}  
    ],
    'mandatoryOptions': [],
    'options': [
    ]
}";

management.cli.GfshPrms-commandSpec += "{
    'command': 'start gateway-receiver',
    'executor': 'management.operations.ops.cli.executors.wan.StartGWReceiverExecutor',
    'arguments' : [],
    'modes' : [
		{
			'name' : 'member',
			'mandatoryOptions': [ 'member'],
			'options': []
		},
		{
			'name' : 'group',
			'mandatoryOptions': [ 'group'],
			'options': []
		},
		{
			'name' : 'all',
			'mandatoryOptions': [],
			'options': []
		}    
    ],
    'mandatoryOptions': [],
    'options': [
    ]
}";

management.cli.GfshPrms-commandSpec += "{
    'command': 'status gateway-receiver',
    'executor': 'management.operations.ops.cli.executors.wan.StatusGWReceiverExecutor',
    'arguments' : [],
    'modes' : [
		{
			'name' : 'member',
			'mandatoryOptions': [ 'member'],
			'options': []
		},
		{
			'name' : 'group',
			'mandatoryOptions': [ 'group'],
			'options': []
		},
		{
			'name' : 'all',
			'mandatoryOptions': [],
			'options': []
		}   
    ],
    'mandatoryOptions': [],
    'options': [
    ]
}";

