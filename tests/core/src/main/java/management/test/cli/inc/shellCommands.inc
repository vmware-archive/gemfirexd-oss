management.cli.GfshPrms-commandSpec += "{
    'command': 'connect',
    'executor': 'management.operations.ops.cli.executors.ExportDataExecutor',
    'arguments' : [    	
    ],
    'modes' : [
    		{
    			'name' : 'database',
    		 	'mandatoryOptions': [ 'database-table', 'driver', 'url'],
    		 	'options': [
	    			{
	    				'name' : 'user',
	    				'alsoInclude' : ['password'],
	    				'probability' : 100	    				
			    	},
					{
						'name' : 'password'
					}
    		 	]
    		},
    		{
    			'name' : 'file',
    			'mandatoryOptions': [ 'file', 'format'],
    			'options': []
    		},
    		{
    			'name' : 'file2',
    			'mandatoryOptions': [ 'file2', 'format'],
    			'options': []
    		},
    		{
    			'name' : 'file3',
    			'mandatoryOptions': [ 'file3', 'format'],
    			'options': []
    		}
    ],    
    'mandatoryOptions': [],
    'options': [
    	{
    		'name' : 'region'
    		
    	}
    ]
}";