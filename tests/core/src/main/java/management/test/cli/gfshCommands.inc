management.cli.GfshPrms-commandSpec += "{
    'command': 'export data',
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

management.cli.GfshPrms-commandSpec += "{
    'command' : 'compact disk-store',
    'executor': 'management.operations.ops.cli.executors.DiskStoreExecutor',
    'arguments' : [ 'disk-store-name' ],
    'modes' : [
    	{
    		'name' : 'onlineMode',
    		'mandatoryOptions': ['online'],
    		'options': [
	    				{
	    					'name' : 'member-group',
	    					'probability' : 33
	    				}
	    	]
    	},
    	{
    		'name' : 'OfflineMode',
    		'mandatoryOptions': ['disk-dirs', 'max-oplog-size'],
    		'options' : []
    	}    	
    ],
    'mandatoryOptions': [],
    'options': []
}";

management.cli.GfshPrms-commandList = "export data" "compact disk-store" ;