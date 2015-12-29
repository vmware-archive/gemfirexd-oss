management.cli.GfshPrms-commandSpec += "{
    'command': 'netstat',
    'executor': 'management.operations.ops.cli.executors.NetstatExecutor',
    'arguments' : [],
    'modes' : [
	    {
			'name' : 'member-lsof-file',
			'mandatoryOptions': [ 'member', 'with-lsof', 'file'],
			'options': []
		},
		{
			'name' : 'member-lsof-console',
			'mandatoryOptions': [ 'member', 'with-lsof'],
			'options': []
		},
		{
			'name' : 'member-no-lsof-file',
			'mandatoryOptions': [ 'member', 'file'],
			'options': []
		},
		{
			'name' : 'member-no-lsof-console',
			'mandatoryOptions': [ 'member'],
			'options': []
		},
		{
			'name' : 'group-lsof-file',
			'mandatoryOptions': [ 'group', 'with-lsof', 'file'],
			'options': []
		},
		{
			'name' : 'group-lsof-console',
			'mandatoryOptions': [ 'group', 'with-lsof'],
			'options': []
		},
		{
			'name' : 'group-no-lsof-file',
			'mandatoryOptions': [ 'group', 'file'],
			'options': []
		},
		{
			'name' : 'group-no-lsof-console',
			'mandatoryOptions': [ 'group'],
			'options': []
		}
    ],    
    'mandatoryOptions': [],
    'options': []
}";


//management.cli.GfshPrms-commandSpec += "{
//    'command': 'netstat',
//    'executor': 'management.operations.ops.cli.executors.NetstatExecutor',
//    'arguments' : [],
//    'modes' : [
//	    {
//			'name' : 'member-lsof-file',
//			'mandatoryOptions': [ 'with-lsof', 'file'],
//			'arguments' : ['member'],
//			'options': []
//		},
//		{
//			'name' : 'member-lsof-console',
//			'mandatoryOptions': [ 'with-lsof'],
//			'arguments' : ['member'],
//			'options': []
//		},
//		{
//			'name' : 'member-no-lsof-file',
//			'mandatoryOptions': [ 'file'],
//			'arguments' : ['member'],
//			'options': []
//		},
//		{
//			'name' : 'member-no-lsof-console',
//			'mandatoryOptions': [ ],
//			'arguments' : ['member'],
//			'options': []
//		},
//		{
//			'name' : 'group-lsof-file',
//			'mandatoryOptions': [ 'group', 'with-lsof', 'file'],
//			'options': []
//		},
//		{
//			'name' : 'group-lsof-console',
//			'mandatoryOptions': [ 'group', 'with-lsof'],
//			'options': []
//		},
//		{
//			'name' : 'group-no-lsof-file',
//			'mandatoryOptions': [ 'group', 'file'],
//			'options': []
//		},
//		{
//			'name' : 'group-no-lsof-console',
//			'mandatoryOptions': [ 'group'],
//			'options': []
//		}
//    ],    
//    'mandatoryOptions': [],
//    'options': []
//}";


management.cli.GfshPrms-commandSpec += "{
    'command': 'show log',
    'executor': 'management.operations.ops.cli.executors.ShowLogExecutor',
    'arguments' : [],
    'modes' : [
		{
			'name' : 'member',
			'mandatoryOptions': [ 'member'],
			'options': []
		}    
    ],
    'mandatoryOptions': [],
    'options': [
    	{
    		'name' : 'num-lines',
    		'probability' : 67	     		
    	}
    ]
}";

management.cli.GfshPrms-commandSpec += "{
    'command': 'gc',
    'executor': 'management.operations.ops.cli.executors.GcExecutor',
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
			'name' : 'all-ds',
			'mandatoryOptions': [],
			'options': []
		}  
    ],
    'mandatoryOptions': [],
    'options': []
}";

management.cli.GfshPrms-commandSpec += "{
    'command': 'shutdown',
    'executor': 'management.operations.ops.cli.executors.ShutDownExecutor',
    'arguments' : [],
    'modes' : [
		{
			'name' : 'all',
			'mandatoryOptions': [],
			'options': []
		}    
    ],
    'mandatoryOptions': [],
    'options': [
    	{
    		'name' : 'time-out',
    		'probability' : 67	     		
    	}
    ]
}";


management.cli.GfshPrms-commandSpec += "{
    'command': 'export logs',
    'executor': 'management.operations.ops.cli.executors.ExportLogsExecutor',
    'arguments' : [],
    'modes' : [
		{
			'name' : 'group',
			'mandatoryOptions': ['group'],
			'options': []
		},
		{
			'name' : 'member',
			'mandatoryOptions': ['member'],
			'options': []
		},
		{
			'name' : 'all',
			'mandatoryOptions': [],
			'options': []
		},
		{
			'name' : 'group-log-level',
			'mandatoryOptions': ['group', 'log-level'],
			'options': []
		},
		{
			'name' : 'member-log-level',
			'mandatoryOptions': ['member', 'log-level'],
			'options': []
		},
		{
			'name' : 'all-log-level',
			'mandatoryOptions': ['log-level'],
			'options': []
		},
		{
			'name' : 'group-only-log-level',
			'mandatoryOptions': ['group', 'only-log-level'],
			'options': []
		},
		{
			'name' : 'member-only-log-level',
			'mandatoryOptions': ['member', 'only-log-level'],
			'options': []
		},
		{
			'name' : 'all-only-log-level',
			'mandatoryOptions': ['only-log-level'],
			'options': []
		},
		{
			'name' : 'member-merge-log',
			'mandatoryOptions': ['member', 'merge-log'],
			'options': []
		},
		{
			'name' : 'group-merge-log',
			'mandatoryOptions': ['group', 'merge-log'],
			'options': []
		},
		{
			'name' : 'all-merge-log',
			'mandatoryOptions': ['merge-log'],
			'options': []
		},
		{
			'name' : 'group-merge-log-log-level',
			'mandatoryOptions': ['group', 'merge-log', 'log-level'],
			'options': []
		},
		{
			'name' : 'member-merge-log-log-level',
			'mandatoryOptions': ['member', 'merge-log', 'log-level'],
			'options': []
		},
		{
			'name' : 'all-merge-log-log-level',
			'mandatoryOptions': ['merge-log', 'log-level'],
			'options': []
		},
		{
			'name' : 'group-start-time',
			'mandatoryOptions': ['group', 'start-time'],
			'options': []
		},
		{
			'name' : 'member-start-time',
			'mandatoryOptions': ['member', 'start-time'],
			'options': []
		},
		{
			'name' : 'all-start-time',
			'mandatoryOptions': ['start-time'],
			'options': []
		},
		{
			'name' : 'group-end-time',
			'mandatoryOptions': ['group', 'end-time'],
			'options': []
		},
		{
			'name' : 'member-end-time',
			'mandatoryOptions': ['member', 'end-time'],
			'options': []
		},
		{
			'name' : 'all-end-time',
			'mandatoryOptions': ['end-time'],
			'options': []
		},
		{
			'name' : 'group-start-time-end-time',
			'mandatoryOptions': ['group', 'start-time', 'end-time'],
			'options': []
		},
		{
			'name' : 'member-start-time-end-time',
			'mandatoryOptions': ['member', 'start-time', 'end-time'],
			'options': []
		},
		{
			'name' : 'all-start-time-end-time',
			'mandatoryOptions': ['start-time', 'end-time'],
			'options': []
		},
		{
			'name' : 'group-start-time-end-time-merge-log',
			'mandatoryOptions': ['group', 'start-time', 'end-time', 'merge-log'],
			'options': []
		},
		{
			'name' : 'member-start-time-end-time-merge-log',
			'mandatoryOptions': ['member', 'start-time', 'end-time', 'merge-log'],
			'options': []
		},
		{
			'name' : 'all-start-time-end-time-merge-log',
			'mandatoryOptions': ['start-time', 'end-time', 'merge-log'],
			'options': []
		}
    ],
    'mandatoryOptions': ['dir'],
    'options': []
}";


management.cli.GfshPrms-commandList = "netstat" "gc" "show log" "shutdown" "export logs";
management.cli.GfshPrms-cliCommands = ONEOF "netstat" "gc" "show log" "shutdown" "export logs" FOENO;