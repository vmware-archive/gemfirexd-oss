function switchResultTab(prefix) {
	var failtab = document.getElementById('failtab');
	var passtab = document.getElementById('passtab');
	var bothtab = document.getElementById('bothtab');
	
	failtab.className = 'inactivetab';
	passtab.className = 'inactivetab';
	bothtab.className = 'inactivetab';
	
	var activetab = document.getElementById(prefix+'tab');
	activetab.className = 'activetab';
	
	var failcontent = document.getElementById('failcontent');
	var passcontent = document.getElementById('passcontent');
	var bothcontent = document.getElementById('bothcontent');
	
	failcontent.style.display = 'none';
	passcontent.style.display = 'none';
	bothcontent.style.display = 'none';
	
	var activecontent = document.getElementById(prefix+'content');
	activecontent.style.display = 'block';
	
}



function switchDashboardTab(prefix) {
	var runstab = document.getElementById('runstab');
	var classestab = document.getElementById('classestab');
	var failurestab = document.getElementById('failurestab');
	
	runstab.className = 'inactivetab';
	classestab.className = 'inactivetab';
	failurestab.className = 'inactivetab';
	
	var activetab = document.getElementById(prefix+'tab');
	activetab.className = 'activetab';
	
	var runscontent = document.getElementById('failcontent');
	var classescontent = document.getElementById('passcontent');
	var bothcontent = document.getElementById('bothcontent');
	
	failcontent.style.display = 'none';
	passcontent.style.display = 'none';
	bothcontent.style.display = 'none';
	
	var activecontent = document.getElementById(prefix+'content');
	activecontent.style.display = 'block';
	
}
