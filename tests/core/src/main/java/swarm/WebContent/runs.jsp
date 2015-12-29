<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<%@taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<jsp:include page="swarm_head_inc.jsp"></jsp:include>
<title>Swarm Dashboard</title>
</head>
<body>

<jsp:include page="swarm_inc.jsp"></jsp:include>

<div class="resultbox">
<div class="selectsection">
<h1>Filter</h1>
<form action="/shh/runs" method="get">
<select name="user">
	<option value="all" ${user=='all' ? 'selected' : ''}>All</option> 
	<c:forEach items="${users}" var="usr">
		<option value="${usr}" ${user==usr ? 'selected' : ''}>${usr}</option>
	</c:forEach>
</select>
<select name="os_name">
	<option value="all" ${os_name=='all' ? 'selected' : ''}>All</option>
	<option value="Linux" ${os_name=='Linux' ? 'selected' : ''}>Linux</option>
	<option value="win" ${os_name=='win' ? 'selected' : ''}>Windows</option>	
	<option value="sol" ${os_name=='sol' ? 'selected' : ''}>Solaris</option>	
</select>
<select name="java_version">
	<option value="all" ${java_version=='all' ? 'selected' : ''}>All</option>
	<option value="1.5" ${java_version=='1.5' ? 'selected' : ''}>jdk1.5</option>
	<option value="1.6" ${java_version=='1.6' ? 'selected' : ''}>jdk1.6</option>	
	<option value="1.7" ${java_version=='1.7' ? 'selected' : ''}>jdk1.7</option>	
</select>
<select name="branch">
<c:forEach items="${branches}" var="branch">
<option value="${branch}" ${sessionScope.branch==branch ? 'selected' : ''}>${branch}</option>
</c:forEach>
</select>
<input type="submit" value="go"/>
</form>
</div>
</div>

<div class="resultbox">
<h1>Last 10 Runs</h1>
<table cellpadding="0" cellspacing="0"  style="font-size:0.6em;">
<tr>
<c:forEach items="${last10}" var="run">
<td style="padding:10px 5px 10px 5px;border-right:4px solid white;" class="${run.failCount>0 ? 'failtd' : 'passtd'}"><a href="#run${run.id}" title="View Run Details">${run.failCount}F/${run.passCount}P</a></td>
</c:forEach>
</tr>
</table>
</div>

<div class="resultbox">
<h1>Recent Run Details</h1>
<c:forEach items="${druns}" var="drun">
${drun.verboseHtml}
</c:forEach>
</div>

</body>
</html>
