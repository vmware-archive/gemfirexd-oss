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


<div class="container" style="background-color:white;text-align:center;padding:10px;">
<h1>DUnit Classes (${classCount})</h1>
<table style="font-size:0.8em;margin:auto;">
<thead>
<tr>
<th>
Class Name
</th>
<th>
Method Count
</th>
<th colspan="5">
Last 2 runs
</th>
</tr>
</thead>
<tbody>
<c:forEach items="${ducis}" var="duci">
<tr>
<td style="text-align:right;font-weight:bold;"><a href="/shh/class/${duci.id}" title="See details about this class">${duci.shortName}</a></td>
<td>${duci.methodCount}</td>
<c:forEach items="${duci.last2Runs}" var="run" varStatus="loop">
<td colspan="${loop.last ? 2-loop.index : 1}" style="padding:10px 5px 10px 5px;border-right:4px solid white;" class="${run.failCount>0 ? 'failtd' : 'passtd'}"><a href="/shh/run/${run.runId}" title="View Run Details">${run.passCount}/${run.passCount+run.failCount}</a></td>
</c:forEach>
</tr>
</c:forEach>
</tbody>
</table>
</div>


</body>
</html>
