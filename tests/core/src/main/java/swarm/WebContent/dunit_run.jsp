<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<%@taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<jsp:include page="swarm_head_inc.jsp"></jsp:include>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<title>Swarm DUnit Run Detail</title>
</head>
<body>

<jsp:include page="swarm_inc.jsp"></jsp:include>

<div class="resultbox">
<h1>Run Detail</h1>
${run.verboseHtml}
<div class="fs"></div>
</div>


<div class="resulttabs">
<div class="activetab" id="failtab"><a href="#" onclick="switchResultTab('fail');return false;">Fails (${run.failCount})</a></div>
<div class="inactivetab" id="passtab"><a href="#" onclick="switchResultTab('pass');return false;">Passes (${run.passCount})</a></div>
<div class="inactivetab" id="bothtab" style="display:none;"><a href="#" onclick="switchResultTab('both');return false;">All (${run.failCount+run.passCount})</a></div>
<div class="fs"></div>
</div>
<div class="resultbox" style="margin-top:0px;">

<div id="failcontent">
<c:forEach items="${run.fails}" var="dutmd">
<div class="dunitMethodResult">
<div class="leftResult">
<div class="timestamp">${dutmd.timeString}</div>
<div class="runInfo"><a href="/shh/run/${dutmd.runId}">DUnit Run #${dutmd.runId}</a></div>
<div class="runInfo"><a href="/shh/user/${dutmd.run.userName}">user: ${dutmd.run.userName}</a></div>
<div class="runInfo">branch: ${dutmd.run.branch}</div>
<div class="runInfo">rev: ${dutmd.run.revision}</div>
<div class="runInfo">os: ${dutmd.run.osName}</div>
<div class="runInfo">jdk: ${dutmd.run.javaVmVendor}: ${dutmd.run.javaVersion}</div>
</div>
<div class="rightResult">
<div class="${dutmd.fail ? 'statusfail' : 'statuspass'}"><a href="/shh/result/${dutmd.id}">${dutmd.status} - ${dutmd.methodInfo.name}</a> <a class="smalllink" href="/shh/method/${dutmd.methodInfo.id}">view method history</a></div>
<div class="errorText">
<c:choose>
<c:when test="${dutmd.error==null}">
<a href="/shh/result/${dutmd.id}">No error - Click for details</a>
</c:when>
<c:otherwise>
<a href="/shh/result/${dutmd.id}">${dutmd.error}</a>
</c:otherwise>
</c:choose>

</div>
</div>
<div class="fs"></div>
</div>
<div class="fs"></div>
</c:forEach>
</div>


<div id="passcontent" style="display: none;">
<table class="passsummary">
<thead>
<th>Status</th>
<th>Class</th>
<th>Method</th>
<th>Time Took</th>
</thead>
<tbody>
<c:forEach items="${run.passSummaries}" var="mrs">
<tr>
<td>
<a href="/shh/result/${mrs.id}" title="View method result detail">PASS</a>
</td>
<td>
<a href="/shh/class/${mrs.classId}" title="View class details: ${mrs.className}">${mrs.veryShortClassName}</a>
</td>
<td>
<a href="/shh/method/${mrs.methodId}" title="View method details">${mrs.shortMethodName}</a>
</td>
<td>${mrs.tookMs}ms</td>
</tr>
</c:forEach>
</tbody>
</table>
</div>


<div id="bothcontent" style="display:none;">
</div>
</div>

</body>
</html>