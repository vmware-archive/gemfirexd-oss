<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<%@taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<jsp:include page="swarm_head_inc.jsp"></jsp:include>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<title>Swarm DUnit Class Detail</title>
</head>
<body>

<jsp:include page="swarm_inc.jsp"></jsp:include>

<div>
<div class="container pad">
<h1>DUnit Class Info</h1>
<h2>${cinfo.name}</h2>
</div>

<div class="resultbox">
<h1>Last 10 Runs</h1>
<table>
<thead>
<tr>
<th>
Total Methods
</th>
<th colspan="10">
Last 10 runs
</th>
</tr>
</thead>
<tbody>
<tr>
<td>${cinfo.methodCount}</td>
<c:forEach items="${cinfo.last10Runs}" var="run" varStatus="loop">
<td colspan="${loop.last ? 10-loop.index : 1}" style="padding:10px 5px 10px 5px;" class="${run.failCount>0 ? 'failtd' : 'passtd'}"><a href="/shh/run/${run.runId}" title="View Run Details">${run.passCount}/${run.passCount+run.failCount}</a></td>
</c:forEach>
</tr>
</tbody>
</table>
</div>



<div class="resultbox">
<h1>Methods</h1>
<table style="width:100%">
<thead style="text-align:center;">
<th>method</th>
<th>last run</th>
<th>last fail</th>
<th>last pass</th>
<th>passes</th>
<th>fails</th>
<th>pass%</th>
</thead>
<tbody  style="font-size:0.8em;">
<c:forEach items="${minfos}" var="minfo">
<tr>
<td style="text-align:right;"><a href="/shh/method/${minfo.id}" title="View Method Detail">${minfo.name}</a></td>
<td class="${minfo.lastRun.fail ? 'failtd' : 'passtd'}"><a href="/shh/result/${minfo.lastRun.id}" title="View result detail">${minfo.lastRun.fail ? 'F' : 'P'}</a></td>
<td><a href="/shh/result/${minfo.lastFail.id}" title="View result detail">${minfo.lastFail.timeString}</a></td>
<td><a href="/shh/result/${minfo.lastFail.id}" title="View result detail">${minfo.lastPass.timeString}</a></td>
<td>${minfo.passCount}</td>
<td>${minfo.failCount}</td>
<td>${minfo.passPercent}</td>

</tr>
</c:forEach>
</tbody>
</table>
</div>
<div class="resulttabs">
<div class="activetab" id="failtab"><a href="#" onclick="switchResultTab('fail');return false;">Fails (${failCount})</a></div>
<div class="inactivetab" id="passtab"><a href="#" onclick="switchResultTab('pass');return false;">Passes (${passCount})</a></div>
<div class="inactivetab" id="bothtab"><a href="#" onclick="switchResultTab('both');return false;">All (${totalCount})</a></div>
<div class="fs"></div>
</div>
<div class="resultbox" style="margin-top:0px;">

<div id="failcontent">
<c:forEach items="${fails}" var="dutmd">
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
<c:forEach items="${passes}" var="dutmd">
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


<div id="bothcontent" style="display:none;">
<c:forEach items="${both}" var="dutmd">
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


</div>
<div class="fs"></div>

</div>


</body>
</html>