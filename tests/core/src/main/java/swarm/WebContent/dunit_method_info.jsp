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

<div>
<div class="container pad">
<h1>DUnit Method Info</h1>
<h2>${minfo.name}</h2>
<h2>from: <a href="/shh/class/${cinfo.id}">${cinfo.name}</a></h2>
</div>
<div class="resulttabs">
<div class="activetab" id="failtab"><a href="#" onclick="switchResultTab('fail');return false;">Fails (${failCount})</a></div>
<div class="inactivetab" id="passtab"><a href="#" onclick="switchResultTab('pass');return false;">Passes (${passCount})</a></div>
<div class="inactivetab" id="bothtab"><a href="#" onclick="switchResultTab('both');return false;">All (${totalCount})</a></div>
<div class="fs"></div>
</div>

<div class="resultbox">

<div id="failcontent">
<c:choose>
<c:when test="${empty fails}">
No Failures.
</c:when>
<c:otherwise>
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
<div class="${dutmd.fail ? 'statusfail' : 'statuspass'}"><a href="/shh/result/${dutmd.id}">${dutmd.status}</a></div>
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
</c:otherwise>
</c:choose>

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
<div class="${dutmd.fail ? 'statusfail' : 'statuspass'}"><a href="/shh/result/${dutmd.id}">${dutmd.status}</a></div>
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
<div class="${dutmd.fail ? 'statusfail' : 'statuspass'}"><a href="/shh/result/${dutmd.id}">${dutmd.status}</a></div>
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