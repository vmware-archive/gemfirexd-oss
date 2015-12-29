<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<%@taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<link rel="stylesheet" type="text/css" href="/swarm.css">
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<title>Swarm DUnit Run Detail</title>
</head>
<body>

<jsp:include page="swarm_inc.jsp"></jsp:include>

<div>
<div class="container pad">
<h1>Test Method Execution Detail</h1>
<h2><a href="/shh/class/${method.methodInfo.classInfo.id}" title="View Class Details">${method.methodInfo.classInfo.veryShortName}</a>: <a href="/shh/method/${method.methodInfo.id}"  title="View Method Details">${method.methodInfo.name}</a></h2>
</div>

<div class="resultbox">
<div class="dunitMethodResult" style="border-bottom:none;margin-bottom:0px;padding-bottom:0px;">
<div class="rightResult" style="width:700px;">
<div class="${method.fail ? 'statusfail' : 'statuspass'}"><a href="/shh/result/${method.id}">${method.status}</a></div>
<div class="bigErrorText">
<c:choose>
<c:when test="${method.error==null}">
No Failure - this test method passed
</c:when>
<c:otherwise>
${method.error}
</c:otherwise>
</c:choose>

</div>
</div>
<div class="fs"></div>
</div>
<div class="fs"></div>
</div>

<div class="resultbox">
<h1>From this DUnit Run  by ${run.userName}:</h1>
<div class="dunitMethodResult" id="run${run.id}"  style="border-bottom:none;margin-bottom:0px;padding-bottom:0px;">
<div class="leftResult">
<div class="timestamp">${run.timeString}</div>
<div class="runInfo"><label>user:</label> <a href="/shh/user/${run.userName}">${run.userName}</a> <label>dunit run:</label> <a href="/shh/run/${run.id}">#${run.id}</a></div>
<div class="runInfo"><label>branch:</label> ${run.shortBranch} <label>rev:</label> ${run.revision}</div>
<div class="runInfo"><label>os:</label> ${run.osName} <label>jdk:</label> ${run.shortJavaVmVendor}: ${run.javaVersion}</div>
</div>
<div class="rightResult">
<div class="${run.failCount>0 ? 'statusfail' : 'statuspass'}"><a href="/shh/run/${run.id}">Failures: ${run.failCount} Passes: ${run.passCount}</a></div>

<div style="padding:10px;">
<c:choose>
<c:when test="${run.failCount==0}">
<a href="/shh/run/${run.id}">No failures - Click for details</a>
</c:when>
<c:otherwise>
<div style="text-align:left;"><strong>Failures:</strong></div>
<div class="errorText" style="padding-left:10px;">
<c:forEach items="${run.fails}" var="fail" varStatus="loop">
<c:if test="${loop.index==3 and !loop.last}">
<div id="showMore${run.id}"><a style="font-weight:bold;" href="#" onclick="document.getElementById('moreFails${run.id}').style.display='block';document.getElementById('showMore${run.id}').style.display='none';return false;">Click to see ${run.failCount-3} more failures...</a></div>
<div style="display:none;" id="moreFails${run.id}">
</c:if>
<div style="border-bottom:1px solid red;padding-bottom:3px;"><a href="/shh/result/${fail.id}" title="${fail.error}">${fail.methodInfo.classInfo.veryShortName} : <strong>${fail.methodInfo.name}()</strong></a></div>
<c:if test="${loop.last and loop.index!=3}">
</div>
</c:if>

</c:forEach>
</div>
</c:otherwise>
</c:choose>
</div>
</div>
<div class="fs"></div>
</div>  <!--  end dunitMethodResult/one run -->
</div> <!--  end resultbox -->


<div class="resultbox">
<h1>Other methods in this class:</h1>
<c:choose>
<c:when test="${empty classResults}">
No other results
</c:when>
<c:otherwise>
<c:forEach items="${classResults}" var="dutmd">
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
</c:forEach>
</c:otherwise>
</c:choose>

<div class="fs"></div>
</div>


</div>



</body>
</html>