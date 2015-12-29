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


<div class="resulttabs">
<div class="activetab" id="runstab"><a href="#" onclick="switchDashboardTab('runs');return false;">DUnit Runs</a></div>
<div class="inactivetab" id="classestab"><a href="#" onclick="switchDashboardTab('classes');return false;">DUnit Test Classes</a></div>
<div class="inactivetab" id="failurestab"><a href="#" onclick="switchDashboardTab('failures');return false;">Recent Failures</a></div>
<div class="fs"></div>
</div>



<div>
<h2>Most Recent Run</h2>
<table>
<thead>
<tr>
<th>
ID
</th>
<th>
user_name
</th>
<th>
path
</th>
<th>
sites
</th>
<th>
revision
</th>
<th>
branch
</th>
<th>
os_name
</th>
<th>
os_version
</th>
<th>
java_version
</th>
<th>
java_vm_version
</th>
<th>
java_vm_vendor
</th>

</tr>
</thead>
<tbody>
<tr>
<td><a href="/shh/run/${latestRun.id}">${latestRun.id}</a></td>
<td>${latestRun.userName}</td>
<td>${latestRun.path}</td>
<td>${latestRun.sites}</td>
<td>${latestRun.revision}</td>
<td>${latestRun.branch}</td>
<td>${latestRun.osName}</td>
<td>${latestRun.osVersion}</td>
<td>${latestRun.javaVersion}</td>
<td>${latestRun.javaVmVersion}</td>
<td>${latestRun.javaVmVendor}</td>
</tr>
</tbody>
</table>

<h3>Fails:</h3>

<table>
<thead>
<tr>
<th>
ID
</th>
<th>
method_id
</th>
<th>
status
</th>
<th>
error
</th>
<th>
ClassName
</th>
<th>
MethodName
</th>
<th>
runId
</th>
</tr>
</thead>
<tbody>
<c:forEach items="${latestRun.fails}" var="dutmd">
<tr>
<td><a href="/shh/result/${dutmd.id}">${dutmd.id}</a></td>
<td>${dutmd.methodId}</td>
<td>${dutmd.status}</td>
<td>${dutmd.error}</td>
<td>${dutmd.methodInfo.classInfo.name}</td>
<td>${dutmd.methodInfo.name}</td>
<td><a href="/shh/run/${dutmd.runId}">${dutmd.runId}</a></td>
</tr>
</c:forEach>
</tbody>
</table>

</div>

</div>
<div>
<h2>Checkouts</h2>
<table>
<thead>
<tr>
<th>
Name
</th>
<th>
Checkout Path
</th>
<th>
Base Output Path
</th>
</tr>
</thead>
<tbody>
<c:forEach items="${base_envs}" var="base_env">
<tr>
<td>${base_env.name}</td>
<td>${base_env.checkoutPath}</td>
<td>${base_env.outputPath}</td>
</tr>
</c:forEach>
</tbody>
</table>
</div>

<div>
<h2>Build/Runtime Environments</h2>
<table>
<thead>
<tr>
<th>
Name
</th>
<th>
Targets
</th>
<th>
Build Properties
</th>
<th>
Build Path
</th>
<th>
Results Path
</th>
</tr>
</thead>
<tbody>
<c:forEach items="${runtime_envs}" var="runtime_env">
<tr>
<td>${runtime_env.name}</td>
<td>${runtime_env.targets}</td>
<td>${runtime_env.buildProperties}</td>
<td>${runtime_env.buildPath == null ? 'Auto' : runtime_env.buildPath}</td>
<td>${runtime_env.resultsPath == null ? 'Auto' : runtime_env.resultsPath}</td>
</tr>
</c:forEach>
</tbody>
</table>
</div>

<div>
<h2>DUnit Runs</h2>
<table>
<thead>
<tr>
<th>
ID
</th>
<th>
user_name
</th>
<th>
path
</th>
<th>
sites
</th>
<th>
revision
</th>
<th>
branch
</th>
<th>
os_name
</th>
<th>
os_version
</th>
<th>
java_version
</th>
<th>
java_vm_version
</th>
<th>
java_vm_vendor
</th>

</tr>
</thead>
<tbody>
<c:forEach items="${druns}" var="drun">
<tr>
<td><a href="/shh/run/${drun.id}">${drun.id}</a></td>
<td>${drun.userName}</td>
<td>${drun.path}</td>
<td>${drun.sites}</td>
<td>${drun.revision}</td>
<td>${drun.branch}</td>
<td>${drun.osName}</td>
<td>${drun.osVersion}</td>
<td>${drun.javaVersion}</td>
<td>${drun.javaVmVersion}</td>
<td>${drun.javaVmVendor}</td>
</tr>
</c:forEach>
</tbody>
</table>
</div>


<!--
<div>
<h2>DUnit Method Run Details</h2>
<table>
<thead>
<tr>
<th>
ID
</th>
<th>
method_id
</th>
<th>
status
</th>
<th>
error
</th>
<th>
ClassName
</th>
<th>
MethodName
</th>
<th>
runId
</th>
</tr>
</thead>
<tbody>
<c:forEach items="${dutmds}" var="dutmd">
<tr>
<td>${dutmd.id}</td>
<td>${dutmd.methodId}</td>
<td>${dutmd.status}</td>
<td>${dutmd.error}</td>
<td>${dutmd.methodInfo.classInfo.name}</td>
<td>${dutmd.methodInfo.name}</td>
<td>${dutmd.runId}</td>
</tr>
</c:forEach>
</tbody>
</table>
</div>
-->


<div>
<h2>DUnit Classes</h2>
<table>
<thead>
<tr>
<th>
Name
</th>
<th>
id
</th>
</tr>
</thead>
<tbody>
<c:forEach items="${ducis}" var="duci">
<tr>
<td>${duci.name}</td>
<td>${duci.id}</td>
</tr>
</c:forEach>
</tbody>
</table>
</div>

</body>
</html>