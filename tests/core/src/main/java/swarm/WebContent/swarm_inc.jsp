<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<%@taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<div class="toolbar">
<div class="section"><a href="/shh/runs">Recent DUnit Runs</a></div>
<div class="section"><a href="/shh/classes">Explore Test Classes</a></div>
<div class="section">
<form action="/shh/search" method="get">
<input type="text" size="10" name="query" value="Text Query" onfocus="this.value='';this.onfocus=null;"/><input type="submit" value="go"/>
</form>
</div>

</div>


