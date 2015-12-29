paging on;

select a.equip_id, a.context_id, a.stop_date, dsid() as datanode_id 
from CONTEXT_HISTORY a 
left join CHART_HISTORY b 
on (a.equip_id =b.equip_id and a.context_id=b.context_id and a.stop_date=b.stop_date) 
where a.equip_id||cast(a.context_id as char(100)) 
in 
( 
	select equip_id||cast(t.context_id as char(100)) 
	from RECEIVED_HISTORY_LOG t where 1=1 
	and exec_time > 40 
	and stop_date > '2014-01-24 18:47:59' 
	and stop_date < '2014-01-24 18:49:59' 
)
  
