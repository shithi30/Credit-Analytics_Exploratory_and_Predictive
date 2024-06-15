/*
- Viz: 
- Data: 
- Function: 
- Table:
- File: 
	- Input: cluster_txns.csv
	- Cluster script: draw_clusters_demo_3.ipynb, Path: http://localhost:8888/notebooks/Cluster/draw_clusters_demo_3.ipynb
- Presentation: https://docs.google.com/presentation/d/1kI5fvuQ-f3e7OXyeI-pfITlOLRv01xS3LQd4U9_CQ9o/edit#slide=id.p
- Email thread: 
- Notes (if any): 
*/

-- recent 5000 registrations
drop table if exists data_vajapora.cluster_nos; 
create table data_vajapora.cluster_nos as
select mobile_number mobile_no, date(created_at) reg_date 
from public.register_usermobile
where date(created_at)>='2021-03-10' and date(created_at)<='2021-04-10'
order by random() 
limit 5000; 

-- their count of txns, events
drop table if exists data_vajapora.cluster_txns; 
create table data_vajapora.cluster_txns as
select 
	mobile_no, 
	count(case when entry_type=2 then pk_id else null end) event_entries,
	count(case when entry_type=1 then pk_id else null end) txn_entries
from 
	data_vajapora.cluster_nos tbl1 
	
	left join 
	
	(select mobile_no, pk_id, entry_type
	from tallykhata.event_transacting_fact 
	) tbl2 using(mobile_no)
group by 1; 
select *
from data_vajapora.cluster_txns; 

/* export this file and run Python clustering script at this point */

-- see/count rows added by Python
select *
from data_vajapora.cluster_txns_res; 

-- see distribution of clusters
select 
	cluster_res::int,
	count(mobile_no) merchants,
	round(avg(event_entries)) avg_events,
	round(avg(txn_entries)) avg_txns
from data_vajapora.cluster_txns_res
where cluster_res>=0
group by 1
order by 1; 
