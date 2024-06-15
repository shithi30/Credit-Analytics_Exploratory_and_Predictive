/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1227936163
- Data: 
- Function: 
- Table:
- File: 
- Presentation: 
- Email thread: 
- Notes (if any): 
*/

-- merchants' sequenced transaction dates
drop table if exists data_vajapora.user_date_seq;
create table data_vajapora.user_date_seq as
select *, row_number() over(partition by mobile_no order by created_datetime) user_date_seq
from 
	(select distinct mobile_no, created_datetime
	from tallykhata.tallykhata_fact_info_final
	) tbl1; 

-- churned merchants', formerly in a specific group, sequenced transaction dates
drop table if exists data_vajapora.help_c;
create table data_vajapora.help_c as
select *
from 
	data_vajapora.user_date_seq tbl0
	
	inner join 

	(select mobile_no
	from tallykhata.tallykhata_fact_info_final 
	group by 1 
	having max(created_datetime)<=current_date-30 -- define churn
	) tbl1 using(mobile_no)
	
	-- specify group
	/*inner join 
	
	(select distinct mobile_no
	from tallykhata.tallykhata_regular_active_user
	where rau_category=3
	) tbl2 using(mobile_no)*/
	
	inner join 
	
	(select distinct mobile_no
	from tallykhata.tallykahta_regular_active_user_new
	where rau_category=10
	) tbl2 using(mobile_no)
; 

-- calculation of avg_consec_day_gap for the churned group 
drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select mobile_no, ceil(avg(consec_day_gap)) avg_consec_day_gap
from 
	(select tbl1.mobile_no, tbl2.created_datetime-tbl1.created_datetime consec_day_gap 
	from 
		data_vajapora.help_c tbl1 
		inner join 
		data_vajapora.help_c tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.user_date_seq=tbl2.user_date_seq-1)
	) tbl1
group by 1 
having ceil(avg(consec_day_gap))<=30 -- days to perform analysis for
order by random() 
limit 25000; -- sample size

-- tendencies in cumulative
select 
	tbl1.avg_consec_day_gap, 
	tbl1.merchants, 
	tbl3.total_merchants, 
	sum(tbl2.merchants) cum_merchants,
	sum(tbl2.merchants)*1.00/tbl3.total_merchants cum_merchants_pct
from 
	(select avg_consec_day_gap, count(mobile_no) merchants
	from data_vajapora.help_b
	group by 1
	) tbl1 
	
	inner join 
	
	(select avg_consec_day_gap, count(mobile_no) merchants
	from data_vajapora.help_b
	group by 1
	) tbl2 on(tbl1.avg_consec_day_gap>=tbl2.avg_consec_day_gap), 
	
	(select count(mobile_no) total_merchants
	from data_vajapora.help_b
	) tbl3
group by 1, 2, 3 
order by 1 asc; 

