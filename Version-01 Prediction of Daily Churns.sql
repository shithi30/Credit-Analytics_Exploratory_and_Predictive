/*
- Viz: 
- Data: 
- Function: 
- Table:
- Path: E:\SureCash Work\Daily Churn Prediction 
- Document/Presentation: https://docs.google.com/document/d/1npCwp_TH3B6dsXdhysSq2aDiP-CyWbvltvNMOwR91e8/edit
- Email thread: 
- Notes (if any): 

I have trained a 6-layer deep NN using 16 features to predict daily churns. 
This gave 86% to 89% accuracy. 

*/

-- + Features: if 3RAU/10RAU/PU, how many days, loc info, BI-type, version info 
-- + Classes: another intermediate class

-- active days in last 21 days
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select 
	mobile_no,
	count(case when event_date>='2021-08-04'::date-21 and event_date<'2021-08-04'::date-14 then event_date else null end) week_1_active_days,
	count(case when event_date>='2021-08-04'::date-14 and event_date<'2021-08-04'::date-7 then event_date else null end) week_2_active_days,
	count(case when event_date>='2021-08-04'::date-7 and event_date<'2021-08-04' then event_date else null end) week_3_active_days
from tallykhata.tallykhata_user_date_sequence_final 
where event_date>='2021-08-04'::date-21 and event_date<'2021-08-04'::date
group by 1;

-- TRT/TACS in last 21 days
drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select 
	mobile_no,
	count(case when created_datetime>='2021-08-04'::date-21 and created_datetime<'2021-08-04'::date-14 and entry_type=1 then auto_id else null end) week_1_trt,
	count(case when created_datetime>='2021-08-04'::date-14 and created_datetime<'2021-08-04'::date-7 and entry_type=1 then auto_id else null end) week_2_trt,
	count(case when created_datetime>='2021-08-04'::date-7 and created_datetime<'2021-08-04' and entry_type=1 then auto_id else null end) week_3_trt,
	count(case when created_datetime>='2021-08-04'::date-21 and created_datetime<'2021-08-04'::date-14 and entry_type=2 then auto_id else null end) week_1_tacs,
	count(case when created_datetime>='2021-08-04'::date-14 and created_datetime<'2021-08-04'::date-7 and entry_type=2 then auto_id else null end) week_2_tacs,
	count(case when created_datetime>='2021-08-04'::date-7 and created_datetime<'2021-08-04' and entry_type=2 then auto_id else null end) week_3_tacs
from tallykhata.tallykhata_fact_info_final
where created_datetime>='2021-08-04'::date-21 and created_datetime<'2021-08-04'::date
group by 1;

-- roaming days in the last 21 days
drop table if exists data_vajapora.help_c;
create table data_vajapora.help_c as
select 
	mobile_no,
	count(case when roaming_date>='2021-08-04'::date-21 and roaming_date<'2021-08-04'::date-14 then roaming_date else null end) week_1_roam_days,
	count(case when roaming_date>='2021-08-04'::date-14 and roaming_date<'2021-08-04'::date-7 then roaming_date else null end) week_2_roam_days,
	count(case when roaming_date>='2021-08-04'::date-7 and roaming_date<'2021-08-04' then roaming_date else null end) week_3_roam_days
from tallykhata.roaming_users
where roaming_date>='2021-08-04'::date-21 and roaming_date<'2021-08-04'::date
group by 1;

-- seconds spent in the last 21 days
drop table if exists data_vajapora.help_d;
create table data_vajapora.help_d as
select 
	mobile_no,
	sum(case when event_date>='2021-08-04'::date-21 and event_date<'2021-08-04'::date-14 then sec_with_tk else 0 end) week_1_spent_sec,
	sum(case when event_date>='2021-08-04'::date-14 and event_date<'2021-08-04'::date-7 then sec_with_tk else 0 end) week_2_spent_sec,
	sum(case when event_date>='2021-08-04'::date-7 and event_date<'2021-08-04' then sec_with_tk else 0 end) week_3_spent_sec
from tallykhata.daily_times_spent_individual_data 
where event_date>='2021-08-04'::date-21 and event_date<'2021-08-04'::date
group by 1; 

-- building main dataset
drop table if exists data_vajapora.help_e; 
create table data_vajapora.help_e as
select 
	-- identifier
	mobile_no, 
	
	-- features
	'2021-08-04'::date-reg_date+1 days_with_tk,
	
	case when week_1_active_days is not null then week_1_active_days else 0 end week_1_active_days, 
	case when week_2_active_days is not null then week_2_active_days else 0 end week_2_active_days, 
	case when week_3_active_days is not null then week_3_active_days else 0 end week_3_active_days, 
	
	case when week_1_trt is not null then week_1_trt else 0 end week_1_trt, 
	case when week_2_trt is not null then week_2_trt else 0 end week_2_trt, 
	case when week_3_trt is not null then week_3_trt else 0 end week_3_trt, 
	
	case when week_1_tacs is not null then week_1_tacs else 0 end week_1_tacs, 
	case when week_2_tacs is not null then week_2_tacs else 0 end week_2_tacs, 
	case when week_3_tacs is not null then week_3_tacs else 0 end week_3_tacs, 
	
	case when week_1_roam_days is not null then week_1_roam_days else 0 end week_1_roam_days, 
	case when week_2_roam_days is not null then week_2_roam_days else 0 end week_2_roam_days, 
	case when week_3_roam_days is not null then week_3_roam_days else 0 end week_3_roam_days, 
	
	case when week_1_spent_sec is not null then week_1_spent_sec else 0 end week_1_spent_sec, 
	case when week_2_spent_sec is not null then week_2_spent_sec else 0 end week_2_spent_sec, 
	case when week_3_spent_sec is not null then week_3_spent_sec else 0 end week_3_spent_sec, 
	
	-- labels
	case when days_active_future is null or days_active_future=0 then 1 else 0 end churn_cat
	
from 
	(select distinct mobile_no 
	from tallykhata.tallykhata_user_date_sequence_final 
	where event_date='2021-08-04'
	) tbl1 
	
	inner join 
	
	(select mobile_number mobile_no, date(created_at) reg_date 
	from public.register_usermobile
	) tbl0 using(mobile_no)
	
	left join 
	data_vajapora.help_a tbl5 using(mobile_no)
	left join 
	data_vajapora.help_b tbl6 using(mobile_no)
	left join 
	data_vajapora.help_c tbl7 using(mobile_no)
	left join 
	data_vajapora.help_d tbl8 using(mobile_no)
	
	left join 
	
	(select mobile_no, count(date_sequence) days_active_future
	from tallykhata.tallykhata_user_date_sequence_final 
	where event_date>'2021-08-04'
	group by 1
	) tbl9 using(mobile_no)
	
where reg_date<'2021-08-04'::date-21; 

-- distribution of labels
select churn_cat, count(mobile_no) merchants
from data_vajapora.help_e
group by 1; 

-- building training/test dataset
select 
	-- identifier
	mobile_no, 
	-- features
	days_with_tk, 
	week_1_active_days, week_2_active_days, week_3_active_days, 
	week_1_trt, week_2_trt, week_3_trt, 
	week_1_tacs, week_2_tacs, week_3_tacs, 
	week_1_roam_days, week_2_roam_days, week_3_roam_days, 
	week_1_spent_sec, week_2_spent_sec, week_3_spent_sec, 
	-- labels
	churn_cat
from
	(select *, row_number() over(partition by churn_cat order by random()) seq
	from data_vajapora.help_e
	) tbl1
where seq<=3500
order by random(); 