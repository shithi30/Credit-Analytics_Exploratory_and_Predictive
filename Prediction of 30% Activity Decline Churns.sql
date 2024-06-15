/*
- Viz: 
- Data: 
- Function: 
- Table:
- File: 
- Path: E:\SureCash Work\Daily Churn Prediction 2
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
	- Train on daily_churn_pred_3_1.csv for representative distribution of classes. 
	- Predict on data_vajapora.help_e, that is daily_churn_pred_2_full.csv
	- find res in pred_churn.csv
*/

-- active days in last 14 days
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select 
	mobile_no,
	count(case when event_date>='2021-08-25'::date-14 and event_date<'2021-08-25'::date-7 then event_date else null end) week_2_active_days,
	count(case when event_date>='2021-08-25'::date-7 and event_date<'2021-08-25' then event_date else null end) week_3_active_days
from tallykhata.tallykhata_user_date_sequence_final 
where event_date>='2021-08-25'::date-14 and event_date<'2021-08-25'::date
group by 1;

-- TRT/TACS in last 14 days
drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select 
	mobile_no,
	count(case when created_datetime>='2021-08-25'::date-14 and created_datetime<'2021-08-25'::date-7 and entry_type=1 then auto_id else null end) week_2_trt,
	count(case when created_datetime>='2021-08-25'::date-7 and created_datetime<'2021-08-25' and entry_type=1 then auto_id else null end) week_3_trt,
	count(case when created_datetime>='2021-08-25'::date-14 and created_datetime<'2021-08-25'::date-7 and entry_type=2 then auto_id else null end) week_2_tacs,
	count(case when created_datetime>='2021-08-25'::date-7 and created_datetime<'2021-08-25' and entry_type=2 then auto_id else null end) week_3_tacs
from tallykhata.tallykhata_fact_info_final
where created_datetime>='2021-08-25'::date-14 and created_datetime<'2021-08-25'::date
group by 1;

-- roaming days in the last 14 days
drop table if exists data_vajapora.help_c;
create table data_vajapora.help_c as
select 
	mobile_no,
	count(case when roaming_date>='2021-08-25'::date-14 and roaming_date<'2021-08-25'::date-7 then roaming_date else null end) week_2_roam_days,
	count(case when roaming_date>='2021-08-25'::date-7 and roaming_date<'2021-08-25' then roaming_date else null end) week_3_roam_days
from tallykhata.roaming_users
where roaming_date>='2021-08-25'::date-14 and roaming_date<'2021-08-25'::date
group by 1;

-- seconds spent in the last 14 days
drop table if exists data_vajapora.help_d;
create table data_vajapora.help_d as
select 
	mobile_no,
	sum(case when event_date>='2021-08-25'::date-14 and event_date<'2021-08-25'::date-7 then sec_with_tk else 0 end) week_2_spent_sec,
	sum(case when event_date>='2021-08-25'::date-7 and event_date<'2021-08-25' then sec_with_tk else 0 end) week_3_spent_sec
from tallykhata.daily_times_spent_individual_data 
where event_date>='2021-08-25'::date-14 and event_date<'2021-08-25'::date
group by 1; 

-- if activity declined >=30% in the next 14 days (for labels)
drop table if exists data_vajapora.help_f;
create table data_vajapora.help_f as
select mobile_no, case when (activity_after-activity_before)*1.00/activity_before<=-0.3 then 1 else 0 end churn_cat
from 
	(select 
		mobile_no, 
		count(case when event_date>='2021-08-25'::date-14 and event_date<'2021-08-25'::date then date_sequence else null end) activity_before, 
		count(case when event_date>'2021-08-25'::date and event_date<='2021-08-25'::date+14 then date_sequence else null end) activity_after
	from tallykhata.tallykhata_user_date_sequence_final 
	group by 1
	) tbl1 
where activity_before!=0; 

-- building main dataset
drop table if exists data_vajapora.help_e; 
create table data_vajapora.help_e as
select 
	-- identifier
	mobile_no, 
	
	-- features
	'2021-08-25'::date-reg_date+1 days_with_tk,
	
	case when week_2_active_days is not null then week_2_active_days else 0 end week_2_active_days, 
	case when week_3_active_days is not null then week_3_active_days else 0 end week_3_active_days, 
	
	case when week_2_trt is not null then week_2_trt else 0 end week_2_trt, 
	case when week_3_trt is not null then week_3_trt else 0 end week_3_trt, 
	
	case when week_2_tacs is not null then week_2_tacs else 0 end week_2_tacs, 
	case when week_3_tacs is not null then week_3_tacs else 0 end week_3_tacs, 
	
	case when week_2_roam_days is not null then week_2_roam_days else 0 end week_2_roam_days, 
	case when week_3_roam_days is not null then week_3_roam_days else 0 end week_3_roam_days, 
	
	case when week_2_spent_sec is not null then week_2_spent_sec else 0 end week_2_spent_sec, 
	case when week_3_spent_sec is not null then week_3_spent_sec else 0 end week_3_spent_sec, 
	
	-- labels	
	case when churn_cat is null then 0 else churn_cat end churn_cat
from 
	(select distinct mobile_no 
	from tallykhata.tallykhata_user_date_sequence_final 
	where event_date='2021-08-25'
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
	data_vajapora.help_f tbl9 using(mobile_no)
where reg_date<'2021-08-25'::date-14; 

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
	week_2_active_days, week_3_active_days, 
	week_2_trt, week_3_trt, 
	week_2_tacs, week_3_tacs, 
	week_2_roam_days, week_3_roam_days, 
	week_2_spent_sec, week_3_spent_sec, 
	-- labels
	churn_cat
from
	(select *, row_number() over(partition by churn_cat order by random()) seq
	from data_vajapora.help_e
	) tbl1
where 
	(churn_cat=1 and seq<=20000)
	or 
	(churn_cat=0 and seq<=60000)
order by random(); 
