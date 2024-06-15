/*
- Viz: 
- Data: 
- Function: 
- Table: data_vajapora.pred_churn, to DB from NB
- File: 
	-- training set: daily_churn_pred_7_2.csv
	-- test set: daily_churn_pred_2_full.csv
- Path: http://localhost:8888/notebooks/Churn%20Prediction%2001/daily_churn_prediction_6_layer_3RAU_stats_2.ipynb
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
	-- accuracy 71%, recall 80%, f1-score 73%
	-- in stead of DAUs of the date, 14 prev day's users were featurised for results on the next 14 days
	-- for training, choose current_date-16
	-- train on 2 lac examples
	-- training-test sets will give 50-50 distribution of classes
	-- use same script for generating output for real-world examples 
*/

-- generating train/validation/test data for: '2021-09-07'

-- active days in last 14 days
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select 
	mobile_no,
	count(case when event_date>='2021-09-07'::date-14 and event_date<'2021-09-07'::date-7 then event_date else null end) week_2_active_days,
	count(case when event_date>='2021-09-07'::date-7 and event_date<'2021-09-07' then event_date else null end) week_3_active_days
from tallykhata.tallykhata_user_date_sequence_final 
where event_date>='2021-09-07'::date-14 and event_date<'2021-09-07'::date
group by 1;

-- TRT/TACS in last 14 days
drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select 
	mobile_no,
	count(case when created_datetime>='2021-09-07'::date-14 and created_datetime<'2021-09-07'::date-7 and entry_type=1 then auto_id else null end) week_2_trt,
	count(case when created_datetime>='2021-09-07'::date-7 and created_datetime<'2021-09-07' and entry_type=1 then auto_id else null end) week_3_trt,
	count(case when created_datetime>='2021-09-07'::date-14 and created_datetime<'2021-09-07'::date-7 and entry_type=2 then auto_id else null end) week_2_tacs,
	count(case when created_datetime>='2021-09-07'::date-7 and created_datetime<'2021-09-07' and entry_type=2 then auto_id else null end) week_3_tacs
from tallykhata.tallykhata_fact_info_final
where created_datetime>='2021-09-07'::date-14 and created_datetime<'2021-09-07'::date
group by 1;

-- roaming days in the last 14 days
drop table if exists data_vajapora.help_c;
create table data_vajapora.help_c as
select 
	mobile_no,
	count(case when roaming_date>='2021-09-07'::date-14 and roaming_date<'2021-09-07'::date-7 then roaming_date else null end) week_2_roam_days,
	count(case when roaming_date>='2021-09-07'::date-7 and roaming_date<'2021-09-07' then roaming_date else null end) week_3_roam_days
from tallykhata.roaming_users
where roaming_date>='2021-09-07'::date-14 and roaming_date<'2021-09-07'::date
group by 1;

-- seconds spent in the last 14 days
drop table if exists data_vajapora.help_d;
create table data_vajapora.help_d as
select 
	mobile_no,
	sum(case when event_date>='2021-09-07'::date-14 and event_date<'2021-09-07'::date-7 then sec_with_tk else 0 end) week_2_spent_sec,
	sum(case when event_date>='2021-09-07'::date-7 and event_date<'2021-09-07' then sec_with_tk else 0 end) week_3_spent_sec
from tallykhata.daily_times_spent_individual_data 
where event_date>='2021-09-07'::date-14 and event_date<'2021-09-07'::date
group by 1; 

-- 3RAU tendencies so far
drop table if exists data_vajapora.help_g;
create table data_vajapora.help_g as
select mobile_no, min(report_date::date) first_3rau_date, max(report_date::date) last_3rau_date, count(report_date::date) days_in_3rau 
from tallykhata.regular_active_user_event
where 
	rau_category=3
	and report_date::date<'2021-09-07'
group by 1; 

-- if activity declined >=30% in the next 14 days (for labels)
drop table if exists data_vajapora.help_f;
create table data_vajapora.help_f as
select mobile_no, case when (activity_after-activity_before)*1.00/activity_before<=-0.3 then 1 else 0 end churn_cat
from 
	(select 
		mobile_no, 
		sum(case when event_date>='2021-09-07'::date-14 and event_date<'2021-09-07'::date then sec_with_tk else 0 end) activity_before, 
		sum(case when event_date>='2021-09-07'::date and event_date<'2021-09-07'::date+14 then sec_with_tk else 0 end) activity_after
	from tallykhata.daily_times_spent_individual_data 
	where event_date>='2021-09-07'::date-14 and event_date<'2021-09-07'::date+14
	group by 1
	) tbl1 
where activity_before!=0; 

/*
-- if activity declined >=30% in the next 14 days (for labels)
drop table if exists data_vajapora.help_f;
create table data_vajapora.help_f as
select mobile_no, case when (activity_after-activity_before)*1.00/activity_before<=-0.3 then 1 else 0 end churn_cat
from 
	(select 
		mobile_no, 
		count(case when event_date>='2021-09-07'::date-14 and event_date<'2021-09-07'::date then date_sequence else null end) activity_before, 
		count(case when event_date>='2021-09-07'::date and event_date<'2021-09-07'::date+14 then date_sequence else null end) activity_after
	from tallykhata.tallykhata_user_date_sequence_final
	where event_date>='2021-09-07'::date-14 and event_date<'2021-09-07'::date+14
	group by 1
	) tbl1 
where activity_before!=0; 
*/

-- building main dataset
drop table if exists data_vajapora.help_e; 
create table data_vajapora.help_e as
select 
	-- identifier
	mobile_no, 
	
	-- features
	'2021-09-07'::date-reg_date+1 days_with_tk,
	reg_date-'2019-07-01'::date+1 tk_maturity_while_reg,
	
	case when days_in_3rau is not null then days_in_3rau else 0 end days_in_3rau, 
	case when first_3rau_date is not null then first_3rau_date-reg_date+1 else -1 end days_before_first_3rau,
	case when last_3rau_date is not null then '2021-09-07'::date-last_3rau_date+1 else -1 end days_after_last_3rau,
	
	/*case when reg_version is not null then reg_version else -1 end reg_version,
	case when latest_version is not null then latest_version else -1 end latest_version,
	case when versions_used is not null then versions_used else 0 end versions_used,*/
	
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
	where event_date>='2021-09-07'::date-14 and event_date<'2021-09-07'::date
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
	data_vajapora.help_g tbl9 using(mobile_no)
	left join 
	(select mobile_no, min(app_version_number) reg_version, max(app_version_number) latest_version, count(app_version_number) versions_used
	from data_vajapora.version_wise_days
	where date(update_or_reg_datetime)<'2021-09-07'
	group by 1
	) tbl10 using(mobile_no)
	
	left join 
	data_vajapora.help_f tbl11 using(mobile_no)
where reg_date<'2021-09-07'::date-14; 

-- see distribution of labels
select churn_cat, count(mobile_no) merchants, count(mobile_no)*1.00/(select count(mobile_no) from data_vajapora.help_e) merchants_pct 
from data_vajapora.help_e
group by 1; 

-- building training/test dataset
select 
	-- identifier
	mobile_no, 
	-- features
	days_with_tk, tk_maturity_while_reg,
	days_in_3rau, days_before_first_3rau, days_after_last_3rau, 
	-- reg_version, latest_version, versions_used,
	week_2_active_days, week_3_active_days, 
	week_2_trt, week_3_trt, 
	week_2_tacs, week_3_tacs, 
	week_2_roam_days, week_3_roam_days, 
	week_2_spent_sec, week_3_spent_sec, 
	-- labels
	churn_cat
from data_vajapora.help_e
order by random()
limit 200000 -- omit while applying on real-world examples
; 

-- potential 3RAU churn number, pct
select 
	rau3_on_day, 
	count(mobile_no) potential_3rau_churn, 
	count(mobile_no)*1.00/rau3_on_day potential_3rau_churn_pct
from 
	(select mobile_no
	from tallykhata.tallykhata_regular_active_user
	where 
		rau_category=3
		and rau_date='2021-09-20'::date -- place desired date
	) tbl1 
	
	inner join 
	
	(select concat('0', pred_churn) mobile_no 
	from data_vajapora.pred_churn -- from ML model
	) tbl2 using(mobile_no),
	
	(select count(mobile_no) rau3_on_day
	from tallykhata.tallykhata_regular_active_user
	where 
		rau_category=3
		and rau_date='2021-09-20'::date -- place desired date
	) tbl3
group by 1; 
