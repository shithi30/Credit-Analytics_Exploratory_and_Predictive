/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1896244099
- Data: features_for_PU_dormant.csv
- Function: 
- Table:
- File: inf_tests.txt
- Presentation: https://docs.google.com/presentation/d/1kI5fvuQ-f3e7OXyeI-pfITlOLRv01xS3LQd4U9_CQ9o/edit#slide=id.p
- Email thread: 
- Notes (if any): 
*/

-- April to May: PU to dormant
drop table if exists data_vajapora.help_c; 
create table data_vajapora.help_c as
select distinct mobile_no
from tallykhata.event_transacting_fact 
where event_date>='2021-05-01' and event_date<='2021-05-31';
	
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select *
from 
	(select mobile_no, max(report_date) max_pu_date 
	from data_vajapora.tk_power_users_10
	where report_date>='2021-04-01' and report_date<='2021-04-30'
	group by 1 
	) tbl1 
	
	left join 
	
	data_vajapora.help_c tbl2 using(mobile_no)
where tbl2.mobile_no is null 
limit 10000; 

drop table if exists data_vajapora.help_e;
create table data_vajapora.help_e as
select 
	tbl0.mobile_no, 
	case when update_or_reg_date is not null then 1 else 0 end if_updated, 
	max_pu_date-reg_date age_till_wk,
	count(distinct case when entry_type=1 then pk_id else null end) trt,
	count(distinct case when entry_type=2 then pk_id else null end) opens,
	sum(case when sec_with_tk is not null then sec_with_tk else 0 end) sec_spent,
	count(distinct roaming_date) roaming_days,
	count(distinct case when entry_type=1 then tbl1.event_date else null end) txn_days,
	count(distinct rau_date) rau_days,
	0 grp 
from 
	(select mobile_no, max_pu_date-6 max_pu_date_minus_7, max_pu_date
	from data_vajapora.help_a
	) tbl0 
	
	left join 

	(select mobile_no, event_date, pk_id, entry_type
	from tallykhata.event_transacting_fact 
	where 
		(entry_type=1 or event_name='app_opened')
		and event_date>='2021-04-01' and event_date<='2021-05-31'
	) tbl1 on(tbl0.mobile_no=tbl1.mobile_no and tbl1.event_date>=tbl0.max_pu_date_minus_7 and tbl1.event_date<=tbl0.max_pu_date)
	
	left join 
	
	(select mobile_no, sec_with_tk, event_date
	from tallykhata.daily_times_spent_individual 
	where event_date>='2021-04-01' and event_date<='2021-05-31'
	) tbl3 on(tbl0.mobile_no=tbl3.mobile_no and tbl3.event_date>=tbl0.max_pu_date_minus_7 and tbl3.event_date<=tbl0.max_pu_date)
	
	left join 
	
	(select mobile_no, roaming_date
	from tallykhata.roaming_users
	where roaming_date>='2021-04-01' and roaming_date<='2021-05-31'
	) tbl4 on(tbl0.mobile_no=tbl4.mobile_no and tbl4.roaming_date>=tbl0.max_pu_date_minus_7 and tbl4.roaming_date<=tbl0.max_pu_date)
	
	left join 
	
	(select mobile_no, date(update_or_reg_datetime) update_or_reg_date
	from data_vajapora.version_wise_days 
	where date(update_or_reg_datetime)>='2021-04-01' and date(update_or_reg_datetime)<='2021-05-31'
	) tbl5 on(tbl0.mobile_no=tbl5.mobile_no and tbl5.update_or_reg_date>=tbl0.max_pu_date_minus_7 and tbl5.update_or_reg_date<=tbl0.max_pu_date)
	
	left join 
	
	(select mobile_number mobile_no, date(created_at) reg_date 
	from public.register_usermobile 
	) tbl6 on(tbl0.mobile_no=tbl6.mobile_no)
	
	left join 
	
	(select mobile_no, rau_date
	from tallykhata.tallykhata_regular_active_user
	where 
		rau_category=3 
		and rau_date>='2021-04-01' and rau_date<='2021-05-31'
	) tbl7 on(tbl0.mobile_no=tbl7.mobile_no and tbl7.rau_date>=tbl0.max_pu_date_minus_7 and tbl7.rau_date<=tbl0.max_pu_date)
group by 1, 2, 3; 



-- April to May: dormant to PU
drop table if exists data_vajapora.help_d; 
create table data_vajapora.help_d as
select distinct mobile_no
from tallykhata.event_transacting_fact 
where event_date>='2021-04-01' and event_date<='2021-04-30';

drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select *
from 
	(select mobile_no, max(report_date) max_pu_date  
	from data_vajapora.tk_power_users_10
	where report_date>='2021-05-01' and report_date<='2021-05-31'
	group by 1
	) tbl1 
	
	left join 
	
	data_vajapora.help_d tbl2 using(mobile_no)
where tbl2.mobile_no is null 
limit 10000; 

drop table if exists data_vajapora.help_f;
create table data_vajapora.help_f as
select 
	tbl0.mobile_no, 
	case when update_or_reg_date is not null then 1 else 0 end if_updated, 
	max_pu_date-reg_date age_till_wk,
	count(distinct case when entry_type=1 then pk_id else null end) trt,
	count(distinct case when entry_type=2 then pk_id else null end) opens,
	sum(case when sec_with_tk is not null then sec_with_tk else 0 end) sec_spent,
	count(distinct roaming_date) roaming_days,
	count(distinct case when entry_type=1 then tbl1.event_date else null end) txn_days,
	count(distinct rau_date) rau_days,
	1 grp 
from 
	(select mobile_no, max_pu_date-6 max_pu_date_minus_7, max_pu_date
	from data_vajapora.help_b
	) tbl0 
	
	left join 

	(select mobile_no, event_date, pk_id, entry_type
	from tallykhata.event_transacting_fact 
	where 
		(entry_type=1 or event_name='app_opened')
		and event_date>='2021-04-01' and event_date<='2021-05-31'
	) tbl1 on(tbl0.mobile_no=tbl1.mobile_no and tbl1.event_date>=tbl0.max_pu_date_minus_7 and tbl1.event_date<=tbl0.max_pu_date)
	
	left join 
	
	(select mobile_no, sec_with_tk, event_date
	from tallykhata.daily_times_spent_individual 
	where event_date>='2021-04-01' and event_date<='2021-05-31'
	) tbl3 on(tbl0.mobile_no=tbl3.mobile_no and tbl3.event_date>=tbl0.max_pu_date_minus_7 and tbl3.event_date<=tbl0.max_pu_date)
	
	left join 
	
	(select mobile_no, roaming_date
	from tallykhata.roaming_users
	where roaming_date>='2021-04-01' and roaming_date<='2021-05-31'
	) tbl4 on(tbl0.mobile_no=tbl4.mobile_no and tbl4.roaming_date>=tbl0.max_pu_date_minus_7 and tbl4.roaming_date<=tbl0.max_pu_date)
	
	left join 
	
	(select mobile_no, date(update_or_reg_datetime) update_or_reg_date
	from data_vajapora.version_wise_days 
	where date(update_or_reg_datetime)>='2021-04-01' and date(update_or_reg_datetime)<='2021-05-31'
	) tbl5 on(tbl0.mobile_no=tbl5.mobile_no and tbl5.update_or_reg_date>=tbl0.max_pu_date_minus_7 and tbl5.update_or_reg_date<=tbl0.max_pu_date)
	
	left join 
	
	(select mobile_number mobile_no, date(created_at) reg_date 
	from public.register_usermobile 
	) tbl6 on(tbl0.mobile_no=tbl6.mobile_no)
	
	left join 
	
	(select mobile_no, rau_date
	from tallykhata.tallykhata_regular_active_user
	where 
		rau_category=3 
		and rau_date>='2021-04-01' and rau_date<='2021-05-31'
	) tbl7 on(tbl0.mobile_no=tbl7.mobile_no and tbl7.rau_date>=tbl0.max_pu_date_minus_7 and tbl7.rau_date<=tbl0.max_pu_date)
group by 1, 2, 3; 



-- unified dataset, with time rightly brougnt
select mobile_no, if_updated, age_till_wk, trt, opens, roaming_days, txn_days, rau_days, sec_spent_2, grp 
from 
	data_vajapora.help_f tbl1 
	
	inner join 
	
	(select 
		tbl0.mobile_no, 
		sum(case when sec_with_tk is not null then sec_with_tk else 0 end) sec_spent_2
	from 
		(select mobile_no, max_pu_date-6 max_pu_date_minus_7, max_pu_date
		from data_vajapora.help_b
		) tbl0 
		
		left join 
		
		(select mobile_no, sec_with_tk, event_date
		from tallykhata.daily_times_spent_individual 
		where event_date>='2021-04-01' and event_date<='2021-05-31'
		) tbl3 on(tbl0.mobile_no=tbl3.mobile_no and tbl3.event_date>=tbl0.max_pu_date_minus_7 and tbl3.event_date<=tbl0.max_pu_date)
	group by 1
	) tbl2 using(mobile_no)

union all

select mobile_no, if_updated, age_till_wk, trt, opens, roaming_days, txn_days, rau_days, sec_spent_2, grp 
from 
	data_vajapora.help_e tbl1 
	
	inner join 
	
	(select 
		tbl0.mobile_no, 
		sum(case when sec_with_tk is not null then sec_with_tk else 0 end) sec_spent_2
	from 
		(select mobile_no, max_pu_date-6 max_pu_date_minus_7, max_pu_date
		from data_vajapora.help_a
		) tbl0 
		
		left join 
		
		(select mobile_no, sec_with_tk, event_date
		from tallykhata.daily_times_spent_individual 
		where event_date>='2021-04-01' and event_date<='2021-05-31'
		) tbl3 on(tbl0.mobile_no=tbl3.mobile_no and tbl3.event_date>=tbl0.max_pu_date_minus_7 and tbl3.event_date<=tbl0.max_pu_date)
	group by 1
	) tbl2 using(mobile_no); 