/*
- Viz: 
	- https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1006915387
	- https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1579175614
	- https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1677214658
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): (answer to second query) 
	Nazrul, do we have any analysis about below two points?
	-Baki purchase and payment behavior of a customer. For example, one customer takes 5 times baki in a month. and pays 2 times in a month. And lead time for purchase is 6 days, payment is 15 days.
	-Number of baki customer txn activity in a week by a PU. For example, average 15 activities in 4 days out of last 7 days by one PU. Activity includes baki sale-collection-customer add-tap on customer bistarito
	Hypothesis
	Number of baki customers, and sum of customers' activities are the key factor for PU retention.
*/

-- daily trend analysis of PUs
do $$ 

declare 
	var_date date:=current_date-30; 
begin 
	raise notice 'New OP goes below:'; 
	
	loop
		insert into data_vajapora.last_7_day_pu_cred_stats 
		select 
			var_date report_date, 
			
			count(mobile_no) last_7_days_pus,
			
			count(case when credit_sale_days=0 then mobile_no else null end) credit_sale_0_days_pus,
			count(case when credit_sale_days in(1, 2) then mobile_no else null end) credit_sale_1_to_2_days_pus,
			count(case when credit_sale_days in(3, 4, 5) then mobile_no else null end) credit_sale_3_to_5_days_pus,
			count(case when credit_sale_days in(6, 7) then mobile_no else null end) credit_sale_6_to_7_days_pus,
			
			count(case when credit_sale_txns=0 then mobile_no else null end) credit_sale_txns_0_pus,
			count(case when credit_sale_txns>=1 and credit_sale_txns<=5 then mobile_no else null end) credit_sale_txns_1_to_5_pus,
			count(case when credit_sale_txns>=6 and credit_sale_txns<=10 then mobile_no else null end) credit_sale_txns_6_to_10_pus,
			count(case when credit_sale_txns>=11 and credit_sale_txns<=15 then mobile_no else null end) credit_sale_txns_11_to_15_pus,
			count(case when credit_sale_txns>=16 and credit_sale_txns<=20 then mobile_no else null end) credit_sale_txns_16_to_20_pus,
			count(case when credit_sale_txns>=21 and credit_sale_txns<=25 then mobile_no else null end) credit_sale_txns_21_to_25_pus,
			count(case when credit_sale_txns>25 then mobile_no else null end) credit_sale_txns_more_than_25_pus
		from 
			(select 
				mobile_no, 
				count(tbl2.report_date) credit_sale_days, 
				sum(case when credit_sale_txns is null then 0 else credit_sale_txns end) credit_sale_txns
			from 
				(select distinct mobile_no, report_date 
				from tallykhata.tk_power_users_10 
				where report_date>=var_date-7 and report_date<var_date
				) tbl1
			
				left join 
				
				(select 
					mobile_no, 
					created_datetime report_date, 
					count(auto_id) credit_sale_txns
				from tallykhata.tallykhata_fact_info_final 
				where 
					created_datetime>=var_date-7 and created_datetime<var_date
					and txn_type in('CREDIT_SALE', 'CREDIT_SALE_RETURN')
				group by 1, 2
				) tbl2 using(mobile_no, report_date)
			group by 1
			) tbl1; 
	
		raise notice 'Data generated for: %', var_date; 	
	
		var_date:=var_date+1;
		if var_date=current_date+1 then exit;
		end if; 
	end loop; 
	
end $$; 

select *
from data_vajapora.last_7_day_pu_cred_stats; 



-- for PUs
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select credit_sale_txns, count(mobile_no) pus
from 
	(select 
		mobile_no, 
		count(tbl2.report_date) credit_sale_days, 
		sum(case when credit_sale_txns is null then 0 else credit_sale_txns end) credit_sale_txns
	from 
		(select distinct mobile_no, report_date 
		from tallykhata.tk_power_users_10 
		where report_date>=current_date-7 and report_date<current_date
		) tbl1
	
		left join 
		
		(select 
			mobile_no, 
			created_datetime report_date, 
			count(auto_id) credit_sale_txns
		from tallykhata.tallykhata_fact_info_final 
		where 
			created_datetime>=current_date-7 and created_datetime<current_date
			and txn_type in('CREDIT_SALE', 'CREDIT_SALE_RETURN')
		group by 1, 2
		) tbl2 using(mobile_no, report_date)
	group by 1
	order by random() 
	limit 100000
	) tbl1 
group by 1; 
	
select tbl1.credit_sale_txns, total_pus, sum(pus) pus, sum(pus)*1.00/total_pus pus_pct
from 
	(select credit_sale_txns 
	from data_vajapora.help_a
	) tbl1 
	
	inner join 
	
	data_vajapora.help_a tbl2 on(tbl1.credit_sale_txns<=tbl2.credit_sale_txns),
	
	(select sum(pus) total_pus
	from data_vajapora.help_a
	) tbl3
group by 1, 2
-- having tbl1.credit_sale_txns%10=0
order by 1; 



-- for 3RAUs
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select credit_sale_txns, count(mobile_no) rau3s
from 
	(select 
		mobile_no, 
		count(tbl2.report_date) credit_sale_days, 
		sum(case when credit_sale_txns is null then 0 else credit_sale_txns end) credit_sale_txns
	from 
		(select distinct mobile_no, report_date::date
		from tallykhata.regular_active_user_event
		where 
			rau_category=3
			and report_date::date>=current_date-7 and report_date::date<current_date
		) tbl1
	
		left join 
		
		(select 
			mobile_no, 
			created_datetime report_date, 
			count(auto_id) credit_sale_txns
		from tallykhata.tallykhata_fact_info_final 
		where 
			created_datetime>=current_date-7 and created_datetime<current_date
			and txn_type in('CREDIT_SALE', 'CREDIT_SALE_RETURN')
		group by 1, 2
		) tbl2 using(mobile_no, report_date)
	group by 1
	order by random() 
	limit 100000
	) tbl1 
group by 1; 
	
select tbl1.credit_sale_txns, total_rau3s, sum(rau3s) rau3s, sum(rau3s)*1.00/total_rau3s rau3s_pct
from 
	(select credit_sale_txns 
	from data_vajapora.help_a
	) tbl1 
	
	inner join 
	
	data_vajapora.help_a tbl2 on(tbl1.credit_sale_txns<=tbl2.credit_sale_txns),
	
	(select sum(rau3s) total_rau3s
	from data_vajapora.help_a
	) tbl3
group by 1, 2
-- having tbl1.credit_sale_txns%10=0
order by 1; 



-- for neither 3RAUs nor PUs
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select distinct mobile_no, event_date report_date
from tallykhata.tallykhata_user_date_sequence_final
where event_date>=current_date-7 and event_date<current_date; 

drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select distinct mobile_no, report_date::date
from tallykhata.regular_active_user_event
where 
	rau_category=3
	and report_date::date>=current_date-7 and report_date::date<current_date;

drop table if exists data_vajapora.help_c;
create table data_vajapora.help_c as
select distinct mobile_no, report_date
from tallykhata.tk_power_users_10 
where report_date>=current_date-7 and report_date<current_date;

drop table if exists data_vajapora.help_d;
create table data_vajapora.help_d as
select tbl1.mobile_no, tbl1.report_date
from 		
	data_vajapora.help_a tbl1 
	left join 
	data_vajapora.help_b tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.report_date=tbl2.report_date)
	left join 
	data_vajapora.help_c tbl3 on(tbl1.mobile_no=tbl3.mobile_no and tbl1.report_date=tbl3.report_date)
where 
	tbl2.mobile_no is null
	and tbl3.mobile_no is null; 
	
drop table if exists data_vajapora.help_e;
create table data_vajapora.help_e as
select credit_sale_txns, count(mobile_no) neither_pus_nor_rau3s
from 
	(select 
		mobile_no, 
		count(tbl2.report_date) credit_sale_days, 
		sum(case when credit_sale_txns is null then 0 else credit_sale_txns end) credit_sale_txns
	from 
		data_vajapora.help_d tbl1
	
		left join 
		
		(select 
			mobile_no, 
			created_datetime report_date, 
			count(auto_id) credit_sale_txns
		from tallykhata.tallykhata_fact_info_final 
		where 
			created_datetime>=current_date-7 and created_datetime<current_date
			and txn_type in('CREDIT_SALE', 'CREDIT_SALE_RETURN')
		group by 1, 2
		) tbl2 using(mobile_no, report_date)
	group by 1
	order by random() 
	limit 100000
	) tbl1 
group by 1; 
	
select tbl1.credit_sale_txns, total_neither_pus_nor_rau3s, sum(neither_pus_nor_rau3s) neither_pus_nor_rau3s, sum(neither_pus_nor_rau3s)*1.00/total_neither_pus_nor_rau3s neither_pus_nor_rau3s_pct
from 
	(select credit_sale_txns 
	from data_vajapora.help_e
	) tbl1 
	
	inner join 
	
	data_vajapora.help_e tbl2 on(tbl1.credit_sale_txns<=tbl2.credit_sale_txns),
	
	(select sum(neither_pus_nor_rau3s) total_neither_pus_nor_rau3s
	from data_vajapora.help_e
	) tbl3
group by 1, 2
-- having tbl1.credit_sale_txns%10=0
order by 1; 

