/*
- Viz: 307.png
- Data: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1430810819
- Table:
- File: 
- Email thread: 
- Notes (if any): 

I analyzed PUs of the last 4 months. Findings:
- On an avg., 50% of them do <=20 CREDIT_SALEs monthly, so limit=20 should satisfy them.
- Similarly, limit=35 should satisfy 70% PUs. 

*/

do $$

declare 
	var_start date:='2021-04-01';
	var_end date:='2021-04-30';
begin
	
	drop table if exists data_vajapora.help_a;
	create table data_vajapora.help_a as
	select cred_txns, count(mobile_no) users
	from 
		(select mobile_no, count(auto_id) cred_txns 
		from tallykhata.tallykhata_fact_info_final
		where 
			created_datetime>=var_start and created_datetime<=var_end
			and txn_type='CREDIT_SALE'
		group by 1 
		) tbl1
		
		inner join 
		
		(select distinct mobile_no
		from tallykhata.tallykhata_usages_data_temp_v1
		where 
			report_date>=var_start and report_date<=var_end
			and total_active_days>=10
		) tbl2 using(mobile_no)
	group by 1
	having cred_txns<=100; 
	
	drop table if exists data_vajapora.help_b;
	create table data_vajapora.help_b as
	select
		tbl1.cred_txns,
		tbl1.users,
		tbl3.total_users,
		sum(tbl2.users) cum_users,
		sum(tbl2.users)/total_users cum_users_pct
	from 
		(select cred_txns, users
		from data_vajapora.help_a
		) tbl1 
		
		inner join 
		
		(select cred_txns, users
		from data_vajapora.help_a
		) tbl2 on(tbl1.cred_txns>=tbl2.cred_txns),
		
		(select sum(users) total_users 
		from data_vajapora.help_a
		) tbl3
	group by 1, 2, 3
	order by 1; 

end $$; 

select *
from data_vajapora.help_b; 