/*
- Viz: 
- Data: 
- Function: 
- Table:
- Instructions: https://docs.google.com/presentation/d/1QZDxiefsRldk-Ha9N8bOgbzJNASUXTvihLdXIcvpJHc/edit?pli=1#slide=id.g15dd1dd6f98_0_5
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
	May be filtered for users whose data got synced yesterday. 
	Look up functions: https://docs.google.com/spreadsheets/d/1uBbLShnDsK4wf7tVWiyxBni_c_qpV11JSx_kDZoEbWg/edit#gid=691075832
*/

CREATE OR REPLACE FUNCTION campaign_analytics.fn_baki_45_days_random()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

declare 
	
begin 
	-- merchants' customers' names
	drop table if exists tallykhata.merchant_customer_names;
	create table tallykhata.merchant_customer_names as
	select 
		*
	from 
		(-- only active customers (for retained merchants)
		select
			tbl1.mobile_no, 
			contact, 
			max(id) account_id
		from public.account tbl1
		inner join tallykhata.retained_users_daily tbl2 on tbl1.mobile_no =tbl2.mobile_number
		where type=2
		and is_active is true
		group by 1, 2
		) tbl1 
		
		inner join 
		
		(-- customers' names (for retained merchants)
		select 
			id account_id, 
			name
		from public.account tbl1
		inner join tallykhata.retained_users_daily tbl2 on tbl1.mobile_no =tbl2.mobile_number 
		) tbl2 using(account_id); 
	
	-- existing baki from customers
	drop table if exists tallykhata.baki_from_customers;
	create table tallykhata.baki_from_customers as
	select 
		tbl1.mobile_no, 
		account_id,
		count(case when txn_type=3 and txn_mode=1 and coalesce(amount, 0)>0 then id else null end) dilam_txn, 
		count(case when txn_type=3 and txn_mode=1 and coalesce(amount_received, 0)>0 then id else null end) pelam_txn, 
		sum(case when txn_type=3 and txn_mode=1 and coalesce(amount, 0)>0 then amount else 0 end)
		-
		sum(case when txn_type=3 and txn_mode=1 and coalesce(amount_received, 0)>0 then amount_received else 0 end)
		baki
	from public.journal tbl1
	inner join tallykhata.retained_users_daily tbl2 on tbl1.mobile_no =tbl2.mobile_number 
	where 1=1
		and tbl2.data_generation_date=current_date
		and tbl1.is_active is true
		and date(tbl1.create_date)<current_date
	group by 1, 2; 
	
	-- cases where customers returned credit in the last 45 days
	drop table if exists tallykhata.porishodh_from_customers_last_45_days; 
	create table tallykhata.porishodh_from_customers_last_45_days as
	select 
		tbl1.mobile_no, 
		account_id 
	from public.journal tbl1
	inner join tallykhata.retained_users_daily tbl2 on tbl1.mobile_no =tbl2.mobile_number
	where 1=1
		and tbl2.data_generation_date=current_date
		and txn_type=3 and txn_mode=1 and coalesce(amount_received, 0)>0
		and is_active is true
		and date(create_date)>=current_date-45
	group by 1,2; 
	
	-- existing start_balance+CREDIT_SALE-CREDIT_SALE_RETURN from customers: cases where customers did not return credit in the last 45 days
	drop table if exists tallykhata.baki_from_customers_more_than_45_days; 
	create table tallykhata.baki_from_customers_more_than_45_days as
	select 
		mobile_no, 
		account_id, 
		baki+start_balance baki
	from 
		tallykhata.baki_from_customers tbl1 
		
		inner join 
		
		(select 
			mobile_no, 
			id account_id, 
			start_balance
		from public.account
		) tbl2 using(mobile_no, account_id)
			
		-- excluding customers who paid baki (any amount in last 45 days)
		left join 
		tallykhata.porishodh_from_customers_last_45_days tbl3 using(mobile_no, account_id) 
	where 
		tbl3.mobile_no is null 
		and baki+start_balance>999 
		and dilam_txn>1 and pelam_txn>1;
	
	-- random 1 baki customer per merchant
	drop table if exists tallykhata.baki_45_days_random; 
	create table tallykhata.baki_45_days_random as
	select mobile_no, customer_name, baki
	from 
		(select 
			mobile_no, 
			name customer_name, 
			translate(trim(to_char(baki, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') baki, 
			row_number() over(partition by mobile_no order by random()) seq
		from 
			tallykhata.baki_from_customers_more_than_45_days 
			left join 
			tallykhata.merchant_customer_names tbl3 using(mobile_no, account_id)
		) tbl1 
	where seq=1; 
	
END;
$function$
;

-- select campaign_analytics.fn_baki_45_days_random(); 

select * 
from tallykhata.baki_45_days_random; 












