/*
- Viz: 
- Data: 
- Function: 
- Table:
- Instructions: 
- Format: 
- File: https://colab.research.google.com/drive/1QntoTOhCQBaUxLJ_VPkiyP5C1nyHanWl?authuser=1#scrollTo=eHVmyis_Q3gI
- Path: https://drive.google.com/drive/u/1/folders/1-5DW-8eUct_kqcI4PSpa_gkb8feVl97t
- filename: biz_from_txns_1.ipynb
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): See comments in code.
*/

-- regular users and their biz types
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select * 
from 
	(select distinct mobile_no
	from cjm_segmentation.retained_users
	where 
		report_date>=current_date-15 and report_date<current_date
		and (tg like '3RAU%' or tg in('SPU')) 
	) tbl1 
	
	inner join 
		
	(select mobile mobile_no, max(business_type) business_type, max(coalesce(shop_name, business_name, name, merchant_name)) shop_name
	from tallykhata.tallykhata_user_personal_info
	where business_type in('GROCERY', 'PHARMACY', 'ELECTRONICS', 'MFS_MOBILE_RECHARGE', 'CLOTH_STORE', 'STATIONERY', 'HARDWARE')
	group by 1
	) tbl2 using(mobile_no); 
	
-- their distributios
select 
	business_type, 
	count(*) merchants, 
	round(count(*)*1.00/(select count(*) from data_vajapora.help_a), 4) merchants_pct,
	(select count(*) from data_vajapora.help_a) all_merchants
from data_vajapora.help_a
group by 1 
order by 2 desc; 

-- txn features
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select
	mobile_no, 
	
	-- customer
	sum(case when txn_type='CASH_SALE' then cleaned_amount else null end) cash_sale_amt, 
	sum(case when txn_type='CASH_SALE' then 1 else null end) cash_sale_txn, 
	count(distinct case when txn_type='CASH_SALE' then contact else null end) cash_sale_custs, 
	
	sum(case when txn_type='CREDIT_SALE' then cleaned_amount else null end) credit_sale_amt, 
	sum(case when txn_type='CREDIT_SALE' then 1 else null end) credit_sale_txn, 
	count(distinct case when txn_type='CREDIT_SALE' then contact else null end) credit_sale_custs, 
	
	sum(case when txn_type='CREDIT_SALE_RETURN' then cleaned_amount else null end) credit_sale_ret_amt, 
	sum(case when txn_type='CREDIT_SALE_RETURN' then 1 else null end) credit_sale_ret_txn, 
	count(distinct case when txn_type='CREDIT_SALE_RETURN' then contact else null end) credit_sale_ret_custs, 
	
	sum(case when txn_type='Add Customer' then 1 else null end) new_cust_added, 
	
	count(distinct case when txn_type in('CASH_SALE', 'CREDIT_SALE', 'CREDIT_SALE_RETURN', 'Add Customer') then created_datetime else null end) cust_txn_days, 
	
	-- supplier
	sum(case when txn_type='CASH_PURCHASE' then cleaned_amount else null end) cash_purchase_amt, 
	sum(case when txn_type='CASH_PURCHASE' then 1 else null end) cash_purchase_txn, 
	count(distinct case when txn_type='CASH_PURCHASE' then contact else null end) cash_purchase_supps, 
	
	sum(case when txn_type='CREDIT_PURCHASE' then cleaned_amount else null end) credit_purchase_amt, 
	sum(case when txn_type='CREDIT_PURCHASE' then 1 else null end) credit_purchase_txn, 
	count(distinct case when txn_type='CREDIT_PURCHASE' then contact else null end) credit_purchase_supps, 
	
	sum(case when txn_type='CREDIT_PURCHASE_RETURN' then cleaned_amount else null end) credit_purchase_ret_amt, 
	sum(case when txn_type='CREDIT_PURCHASE_RETURN' then 1 else null end) credit_purchase_ret_txn, 
	count(distinct case when txn_type='CREDIT_PURCHASE_RETURN' then contact else null end) credit_purchase_ret_supps, 
	
	sum(case when txn_type='Add Supplier' then 1 else null end) new_supp_added,
	
	count(distinct case when txn_type in('CASH_PURCHASE', 'CREDIT_PURCHASE', 'CREDIT_PURCHASE_RETURN', 'Add Supplier') then created_datetime else null end) supp_txn_days, 
	
	-- malik 
	sum(case when txn_type='MALIK_DILO' then cleaned_amount else null end) malik_dilo_amt, 
	sum(case when txn_type='MALIK_DILO' then 1 else null end) malik_dilo_txn,
	sum(case when txn_type='MALIK_NILO' then cleaned_amount else null end) malik_nilo_amt, 
	sum(case when txn_type='MALIK_NILO' then 1 else null end) malik_nilo_txn,
	count(distinct case when txn_type in('MALIK_DILO', 'MALIK_NILO') then created_datetime else null end) malik_txn_days, 
	
	-- shop
	sum(case when txn_type='CASH_ADJUSTMENT' then cleaned_amount else null end) cash_adj_amt, 
	sum(case when txn_type='CASH_ADJUSTMENT' then 1 else null end) cash_adj_txn, 
	sum(case when txn_type='EXPENSE' then cleaned_amount else null end) expense_amt, 
	sum(case when txn_type='EXPENSE' then 1 else null end) expense_txn, 
	count(distinct case when txn_type in('CASH_ADJUSTMENT', 'EXPENSE') then created_datetime else null end) dokan_txn_days, 
	
	-- ticket size
	 sum(case when txn_type in('CASH_SALE', 'CREDIT_SALE') then cleaned_amount else null end)
	/sum(case when txn_type in('CASH_SALE', 'CREDIT_SALE') then 1 else null end) sale_ticket, 
	avg(case when txn_type in('CASH_SALE') then cleaned_amount else null end) cash_sale_ticket, 
	avg(case when txn_type in('CREDIT_SALE') then cleaned_amount else null end) credit_sale_ticket, 
	
	 sum(case when txn_type in('CASH_PURCHASE', 'CREDIT_PURCHASE') then cleaned_amount else null end)
	/sum(case when txn_type in('CASH_PURCHASE', 'CREDIT_PURCHASE') then 1 else null end) purchase_ticket,
	avg(case when txn_type in('CASH_PURCHASE') then cleaned_amount else null end) cash_purchase_ticket, 
	
	 sum(case when txn_type in('CASH_ADJUSTMENT', 'EXPENSE') then cleaned_amount else null end)
	/sum(case when txn_type in('CASH_ADJUSTMENT', 'EXPENSE') then 1 else null end) ops_ticket
from 
	(select mobile_no, cleaned_amount, txn_type, contact, created_datetime
	from tallykhata.tallykhata_fact_info_final 
	where created_datetime>=current_date-21 and created_datetime<current_date
	) tbl1 
	
	inner join 
	
	(select mobile_no 
	from data_vajapora.help_a
	) tbl2 using(mobile_no) 
group by 1; 

-- sms features
drop table if exists data_vajapora.help_c; 
create table data_vajapora.help_c as
select 
	translate(substring(message_body, length(message_body)-10, 12), '০১২৩৪৫৬৭৮৯', '0123456789') mobile_no, 
	count(id) sms_used, 
	count(case when message_body like '%অনুগ্রহ করে%' then id else null end) tagada_sms_used, 
	count(case when message_body like '%http%' then id else null end) link_sms_used, 
	count(distinct date(request_time)) sms_used_days
from public.t_scsms_message_archive_v2 as s
where
	upper(s.channel) in('TALLYKHATA_TXN') 
	and upper(trim(s.bank_name)) = 'SURECASH'
	and lower(s.message_body) not like '%verification code%'
	and s.telco_identifier_id in(66, 64, 61, 62, 49, 67) 
	and upper(s.message_status) in ('SUCCESS', '0') 
	and date(request_time)>=current_date-21 and date(request_time)<current_date 
group by 1; 

-- cust/supp+ features
drop table if exists data_vajapora.help_d; 
create table data_vajapora.help_d as
select 
	mobile_no, 
	count(distinct case when type=2 then contact else null end) all_cust_added, 
	count(distinct case when type=3 then contact else null end) all_supp_added
from 
	(select mobile_no, type, contact
	from public.account
	where type in(2, 3)
	) tbl1 
	
	inner join 
	
	(select mobile_no 
	from data_vajapora.help_a
	) tbl2 using(mobile_no) 
group by 1; 

-- TK features
drop table if exists data_vajapora.help_e; 
create table data_vajapora.help_e as
select 
	mobile mobile_no, 
	count(distinct app_version_number) versions_exp, 
	max(app_version_number) version_now, 
	current_date-min(created_at)::date days_with_tk
from public.register_historicalregistereduser
group by 1;    

-- full dataset
select 
	-- category
	business_type category,
	
	-- txn features
	coalesce(cash_sale_amt, 0) cash_sale_amt, 
	coalesce(cash_sale_txn, 0) cash_sale_txn, 
	coalesce(cash_sale_custs, 0) cash_sale_custs, 
	coalesce(credit_sale_amt, 0) credit_sale_amt, 
	coalesce(credit_sale_txn, 0) credit_sale_txn, 
	coalesce(credit_sale_custs, 0) credit_sale_custs, 
	coalesce(credit_sale_ret_amt, 0) credit_sale_ret_amt, 
	coalesce(credit_sale_ret_txn, 0) credit_sale_ret_txn, 
	coalesce(credit_sale_ret_custs, 0) credit_sale_ret_custs, 
	coalesce(new_cust_added, 0) new_cust_added, 
	coalesce(cust_txn_days, 0) cust_txn_days, 
	
	coalesce(cash_purchase_amt, 0) cash_purchase_amt, 
	coalesce(cash_purchase_txn, 0) cash_purchase_txn, 
	coalesce(cash_purchase_supps, 0) cash_purchase_supps, 
	coalesce(credit_purchase_amt, 0) credit_purchase_amt, 
	coalesce(credit_purchase_txn, 0) credit_purchase_txn, 
	coalesce(credit_purchase_supps, 0) credit_purchase_supps, 
	coalesce(credit_purchase_ret_amt, 0) credit_purchase_ret_amt, 
	coalesce(credit_purchase_ret_txn, 0) credit_purchase_ret_txn, 
	coalesce(credit_purchase_ret_supps, 0) credit_purchase_ret_supps, 
	coalesce(new_supp_added, 0) new_supp_added,
	coalesce(supp_txn_days, 0) supp_txn_days, 
	
	coalesce(malik_dilo_amt, 0) malik_dilo_amt, 
	coalesce(malik_dilo_txn, 0) malik_dilo_txn, 
	coalesce(malik_nilo_amt, 0) malik_nilo_amt, 
	coalesce(malik_nilo_txn, 0) malik_nilo_txn, 
	coalesce(malik_txn_days, 0) malik_txn_days, 
	
	coalesce(cash_adj_amt, 0) cash_adj_amt, 
	coalesce(cash_adj_txn, 0) cash_adj_txn, 
	coalesce(expense_amt, 0) expense_amt, 
	coalesce(expense_txn, 0) expense_txn, 
	coalesce(dokan_txn_days, 0) dokan_txn_days, 
	
	coalesce(sale_ticket, 0) sale_ticket,
	coalesce(cash_sale_ticket, 0) cash_sale_ticket,
	coalesce(credit_sale_ticket, 0) credit_sale_ticket,
	coalesce(purchase_ticket, 0) purchase_ticket, 
	coalesce(cash_purchase_ticket, 0) cash_purchase_ticket,
	coalesce(ops_ticket, 0) ops_ticket,
	
	-- sms features
	coalesce(sms_used, 0) sms_used, 
	coalesce(tagada_sms_used, 0) tagada_sms_used, 
	coalesce(link_sms_used, 0) link_sms_used, 
	coalesce(sms_used_days, 0) sms_used_days, 
	
	-- cust/supp+ features
	coalesce(all_cust_added, 0) all_cust_added, 
	coalesce(all_supp_added, 0) all_supp_added, 
	
	-- TK features
	coalesce(versions_exp, 0) versions_exp, 
	coalesce(version_now, 0) version_now, 
	coalesce(days_with_tk, 0) days_with_tk, 
	coalesce(dist_from_dhaka, 0) dist_from_dhaka, 
	coalesce(sec_spent, 0) sec_spent,
	
	-- identifier/other info
	mobile_no, shop_name
from 
	data_vajapora.help_a tbl1
	left join 
	data_vajapora.help_b tbl2 using(mobile_no)
	left join 
	data_vajapora.help_c tbl3 using(mobile_no)
	left join 
	data_vajapora.help_d tbl4 using(mobile_no)
	left join 
	data_vajapora.help_e tbl5 using(mobile_no)
	left join 
	(select mobile mobile_no, data_vajapora.lat_long_dist_meters(23.8103, 90.4125, lat::numeric, lng::numeric) dist_from_dhaka
	from tallykhata.tallykhata_clients_location_info
	) tbl6 using(mobile_no)
	left join 
	(select mobile_no, sum(sec_with_tk) sec_spent
	from tallykhata.daily_times_spent_individual_data
	where event_date>=current_date-21 and event_date<current_date 
	group by 1
	) tbl7 using(mobile_no)
order by random();