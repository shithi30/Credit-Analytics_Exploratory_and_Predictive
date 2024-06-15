/*
- Viz: 
	- daily: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1315368376
	- monthly 1 (me): https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1945005861
	- monthly 2 (Md. Nazrul Islam): https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1176953729
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
*/

/* for daily analysis */

drop table if exists data_vajapora.help_b;
create table  data_vajapora.help_b as
select *
from
	(-- BI-users more than 70 days' old
	select mobile merchant_mobile, current_date-registration_date tk_age_days, new_bi_business_type
	from tallykhata.tallykhata_user_personal_info
	where current_date-registration_date>=70
	) tbl1
	
	inner join 
		
	(-- last 40 days' credit/return
	select mobile_no merchant_mobile, txn_type, created_datetime, cleaned_amount
	from tallykhata.tallykhata_fact_info_final 
	where 
		txn_type in('CREDIT_SALE', 'CREDIT_SALE_RETURN')
		and created_datetime>=current_date-40 and created_datetime<current_date
	) tbl2 using(merchant_mobile); 

-- BI-type wise daily credit/return metrics									
select									
	created_datetime,								
									
	sum(case when txn_type='CREDIT_SALE_RETURN' then cleaned_amount else 0 end)*1.00/								
	sum(case when txn_type='CREDIT_SALE' then cleaned_amount else 0 end) cred_sale_ret_pct_overall,								
									
	sum(case when txn_type='CREDIT_SALE_RETURN' and new_bi_business_type='Grocery' then cleaned_amount else 0 end)*1.00/								
	sum(case when txn_type='CREDIT_SALE' and new_bi_business_type='Grocery' then cleaned_amount else 0 end) cred_sale_ret_pct_grocery,								
									
	sum(case when txn_type='CREDIT_SALE_RETURN' and new_bi_business_type='Pharmacy' then cleaned_amount else 0 end)*1.00/								
	sum(case when txn_type='CREDIT_SALE' and new_bi_business_type='Pharmacy' then cleaned_amount else 0 end) cred_sale_ret_pct_pharmacy,								
									
	sum(case when txn_type='CREDIT_SALE_RETURN' and new_bi_business_type='Electronics Store' then cleaned_amount else 0 end)*1.00/								
	sum(case when txn_type='CREDIT_SALE' and new_bi_business_type='Electronics Store' then cleaned_amount else 0 end) cred_sale_ret_pct_electronics,								
									
	sum(case when txn_type='CREDIT_SALE_RETURN' and new_bi_business_type='Fabrics and Cloths' then cleaned_amount else 0 end)*1.00/								
	sum(case when txn_type='CREDIT_SALE' and new_bi_business_type='Fabrics and Cloths' then cleaned_amount else 0 end) cred_sale_ret_pct_fabrics,								
									
	sum(case when txn_type='CREDIT_SALE_RETURN' and new_bi_business_type='Market & Supershop' then cleaned_amount else 0 end)*1.00/								
	sum(case when txn_type='CREDIT_SALE' and new_bi_business_type='Market & Supershop' then cleaned_amount else 0 end) cred_sale_ret_pct_supershop,								
									
	sum(case when txn_type='CREDIT_SALE_RETURN' and new_bi_business_type='MFS-Mobile Recharge Store' then cleaned_amount else 0 end)*1.00/								
	sum(case when txn_type='CREDIT_SALE' and new_bi_business_type='MFS-Mobile Recharge Store' then cleaned_amount else 0 end) cred_sale_ret_pct_recharge,								
									
	sum(case when txn_type='CREDIT_SALE_RETURN' and new_bi_business_type ilike '%Wholes%' then cleaned_amount else 0 end)*1.00/								
	sum(case when txn_type='CREDIT_SALE' and new_bi_business_type ilike '%Wholes%' then cleaned_amount else 0 end) cred_sale_ret_pct_wholeseller								
									
from data_vajapora.help_b									
group by 1									
order by 1 asc;									
																	
-- BI-type wise daily credit/return metrics: to pivot								
select									
	created_datetime,								
	concat(lower(new_bi_business_type), ' credit return vol. pct') new_bi_business_type,								
	sum(case when txn_type='CREDIT_SALE_RETURN' then cleaned_amount else 0 end)*1.00/								
	sum(case when txn_type='CREDIT_SALE' then cleaned_amount else 0 end) cred_sale_ret_pct							
from data_vajapora.help_b									
group by 1, 2	
having sum(case when txn_type='CREDIT_SALE' then cleaned_amount else 0 end)!=0
order by 1 asc;	

/* for monthly analysis */

drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select *
from
	(-- BI-users
	select mobile merchant_mobile, new_bi_business_type
	from tallykhata.tallykhata_user_personal_info
	) tbl1
	
	inner join 
		
	(-- 3 months' credit/return
	select mobile_no merchant_mobile, txn_type, created_datetime, cleaned_amount
	from tallykhata.tallykhata_fact_info_final 
	where 
		txn_type in('CREDIT_SALE', 'CREDIT_SALE_RETURN')
		and created_datetime>='2021-05-01' and created_datetime<'2021-08-01'
	) tbl2 using(merchant_mobile); 

-- BI-type wise monthly credit/return metrics: to pivot						
select									
	to_char(created_datetime, 'YYYY-MM') year_month,										
	new_bi_business_type,					
	sum(case when txn_type='CREDIT_SALE_RETURN' then cleaned_amount else 0 end)*1.00/								
	sum(case when txn_type='CREDIT_SALE' then cleaned_amount else 0 end) cred_sale_ret_pct							
from data_vajapora.help_b									
group by 1, 2	
having sum(case when txn_type='CREDIT_SALE' then cleaned_amount else 0 end)!=0
order by 1 asc;	
									
									