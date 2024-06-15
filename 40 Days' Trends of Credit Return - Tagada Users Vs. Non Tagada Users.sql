/*
- Viz: https://docs.google.com/spreadsheets/d/1NQ17yJjfX3j9ubvatCGh_JWl4urwwGtQBH4xmIoga0I/edit?pli=1#gid=1577059738
- Data: 
- Function: 
- Table:
- File: 
- Presentation: 
- Email thread: 
- Notes (if any): Mature (TK age>=90 days) Tagada users' metrics (in terms of txns, users, volume) are prepared for the last 40 days.
*/

-- for users who used Tagada-feature
drop table if exists data_vajapora.help_a;
create table  data_vajapora.help_a as
select *
from 
	(-- users who have sent Tagada to >=10 custs
	select merchant_mobile, count(distinct customer_mobile_no) tagada_sent_to_custs
	from public.notification_tagadasms 
	where 
		customer_mobile_no!=merchant_mobile
		and date(created_at)>='2021-05-09'
	group by 1
	having count(distinct customer_mobile_no)>=10
	) tbl1 
	
	inner join 
	
	(-- users more than 90 days' old
	select mobile merchant_mobile, current_date-registration_date tk_age_days
	from tallykhata.tallykhata_user_personal_info
	where current_date-registration_date>=90
	) tbl2 using(merchant_mobile)
	
	inner join 
		
	(-- last 40 days' credit/return
	select mobile_no merchant_mobile, txn_type, auto_id, created_datetime, cleaned_amount, input_amount
	from tallykhata.tallykhata_fact_info_final 
	where 
		txn_type in('CREDIT_SALE', 'CREDIT_SALE_RETURN')
		and created_datetime>=current_date-40 and created_datetime<current_date
	) tbl3 using(merchant_mobile); 

-- for users who did not use Tagada-feature
drop table if exists data_vajapora.help_b;
create table  data_vajapora.help_b as
select *
from
	(-- users more than 90 days' old
	select mobile merchant_mobile, current_date-registration_date tk_age_days
	from tallykhata.tallykhata_user_personal_info
	where current_date-registration_date>=90 
	order by random() 
	limit 450000 -- tuned to keep cred_sale_users within 5k to 6k
	) tbl2
	
	inner join 
		
	(-- last 40 days' credit/return
	select mobile_no merchant_mobile, txn_type, auto_id, created_datetime, cleaned_amount, input_amount
	from tallykhata.tallykhata_fact_info_final 
	where 
		txn_type in('CREDIT_SALE', 'CREDIT_SALE_RETURN')
		and created_datetime>=current_date-40 and created_datetime<current_date
	) tbl3 using(merchant_mobile)
	
	left join 
	
	(-- users who have sent Tagada to >=1 custs
	select merchant_mobile, count(distinct customer_mobile_no) tagada_sent_to_custs
	from public.notification_tagadasms 
	where 
		customer_mobile_no!=merchant_mobile
		and date(created_at)>='2021-05-09'
	group by 1
	having count(distinct customer_mobile_no)>=1
	) tbl1 using(merchant_mobile)
where tbl1.merchant_mobile is null; 

-- daily credit/return metrics (check if spikes show up after 09-May-21)
select 
	created_datetime, 
	
	count(case when txn_type='CREDIT_SALE' then auto_id else null end) cred_sale_trt,
	count(distinct case when txn_type='CREDIT_SALE' then merchant_mobile else null end) cred_sale_users,
	count(case when txn_type='CREDIT_SALE' then auto_id else null end)*1.00/
	count(distinct case when txn_type='CREDIT_SALE' then merchant_mobile else null end) cred_sale_rate,
	sum(case when txn_type='CREDIT_SALE' then cleaned_amount else null end) total_cred_sale_vol,
	avg(case when txn_type='CREDIT_SALE' then cleaned_amount else null end) avg_cred_sale_vol,
	
	count(case when txn_type='CREDIT_SALE_RETURN' then auto_id else null end) cred_ret_trt,
	count(distinct case when txn_type='CREDIT_SALE_RETURN' then merchant_mobile else null end) cred_sale_ret_users,
	count(case when txn_type='CREDIT_SALE_RETURN' then auto_id else null end)*1.00/
	count(distinct case when txn_type='CREDIT_SALE_RETURN' then merchant_mobile else null end) cred_sale_ret_rate,
	sum(case when txn_type='CREDIT_SALE_RETURN' then cleaned_amount else null end) total_cred_sale_ret_vol,
	avg(case when txn_type='CREDIT_SALE_RETURN' then cleaned_amount else null end) avg_cred_sale_ret_vol
from data_vajapora.help_a -- change
group by 1
order by 1 asc; 
