/*
- Viz: https://docs.google.com/spreadsheets/d/1NQ17yJjfX3j9ubvatCGh_JWl4urwwGtQBH4xmIoga0I/edit?pli=1#gid=1577059738
- Data: 
- Function: 
- Table:
- File: 
- Presentation: 
- Email thread: 
- Notes (if any): 

I tried to analyse: How fast are CREDIT_SALEs being returned after shooting Tagada-SMS?
For this, I took 51.5k merchants who used Tagada and recorded CREDIT_SALE_RETURNs with the same customers after the Tagada-SMS was shot. Findings:
- 25% credit deals are closed within 20 hours
- 50% credit deals are closed within 45 hours
- 70% credit deals are closed within 72 hours
NB: CREDIT_SALEs returned within 5 mins are excluded from the analysis, since users may be experimenting in such cases.

*/

drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select  
	date_part('day', cred_ret_timestamp-tagada_sms_timestamp)*24+date_part('hour', cred_ret_timestamp-tagada_sms_timestamp) hours_to_ret_cred,
	count(distinct merchant_mobile) merchants
from 
	(select created_at tagada_sms_timestamp, customer_mobile_no cust_mobile, merchant_mobile
	from public.notification_tagadasms 
	where customer_mobile_no!=merchant_mobile
	) tbl1 
	
	inner join 
	
	(select mobile_no merchant_mobile, contact cust_mobile, created_timestamp cred_ret_timestamp
	from tallykhata.tallykhata_fact_info_final
	where 
		txn_type='CREDIT_SALE_RETURN'
		and created_datetime>='2021-05-09' and created_datetime<=current_date
	) tbl2 using(merchant_mobile, cust_mobile) 
where 
	tagada_sms_timestamp<cred_ret_timestamp
	and 
	date_part('day', cred_ret_timestamp-tagada_sms_timestamp)*24*60
	+date_part('hour', cred_ret_timestamp-tagada_sms_timestamp)*60 
	+date_part('minute', cred_ret_timestamp-tagada_sms_timestamp)>=5
group by 1; 

select 
	tbl1.hours_to_ret_cred,
	tbl1.merchants,
	total_merchants,
	sum(tbl2.merchants) cum_merchants,
	sum(tbl2.merchants)/total_merchants cum_merchants_pct
from 
	(select hours_to_ret_cred, merchants
	from data_vajapora.help_a
	) tbl1
	
	inner join 
	
	(select hours_to_ret_cred, merchants
	from data_vajapora.help_a
	) tbl2 on(tbl1.hours_to_ret_cred>=tbl2.hours_to_ret_cred),
	
	(select sum(merchants) total_merchants
	from data_vajapora.help_a
	) tbl3
group by 1, 2, 3
order by 1; 