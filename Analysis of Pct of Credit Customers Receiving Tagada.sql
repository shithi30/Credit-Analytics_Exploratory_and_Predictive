/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1941202731
- Data: 
- Function: 
- Table:
- File: 
- Presentation: 
- Email thread: 
- Notes (if any): 

I think we can do a survey, how new merchants think about txn SMS and tagada message.
- Are they sending tagada to customers everyday / after every txn
- Are they ignoring txn SMS?
- What is the double message count per customers (txn+tagada)
Nazrul can help us digging down data. And we can ask PU who first join 3.0.

*/

select 
	reg_date, 
	date(date(reg_date)+interval '2 days') reg_date_plus_3_days,
	
	count(distinct tbl1.mobile_no) reg, 
	count(distinct tbl2.mobile_no) credit_txn_merchants,
	count(distinct contact) credit_txn_custs,
	count(distinct auto_id) credit_txns, 
	
	count(distinct merchant_mobile) tagada_merchants,
	count(distinct customer_mobile_no) tagada_custs, 
	count(distinct id) tagada_shot, 
	
	count(distinct customer_mobile_no)*1.00/count(distinct contact) pct_of_credit_custs_shot_tagada_against,
	count(distinct id)*1.00/count(distinct auto_id) pct_of_credit_txns_shot_tagada_against	
from 
	(select mobile_no, date(reg_datetime) reg_date, date(date(reg_datetime)+interval '2 days') reg_date_plus_3_days
	from data_vajapora.version_wise_days 
	where 
		app_version_name in('3.0.0') 
		and date(update_or_reg_datetime)=date(reg_datetime) 
		and date(reg_datetime)>='2021-05-10' and date(reg_datetime)<'2021-06-01'
	) tbl1 
	
	left join 
		
	(select auto_id, mobile_no, created_datetime, contact
	from tallykhata.tallykhata_fact_info_final 
	where txn_type='CREDIT_SALE'
	) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl2.created_datetime>=reg_date and tbl2.created_datetime<=reg_date_plus_3_days)
	
	left join 

	(select merchant_mobile, customer_mobile_no, date, id
	from public.notification_tagadasms 
	) tbl3 on(tbl1.mobile_no=tbl3.merchant_mobile and tbl3.date>=tbl1.reg_date and tbl3.date<=tbl1.reg_date_plus_3_days)
group by 1, 2 
order by 1 asc; 
