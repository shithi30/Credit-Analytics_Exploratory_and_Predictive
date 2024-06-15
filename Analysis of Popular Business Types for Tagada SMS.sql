/*
- Viz: https://docs.google.com/spreadsheets/d/1NQ17yJjfX3j9ubvatCGh_JWl4urwwGtQBH4xmIoga0I/edit?pli=1#gid=1317634057
- Data: 
- Function: 
- Table:
- File: 
- Presentation: 
- Email thread: 
- Notes (if any):

I have identified top-05 BI-types for Tagada, on a daily basis. They are:
- Grocery
- MFS-Mobile Recharge Store
- Pharmacy
- Tea-Coffee Store
- Electronics Store
   
*/

-- 5 most popular choices, daily
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select tagada_date, popularity_seq, business_type, tagada_sms_sent, bi_type_sms_sent
from 
	(select 
		date(tagada_sms_timestamp) tagada_date, 
		business_type, 
		count(distinct id) tagada_sms_sent,
		concat(business_type, ': ', count(distinct id)) bi_type_sms_sent, 
		row_number() over(partition by date(tagada_sms_timestamp) order by count(distinct id) desc) popularity_seq
	from 
		(select created_at tagada_sms_timestamp, customer_mobile_no cust_mobile, merchant_mobile, id
		from public.notification_tagadasms 
		where 
			customer_mobile_no!=merchant_mobile
			and date(created_at)>='2021-05-09'
		) tbl1 
		
		inner join 
		
		(select mobile merchant_mobile, new_bi_business_type business_type
		from tallykhata.tallykhata_user_personal_info
		where new_bi_business_type!='Other Business' -- excluding unidentifiable BI-type
		) tbl2 using(merchant_mobile)
	group by 1, 2
	) tbl1
where popularity_seq<=5; 
select *
from data_vajapora.help_a; 
 
/*
-- top-05 most popular businesses
Grocery
MFS-Mobile Recharge Store
Pharmacy
Tea-Coffee Store
Electronics Store
*/

-- pivot table
select * 
from crosstab('select tagada_date, business_type, tagada_sms_sent::numeric from data_vajapora.help_a order by 1 asc, 3 desc') 
     as final_result(tagada_date date, grocery numeric, mfs_mobile_rercharge numeric, pharmacy numeric, tea_coffee numeric, electronics numeric);
    
-- popular choices, aggregated
select business_type, tagada_sms_sent, tagada_sms_sent_pct, popularity_seq
from 
	(select 
		business_type, total_tagada_sent,
		count(distinct id) tagada_sms_sent,
		count(distinct id)*1.00/total_tagada_sent tagada_sms_sent_pct,
		row_number() over(order by count(distinct id) desc) popularity_seq
	from 
		(select created_at tagada_sms_timestamp, customer_mobile_no cust_mobile, merchant_mobile, id
		from public.notification_tagadasms 
		where 
			customer_mobile_no!=merchant_mobile
			and date(created_at)>='2021-05-09'
		) tbl1 
		
		left join 
		
		(select mobile merchant_mobile, new_bi_business_type business_type
		from tallykhata.tallykhata_user_personal_info
		) tbl2 using(merchant_mobile),
		
		(select count(distinct id) total_tagada_sent
		from public.notification_tagadasms 
		where 
			customer_mobile_no!=merchant_mobile
			and date(created_at)>='2021-05-09'
		) tbl3
	group by 1, 2
	order by 5 asc
	) tbl1; 
		
-- for the rightmost table added by Md.Nazrul Islam
select tagada_date, count(distinct merchant_mobile) tagada_merchants, count(distinct id) sms
from 
	(select date tagada_date, id, merchant_mobile
	from notification_tagadasms
	where date>='2021-05-09' and date<=current_date
	) tbl1
group by 1
order by 1 asc; 

select count(distinct merchant_mobile) tagada_merchants, count(distinct id) sms
from 
	(select date tagada_date, id, merchant_mobile
	from notification_tagadasms
	where date>='2021-05-09' and date<=current_date
	) tbl1; 

