/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1530713004
- Data: 
- Function: 
- Table:
- File: 
- Presentation: 
- Email thread: 
- Notes (if any): 
*/

-- daily metrics
select 
	tagada_date, 
	
	count(distinct tbl1.merchant_mobile) merchants_sent_tagada,
	count(distinct case when supplier_mobile is not null then tbl1.merchant_mobile else null end) merchants_sent_tagada_to_suppliers,
	count(distinct case when supplier_mobile is not null then tbl1.merchant_mobile else null end)*1.00/count(distinct tbl1.merchant_mobile) merchants_sent_tagada_to_suppliers_pct,
	
	count(distinct tbl1.id) tagada_sent,
	count(distinct case when supplier_mobile is not null then tbl1.id else null end) tagada_sent_to_suppliers,
	count(distinct case when supplier_mobile is not null then tbl1.id else null end)*1.00/count(distinct tbl1.id) tagada_sent_to_suppliers_pct,
	
	count(distinct tbl1.customer_mobile_no) people_got_tagada,
	count(distinct case when supplier_mobile is not null then tbl1.customer_mobile_no else null end) suppliers_got_tagada,
	count(distinct case when supplier_mobile is not null then tbl1.customer_mobile_no else null end)*1.00/count(distinct tbl1.customer_mobile_no) suppliers_got_tagada_pct
from 
	(select id, amount, customer_mobile_no, merchant_mobile, date(created_at) tagada_date 
	from public.notification_tagadasms 
	) tbl1 
	
	left join 
	
	(select distinct mobile_no merchant_mobile, contact supplier_mobile
	from public.account 
	where 
		type=3
		and left(contact, 3) not in('010', '011', '012')
	) tbl2 on(tbl1.merchant_mobile=tbl2.merchant_mobile and tbl1.customer_mobile_no=tbl2.supplier_mobile)
group by 1 
order by 1 asc; 

-- monthly metrics
select 
	concat(date_part('year', tagada_date), '-', date_part('month', tagada_date)) year_month,
	
	count(distinct tbl1.merchant_mobile) merchants_sent_tagada,
	count(distinct case when supplier_mobile is not null then tbl1.merchant_mobile else null end) merchants_sent_tagada_to_suppliers,
	count(distinct case when supplier_mobile is not null then tbl1.merchant_mobile else null end)*1.00/count(distinct tbl1.merchant_mobile) merchants_sent_tagada_to_suppliers_pct,
	
	count(distinct tbl1.id) tagada_sent,
	count(distinct case when supplier_mobile is not null then tbl1.id else null end) tagada_sent_to_suppliers,
	count(distinct case when supplier_mobile is not null then tbl1.id else null end)*1.00/count(distinct tbl1.id) tagada_sent_to_suppliers_pct,
	
	count(distinct tbl1.customer_mobile_no) people_got_tagada,
	count(distinct case when supplier_mobile is not null then tbl1.customer_mobile_no else null end) suppliers_got_tagada,
	count(distinct case when supplier_mobile is not null then tbl1.customer_mobile_no else null end)*1.00/count(distinct tbl1.customer_mobile_no) suppliers_got_tagada_pct
from 
	(select id, amount, customer_mobile_no, merchant_mobile, date(created_at) tagada_date 
	from public.notification_tagadasms 
	) tbl1 
	
	left join 
	
	(select distinct mobile_no merchant_mobile, contact supplier_mobile
	from public.account 
	where 
		type=3
		and left(contact, 3) not in('010', '011', '012')
	) tbl2 on(tbl1.merchant_mobile=tbl2.merchant_mobile and tbl1.customer_mobile_no=tbl2.supplier_mobile)
group by 1
order by 1 asc; 

-- summary metrics
select 
	count(distinct tbl1.merchant_mobile) merchants_sent_tagada_to_suppliers,
	count(distinct supplier_mobile) suppliers_got_tagada_from_merchants,
	count(distinct id) merchant_to_supplier_tagada_sms
from 
	(select id, amount, customer_mobile_no, merchant_mobile, date(created_at) tagada_date 
	from public.notification_tagadasms 
	) tbl1 
	
	inner join 
	
	(select distinct mobile_no merchant_mobile, contact supplier_mobile
	from public.account 
	where 
		type=3
		and left(contact, 3) not in('010', '011', '012')
	) tbl2 on(tbl1.merchant_mobile=tbl2.merchant_mobile and tbl1.customer_mobile_no=tbl2.supplier_mobile); 
	
