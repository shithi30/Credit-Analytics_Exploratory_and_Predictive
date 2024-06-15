/*
- Viz: https://docs.google.com/spreadsheets/d/1NQ17yJjfX3j9ubvatCGh_JWl4urwwGtQBH4xmIoga0I/edit?pli=1#gid=1041052901
- Data: 
- Function: 
- Table:
- File: 
- Presentation: 
- Email thread: 
- Notes (if any): 

I have examined effectiveness of Tagada SMS for different BI-types from 4 perspectives: 
- Pct of merchants who got their credit back
- Pct of customers who returned credit
- ratio of CREDIT_SALE_RETURN-txns to Tagadas sent
- ratio of amount sent Tagadas for, to amount retrieved

*/

drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select *
from 
	(select created_at tagada_sms_timestamp, customer_mobile_no cust_mobile, merchant_mobile, amount tagada_amount, id tagada_id
	from public.notification_tagadasms 
	where 
		customer_mobile_no!=merchant_mobile
		and date(created_at)>='2021-05-09'
	) tbl1 
	
	inner join 
	
	(select mobile merchant_mobile, new_bi_business_type 
	from tallykhata.tallykhata_user_personal_info
	) tbl0 using(merchant_mobile)
	
	left join 
	
	(select mobile_no merchant_mobile, contact cust_mobile, created_timestamp cred_ret_timestamp, cleaned_amount returned_amount, auto_id 
	from tallykhata.tallykhata_fact_info_final
	where 
		txn_type='CREDIT_SALE_RETURN'
		and created_datetime>='2021-05-09' and created_datetime<=current_date
	) tbl2 using(merchant_mobile, cust_mobile)
where 
	(tagada_sms_timestamp<cred_ret_timestamp
	and 
	date_part('day', cred_ret_timestamp-tagada_sms_timestamp)*24*60
	+date_part('hour', cred_ret_timestamp-tagada_sms_timestamp)*60 
	+date_part('minute', cred_ret_timestamp-tagada_sms_timestamp)>=5
	) 
	or cred_ret_timestamp is null
order by 1, 2; 

select 
	/*-- refining BI-type names
	case 
		when concat(lower(split_part(bi_business_type, ' ', 1)), ' ', lower(split_part(bi_business_type, ' ', 2))) like '%(%' 
		then replace(concat(lower(split_part(bi_business_type, ' ', 1)), ' ', lower(split_part(bi_business_type, ' ', 2))), '(', '')
		
		when concat(lower(split_part(bi_business_type, ' ', 1)), ' ', lower(split_part(bi_business_type, ' ', 2))) like '%and%' 
		then replace(concat(lower(split_part(bi_business_type, ' ', 1)), ' ', lower(split_part(bi_business_type, ' ', 2))), 'and', '')
		
		else concat(lower(split_part(bi_business_type, ' ', 1)), ' ', lower(split_part(bi_business_type, ' ', 2)))
	end business_type,*/

	new_bi_business_type,
	
	tagada_merchants, merchants_returned, merchants_returned*1.00/tagada_merchants merchants_returned_pct,
	tagada_custs, custs_returned, custs_returned*1.00/tagada_custs custs_returned_pct,
	tagada_sms, cred_ret_txns, cred_ret_txns*1.00/tagada_sms cred_ret_txns_pct,
	tagada_sum, returned_sum, returned_sum*1.00/tagada_sum returned_sum_pct
from 
	(select 
		new_bi_business_type,
		count(distinct merchant_mobile) tagada_merchants,
		count(distinct cust_mobile) tagada_custs,
		count(distinct case when auto_id is not null then merchant_mobile else null end) merchants_returned,
		count(distinct case when auto_id is not null then cust_mobile else null end) custs_returned,
		count(distinct tagada_id) tagada_sms,
		count(distinct auto_id) cred_ret_txns
	from data_vajapora.help_a tbl2
	group by 1
	) tbl1
	
	inner join 
	
	(select new_bi_business_type, round(sum(tagada_amount)) tagada_sum
	from 
		(select distinct tagada_amount, tagada_id, new_bi_business_type
		from data_vajapora.help_a
		) tbl1
	group by 1
	) tbl2 using(new_bi_business_type)
	
	inner join 
	
	(select new_bi_business_type, round(sum(returned_amount)) returned_sum 
	from 
		(select distinct returned_amount, auto_id, new_bi_business_type
		from data_vajapora.help_a
		) tbl1 
	group by 1
	) tbl3 using(new_bi_business_type); 
