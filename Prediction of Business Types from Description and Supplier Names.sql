
-- doc: https://docs.google.com/document/d/1paGxjhraczvMV98UhYToZ0hMP513csSIeo0w-hvQbfc/edit

-- from sale descriptions
-- code: https://colab.research.google.com/drive/1ZGSoxaVjJb9uU-ksZJ1HRPzoYfhdfV9h?authuser=1
-- filename: type_pred_desc.ipynb
drop table if exists data_vajapora.help_f; 
create table data_vajapora.help_f as
select mobile_no, shop_name, business_type, description
from 
	(select id jr_id, mobile_no, description
	from public.journal 
	where 
		date(create_date)>=current_date-7 and date(create_date)<current_date
		and description is not null 
		and description!=''
	) tbl1 
	
	inner join 
			
	(select mobile mobile_no, max(business_type) business_type, max(coalesce(shop_name, business_name, name, merchant_name)) shop_name
	from tallykhata.tallykhata_user_personal_info
	where business_type in('GROCERY', 'PHARMACY', 'ELECTRONICS', 'MFS_MOBILE_RECHARGE', 'CLOTH_STORE', 'STATIONERY', 'HARDWARE')
	group by 1
	) tbl2 using(mobile_no) 
	
	inner join 
	
	(select jr_ac_id jr_id 
	from tallykhata.tallykhata_fact_info_final 
	where 
		txn_type like '%SALE%' 
		or txn_type like '%PURCHASE%'
	) tbl3 using(jr_id); 

select 
	replace(business_type, '_', '') category, 
	clean_description "text"
from 
	(select  
		mobile_no, 
		shop_name, 
		business_type, 
		string_agg(
			distinct trim(
				regexp_replace(
					regexp_replace(
						translate(description, '০১২৩৪৫৬৭৮৯', '0123456789'
						), '[^[:alpha:]]', ' ', 'g'
					), 
				'\s+', ' ', 'g'
				)
			), ' '
		) clean_description  
	from data_vajapora.help_f
	where description !~* '[a-z]' 
	group by 1, 2, 3
	) tbl1 
where clean_description!='' 
order by random(); 

-- from supplier names
-- code: https://colab.research.google.com/drive/1DsZA6FZJQpzPiFd5oY-j-kBtifoYwhQt?authuser=1
-- filename: type_pred_supp_name.ipynb
drop table if exists data_vajapora.help_f; 
create table data_vajapora.help_f as
select mobile_no, shop_name, business_type, clean_supp_name
from 
	(select 
		mobile_no, 
		string_agg(
			distinct trim(
				regexp_replace(
					regexp_replace(
						translate(lower(name), '০১২৩৪৫৬৭৮৯', '0123456789'
						), '[^[:alpha:]]', ' ', 'g'
					), 
				'\s+', ' ', 'g'
				)
			), ' '
		) clean_supp_name
	from public.account
	where 
		is_active is true 
		and type=3 
	group by 1
	) tbl1 

	inner join 
			
	(select 
		mobile mobile_no, 
		max(business_type) business_type, 
		max(coalesce(shop_name, business_name, name, merchant_name)) shop_name
	from tallykhata.tallykhata_user_personal_info
	where business_type in
		('GROCERY', 'PHARMACY', 'ELECTRONICS', 'MFS_MOBILE_RECHARGE', 
		'CLOTH_STORE', 'STATIONERY', 'HARDWARE'
		)
	group by 1
	) tbl2 using(mobile_no);

select replace(business_type, '_', '') category, clean_supp_name "text"
from data_vajapora.help_f 
where clean_supp_name!='' 
order by random();