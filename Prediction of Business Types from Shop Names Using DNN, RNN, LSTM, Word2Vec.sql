-- Drive workspace: https://drive.google.com/drive/u/1/folders/1-5DW-8eUct_kqcI4PSpa_gkb8feVl97t

-- Version-02 
-- model: https://colab.research.google.com/drive/1Rlc50rLnG3NpYNT7I8ydHv0dcvAqfXHg?authuser=1#scrollTo=8dEj3ugelXd1
-- filename: experiments_2.ipynb

drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select lower(regexp_replace(coalesce(shop_name, business_name, name, merchant_name), '[[:punct:]]', '', 'g')) shop_name, business_type category
from tallykhata.tallykhata_user_personal_info
where 
	business_type in('GROCERY', 'PHARMACY', 'ELECTRONICS', 'MFS_MOBILE_RECHARGE', 'CLOTH_STORE', 'STATIONERY', 'HARDWARE')
	and replace(regexp_replace(coalesce(shop_name, business_name, name, merchant_name), '[[:punct:]]', '', 'g'), ' ', '') ~* '\A[A-Z]*\Z'; 

select 
	category, 
	ascii(substring(lpad(right(shop_name, 30), 30) from 1 for 1)) char_1,
	ascii(substring(lpad(right(shop_name, 30), 30) from 2 for 1)) char_2,
	ascii(substring(lpad(right(shop_name, 30), 30) from 3 for 1)) char_3,
	ascii(substring(lpad(right(shop_name, 30), 30) from 4 for 1)) char_4,
	ascii(substring(lpad(right(shop_name, 30), 30) from 5 for 1)) char_5,
	ascii(substring(lpad(right(shop_name, 30), 30) from 6 for 1)) char_6,
	ascii(substring(lpad(right(shop_name, 30), 30) from 7 for 1)) char_7,
	ascii(substring(lpad(right(shop_name, 30), 30) from 8 for 1)) char_8,
	ascii(substring(lpad(right(shop_name, 30), 30) from 9 for 1)) char_9,
	ascii(substring(lpad(right(shop_name, 30), 30) from 10 for 1)) char_10,
	ascii(substring(lpad(right(shop_name, 30), 30) from 11 for 1)) char_11,
	ascii(substring(lpad(right(shop_name, 30), 30) from 12 for 1)) char_12,
	ascii(substring(lpad(right(shop_name, 30), 30) from 13 for 1)) char_13,
	ascii(substring(lpad(right(shop_name, 30), 30) from 14 for 1)) char_14,
	ascii(substring(lpad(right(shop_name, 30), 30) from 15 for 1)) char_15,
	ascii(substring(lpad(right(shop_name, 30), 30) from 16 for 1)) char_16,
	ascii(substring(lpad(right(shop_name, 30), 30) from 17 for 1)) char_17,
	ascii(substring(lpad(right(shop_name, 30), 30) from 18 for 1)) char_18,
	ascii(substring(lpad(right(shop_name, 30), 30) from 19 for 1)) char_19,
	ascii(substring(lpad(right(shop_name, 30), 30) from 20 for 1)) char_20,
	ascii(substring(lpad(right(shop_name, 30), 30) from 21 for 1)) char_21,
	ascii(substring(lpad(right(shop_name, 30), 30) from 22 for 1)) char_22,
	ascii(substring(lpad(right(shop_name, 30), 30) from 23 for 1)) char_23,
	ascii(substring(lpad(right(shop_name, 30), 30) from 24 for 1)) char_24,
	ascii(substring(lpad(right(shop_name, 30), 30) from 25 for 1)) char_25,
	ascii(substring(lpad(right(shop_name, 30), 30) from 26 for 1)) char_26,
	ascii(substring(lpad(right(shop_name, 30), 30) from 27 for 1)) char_27,
	ascii(substring(lpad(right(shop_name, 30), 30) from 28 for 1)) char_28,
	ascii(substring(lpad(right(shop_name, 30), 30) from 29 for 1)) char_29,
	ascii(substring(lpad(right(shop_name, 30), 30) from 30 for 1)) char_30, 
	shop_name
from data_vajapora.help_a
order by random()
limit 69000; 

-- Version-03
-- model: https://colab.research.google.com/drive/1bKFwLGjXG6uWdI4JZqfVBMqljYxH_o9j?authuser=1#scrollTo=rkaYz4axLo2X
-- filename: type_pred_01.ipynb

select 
	replace(business_type, '_', '') category,  
	lower(regexp_replace(coalesce(shop_name, business_name, name, merchant_name), '[[:punct:]]', ' ', 'g')) "text"
from tallykhata.tallykhata_user_personal_info
where business_type in('GROCERY', 'PHARMACY', 'ELECTRONICS', 'MFS_MOBILE_RECHARGE', 'CLOTH_STORE', 'STATIONERY', 'HARDWARE')
order by random() 
limit 190000; 