/*
- Viz: https://docs.google.com/spreadsheets/d/1ixqudNf1dKAk-0ww8wQ2V4wujxp4vrIUxpVZixzu-BU/edit#gid=1681939131
- Data: 
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
	A weekly Tagada consumption analysis shows the following tendencies: 
	- Merchants consume the most Tagada on Thursdays (15.95%), followed by Wednesdays (14.95%) and Tuesdays (14.36%). 
	- The greatest consumers are Grocery, Pharmacy, MFS, Electronics, Wholesalers, respectively. 
*/

-- biz-types (Mahmud)
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select 
	mobile_no, 
	case 
		when business_type in('BAKERY_AND_CONFECTIONERY') then 'SWEETS AND CONFECTIONARY'
		when business_type in('ELECTRONICS') then 'ELECTRONICS STORE'
		when business_type in('MFS_AGENT','MFS_MOBILE_RECHARGE') then 'MFS-MOBILE RECHARGE STORE'
		when business_type in('GROCERY') then 'GROCERY'
		when business_type in('DISTRIBUTOR_OR_WHOLESALE','WHOLESALER','DEALER') then 'OTHER WHOLESELLER'
		when business_type in('HOUSEHOLD_AND_FURNITURE') then 'FURNITURE SHOP'
		when business_type in('STATIONERY') then 'STATIONARY BUSINESS'
		when business_type in('TAILORS') then 'TAILERS'
		when business_type in('PHARMACY') then 'PHARMACY'
		when business_type in('SHOE_STORE') then 'SHOE STORE'
		when business_type in('MOTOR_REPAIR') then 'VEHICLE-CAR SERVICING'
		when business_type in('COSMETICS') then 'COSMETICS AND PERLOUR'
		when business_type in('ROD_CEMENT') then 'CONSTRUCTION RAW MATERIAL'
		when business_type='' then upper(case when new_bi_business_type!='Other Business' then new_bi_business_type else null end) 
		else null 
	end biz_type
from 
	(select id, business_type 
	from public.register_tallykhatauser 
	) tbl1 
	
	inner join 
	
	(select mobile_no, max(id) id
	from public.register_tallykhatauser 
	group by 1
	) tbl2 using(id)
	
	inner join 
	
	(select mobile mobile_no, max(new_bi_business_type) new_bi_business_type
	from tallykhata.tallykhata_user_personal_info 
	group by 1
	) tbl3 using(mobile_no); 

-- daily stats
select 
	concat(date(create_date), ' ', left(to_char(date(create_date), 'Day'), 3)) tagada_date_weekday,
	count(id) tagada_consumed, 
	count(case when biz_type='GROCERY' then id else null end) tagada_consumed_grocery, 
	count(case when biz_type='MFS-MOBILE RECHARGE STORE' then id else null end) tagada_consumed_mfs, 
	count(case when biz_type='PHARMACY' then id else null end) tagada_consumed_pharmacy, 
	count(case when biz_type='OTHER WHOLESELLER' then id else null end) tagada_consumed_whosale, 
	count(case when biz_type='ELECTRONICS STORE' then id else null end) tagada_consumed_electronics, 
	count(case when biz_type='TEA-COFFEE STORE' then id else null end) tagada_consumed_teacoffe, 
	count(case when biz_type='VEHICLE-CAR SERVICING' then id else null end) tagada_consumed_vehicle, 
	count(case when biz_type='SWEETS AND CONFECTIONARY' then id else null end) tagada_consumed_confectionary, 
	count(case when biz_type not in(
		'GROCERY', 'MFS-MOBILE RECHARGE STORE', 'PHARMACY', 'OTHER WHOLESELLER', 
		'ELECTRONICS STORE', 'TEA-COFFEE STORE', 'VEHICLE-CAR SERVICING', 'SWEETS AND CONFECTIONARY')   
		or biz_type is null
		then id else null end
	) tagada_consumed_others
from 
	(select create_date, id, mobile_no
	from public.tagada_log 
	where 
		tagada_type in('TAGADA_BY_SMS', 'TAGADA_BY_FREE_SMS')
		and date(create_date)>=current_date-35 and date(create_date)<current_date 
	) tbl1 
	
	left join 
	
	data_vajapora.help_b tbl2 using(mobile_no)
group by 1
order by 1; 

-- weekly stats
select 
	to_char(date(create_date), 'YYYY-WW') year_week, 
	min(date(create_date)) week_start_date, 
	max(date(create_date)) week_end_date, 
	count(id) sms_consumed, 
	count(case when left(to_char(date(create_date), 'Day'), 3)='Sun' then id else null end) sms_consumed_sunday,
	count(case when left(to_char(date(create_date), 'Day'), 3)='Mon' then id else null end) sms_consumed_monday,
	count(case when left(to_char(date(create_date), 'Day'), 3)='Tue' then id else null end) sms_consumed_tuesday,
	count(case when left(to_char(date(create_date), 'Day'), 3)='Wed' then id else null end) sms_consumed_wednesday,
	count(case when left(to_char(date(create_date), 'Day'), 3)='Thu' then id else null end) sms_consumed_thursday,
	count(case when left(to_char(date(create_date), 'Day'), 3)='Fri' then id else null end) sms_consumed_friday,
	count(case when left(to_char(date(create_date), 'Day'), 3)='Sat' then id else null end) sms_consumed_saturday
from 
	(select create_date, id, mobile_no
	from public.tagada_log 
	where 
		tagada_type in('TAGADA_BY_SMS', 'TAGADA_BY_FREE_SMS')
		and date(create_date)>=current_date-77 and date(create_date)<current_date 
	) tbl1 
	
	left join 
	
	data_vajapora.help_b tbl2 using(mobile_no)
group by 1
order by 1;