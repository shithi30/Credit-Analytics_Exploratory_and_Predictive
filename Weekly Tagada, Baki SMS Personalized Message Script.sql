/*
- Viz: 
- Data: 
- Function: 
- Table:
- Instructions: https://docs.google.com/spreadsheets/d/1cK9Ap07wlMeXmUk3QcoLVXUWpAVXuLJeCuHnSZbueKY/edit#gid=1954449328
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: New Personalized Message Copy SMS Count!
- Notes (if any): 
*/

CREATE OR REPLACE FUNCTION data_vajapora.fn_weekly_sms_stats()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

declare

begin
	-- this week's baki txns 
	drop table if exists data_vajapora.weekly_sms_stats_help;
	create table data_vajapora.weekly_sms_stats_help as
	select 
		mobile_no, 
		count(distinct case when txn_type in('CREDIT_SALE') then account_id else null end) ei_soptaher_baki_becha_customers,
		count(distinct case when txn_type in('CREDIT_SALE_RETURN') then account_id else null end) ei_soptaher_baki_aday_customers
	from 
		(select mobile_no, account_id, txn_type, journal_tbl_id jr_id
		from tallykhata.tallykhata_user_transaction_info
		where date(created_datetime)>=current_date-7 and date(created_datetime)<current_date
		) tbl1
		
		inner join 
			
		(select id jr_id
		from public.journal 
		where 
			is_active is true
			and date(create_date)>=current_date-7 and date(create_date)<current_date
		) tbl2 using(jr_id)
	group by 1; 
		
	-- this week's baki SMS 
	drop table if exists data_vajapora.weekly_sms_stats_help_2;
	create table data_vajapora.weekly_sms_stats_help_2 as
	select 
		translate(substring(message_body, length(message_body)-10, 12), '০১২৩৪৫৬৭৮৯', '0123456789') mobile_no, 
		mobile_no customer_mobile_no, 
		message_body, 
		case 
			when message_body like '%অনুগ্রহ করে%' then 'tagada'
			when message_body like '%বাকি%' and message_body like 'প্রিয় গ্রাহক%' then 'baki add'
			when message_body like '%বাকি%' and (message_body like 'পরিশোধ%' or message_body like 'দিলাম%' or message_body like 'কেনা%') then 'baki txn'
		end sms_type
	from public.t_scsms_message_archive_v2 as s
	where
		upper(s.channel) in('TALLYKHATA_TXN') 
		and upper(trim(s.bank_name)) = 'SURECASH'
		and s.telco_identifier_id in(66, 64, 61, 62, 49) 
		and upper(s.message_status) in ('SUCCESS', '0')
		and s.request_time::date>=current_date-7 and s.request_time::date<current_date; 
	
	drop table if exists data_vajapora.weekly_sms_stats_help_4;
	create table data_vajapora.weekly_sms_stats_help_4 as
	select 
		mobile_no, 
		count(distinct case when sms_type in('tagada') then customer_mobile_no else null end) customers_got_tagada_sms, 
		count(distinct case when sms_type in('baki add', 'baki txn') then customer_mobile_no else null end) customers_got_baki_sms
	from data_vajapora.weekly_sms_stats_help_2
	group by 1; 
	
	-- this week's statistics
	drop table if exists data_vajapora.weekly_sms_stats_help_3; 
	create table data_vajapora.weekly_sms_stats_help_3 as
	select 
		mobile_no,
		coalesce(ei_soptaher_baki_becha_customers, 0) ei_soptaher_baki_becha_customers,
		coalesce(ei_soptaher_baki_aday_customers, 0) ei_soptaher_baki_aday_customers,
		coalesce(customers_got_tagada_sms, 0) customers_got_tagada_sms,
		coalesce(customers_got_baki_sms, 0) customers_got_baki_sms
	from 
		(-- retained today
		select mobile_no 
		from cjm_segmentation.retained_users 
		where report_date=current_date
		) tbl1 
		
		left join 
		
		-- this week's baki txns 
		data_vajapora.weekly_sms_stats_help using(mobile_no)
		
		left join 
		
		-- this week's baki SMS
		data_vajapora.weekly_sms_stats_help_4 tbl3 using(mobile_no); 
		
	-- this week's statistics in Bangla, with shop-names
	drop table if exists data_vajapora.weekly_sms_stats;
	create table data_vajapora.weekly_sms_stats as
	select 
		mobile_no,
		coalesce(shop_name, 'প্রিয় ব্যবসায়ী') as shop_name, 
		ei_soptaher_baki_becha_customers,
		ei_soptaher_baki_aday_customers,
		customers_got_tagada_sms,
		customers_got_baki_sms
	from 
		(select 
			mobile_no,
			translate(trim(to_char(ei_soptaher_baki_becha_customers, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_soptaher_baki_becha_customers,
			translate(trim(to_char(ei_soptaher_baki_aday_customers, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_soptaher_baki_aday_customers, 
			translate(trim(to_char(customers_got_tagada_sms, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') customers_got_tagada_sms,
			translate(trim(to_char(customers_got_baki_sms, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') customers_got_baki_sms 
		from data_vajapora.weekly_sms_stats_help_3
		) tbl1 
		
		left join 
		
		(-- shop names
		select mobile_no, shop_name
		from public.register_tallykhatauser 
		) tbl2 using(mobile_no)
		
		inner join 
			
		(-- users whose data got synced yesterday
		select distinct user_id mobile_no
		from tallykhata.eventapp_event_temp
		where
			event_name like '%device_to_server%' 
			and message like '%response%'
			and date(created_at)=current_date-1
		) tbl3 using(mobile_no);
	
	-- drop auxiliary tables
	drop table if exists data_vajapora.weekly_sms_stats_help; 
	drop table if exists data_vajapora.weekly_sms_stats_help_2;
	drop table if exists data_vajapora.weekly_sms_stats_help_3; 
	drop table if exists data_vajapora.weekly_sms_stats_help_4; 
END;
$function$
;

select *
from data_vajapora.weekly_sms_stats;
