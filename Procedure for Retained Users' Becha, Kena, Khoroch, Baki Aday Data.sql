/*
- Viz: 
- Data: 
- Function: 
- Table: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: Data requirement: Retained users
- Notes (if any): UTF8-mode csv dumping gives right results. This procedure is called via a Python script: http://localhost:8888/notebooks/Import%20from%20csv%20to%20DB/retained_data_gen.ipynb 
*/

CREATE OR REPLACE FUNCTION data_vajapora.fn_daily_retained_campaign_data_generation()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

declare

begin
	
	/* active-inactive users */
	
	-- active users
	drop table if exists data_vajapora.help_a;
	create table data_vajapora.help_a as
	select *
	from 
		(select mobile_no
		from tallykhata.tallykhata_user_date_sequence_final
		group by 1
		having max(event_date)>='2021-08-01'
		) tbl1 
		
		inner join 
		
		(select concat('0', mobile_no) mobile_no
		from data_vajapora.retained_today
		) tbl2 using(mobile_no); 
	
	-- inactive users
	drop table if exists data_vajapora.help_c;
	create table data_vajapora.help_c as 
	select *
	from 
		(select concat('0', mobile_no) mobile_no
		from data_vajapora.retained_today
		) tbl1 
		left join 
		data_vajapora.help_a tbl2 using(mobile_no)
	where tbl2.mobile_no is null; 
	
	/* for active users */
	drop table if exists data_vajapora.retained_active_today_stats; 
	create table data_vajapora.retained_active_today_stats as
	select 
		tbl1.mobile_no,
		shop_name, 
		case when ajker_becha is not null then ajker_becha else 0 end ajker_becha,
		case when ajker_kena is not null then ajker_kena else 0 end ajker_kena,
		case when ajker_khoroch is not null then ajker_khoroch else 0 end ajker_khoroch,
		case when ajker_baki_aday is not null then ajker_baki_aday else 0 end ajker_baki_aday
	from 
		-- retained active today
		data_vajapora.help_a tbl1 
		
		left join 
		
		(-- shop names
		select mobile mobile_no, case when shop_name is null then merchant_name else shop_name end shop_name
		from tallykhata.tallykhata_user_personal_info 
		) tbl2 on(tbl1.mobile_no=tbl2.mobile_no)
		
		left join 
		
		(-- activities
		select 
			mobile_no, 
			sum(case when txn_type in('CASH_SALE', 'CREDIT_SALE') then input_amount else 0 end) ajker_becha,
			sum(case when txn_type in('CASH_PURCHASE', 'CREDIT_PURCHASE') then input_amount else 0 end) ajker_kena,
			sum(case when txn_type in('EXPENSE') then input_amount else 0 end) ajker_khoroch,
			sum(case when txn_type in('CREDIT_SALE_RETURN') then input_amount else 0 end) ajker_baki_aday
		from tallykhata.tallykhata_fact_info_final 
		where created_datetime>='2021-08-01' and created_datetime<current_date
		group by 1
		) tbl3 on(tbl1.mobile_no=tbl3.mobile_no);
	
	drop table if exists data_vajapora.retained_active_today_stats_2;
	create table data_vajapora.retained_active_today_stats_2 as
	select 
		mobile_no,
		shop_name, 
		translate(case when ajker_becha is not null then ajker_becha else 0 end::text, '0123456789', '০১২৩৪৫৬৭৮৯') ajker_becha,
		translate(case when ajker_kena is not null then ajker_kena else 0 end::text, '0123456789', '০১২৩৪৫৬৭৮৯') ajker_kena,
		translate(case when ajker_khoroch is not null then ajker_khoroch else 0 end::text, '0123456789', '০১২৩৪৫৬৭৮৯') ajker_khoroch,
		translate(case when ajker_baki_aday is not null then ajker_baki_aday else 0 end::text, '0123456789', '০১২৩৪৫৬৭৮৯') ajker_baki_aday
	from data_vajapora.retained_active_today_stats; 
	
	/* for inactive users */
	drop table if exists data_vajapora.retained_inactive_today_stats; 
	create table data_vajapora.retained_inactive_today_stats as
	select 
		tbl1.mobile_no,
		shop_name, 
		case when ajker_becha is not null then ajker_becha else 0 end ajker_becha,
		case when ajker_kena is not null then ajker_kena else 0 end ajker_kena,
		case when ajker_khoroch is not null then ajker_khoroch else 0 end ajker_khoroch,
		case when ajker_baki_aday is not null then ajker_baki_aday else 0 end ajker_baki_aday
	from 
		-- retained inactive today
		data_vajapora.help_c tbl1 
		
		left join 
		
		(-- shop names
		select mobile mobile_no, case when shop_name is null then merchant_name else shop_name end shop_name
		from tallykhata.tallykhata_user_personal_info 
		) tbl2 on(tbl1.mobile_no=tbl2.mobile_no)
		
		left join 
		
		(-- activities
		select 
			mobile_no, 
			sum(case when txn_type in('CASH_SALE', 'CREDIT_SALE') then input_amount else 0 end) ajker_becha,
			sum(case when txn_type in('CASH_PURCHASE', 'CREDIT_PURCHASE') then input_amount else 0 end) ajker_kena,
			sum(case when txn_type in('EXPENSE') then input_amount else 0 end) ajker_khoroch,
			sum(case when txn_type in('CREDIT_SALE_RETURN') then input_amount else 0 end) ajker_baki_aday
		from tallykhata.tallykhata_fact_info_final 
		where created_datetime>='2021-07-01' and created_datetime<'2021-08-01'
		group by 1
		) tbl3 on(tbl1.mobile_no=tbl3.mobile_no);
	
	drop table if exists data_vajapora.retained_inactive_today_stats_2;
	create table data_vajapora.retained_inactive_today_stats_2 as
	select 
		mobile_no,
		shop_name, 
		translate(case when ajker_becha is not null then ajker_becha else 0 end::text, '0123456789', '০১২৩৪৫৬৭৮৯') ajker_becha,
		translate(case when ajker_kena is not null then ajker_kena else 0 end::text, '0123456789', '০১২৩৪৫৬৭৮৯') ajker_kena,
		translate(case when ajker_khoroch is not null then ajker_khoroch else 0 end::text, '0123456789', '০১২৩৪৫৬৭৮৯') ajker_khoroch,
		translate(case when ajker_baki_aday is not null then ajker_baki_aday else 0 end::text, '0123456789', '০১২৩৪৫৬৭৮৯') ajker_baki_aday
	from data_vajapora.retained_inactive_today_stats; 
	
	/* for holistic data */
	drop table if exists data_vajapora.retained_holistic_today_stats; 
	create table data_vajapora.retained_holistic_today_stats as
	select 
		tbl1.mobile_no,
		shop_name, 
		case when ajker_becha is not null then ajker_becha else 0 end ajker_becha,
		case when ajker_kena is not null then ajker_kena else 0 end ajker_kena,
		case when ajker_khoroch is not null then ajker_khoroch else 0 end ajker_khoroch,
		case when ajker_baki_aday is not null then ajker_baki_aday else 0 end ajker_baki_aday
	from 
		(-- retained today
		select concat('0', mobile_no) mobile_no
		from data_vajapora.retained_today
		) tbl1 
		
		left join 
		
		(-- shop names
		select mobile mobile_no, case when shop_name is null then merchant_name else shop_name end shop_name
		from tallykhata.tallykhata_user_personal_info 
		) tbl2 on(tbl1.mobile_no=tbl2.mobile_no)
		
		left join 
		
		(-- yesterday's activities
		select 
			mobile_no, 
			sum(case when txn_type in('CASH_SALE', 'CREDIT_SALE') then input_amount else 0 end) ajker_becha,
			sum(case when txn_type in('CASH_PURCHASE', 'CREDIT_PURCHASE') then input_amount else 0 end) ajker_kena,
			sum(case when txn_type in('EXPENSE') then input_amount else 0 end) ajker_khoroch,
			sum(case when txn_type in('CREDIT_SALE_RETURN') then input_amount else 0 end) ajker_baki_aday
		from tallykhata.tallykhata_fact_info_final 
		where created_datetime=current_date-1
		group by 1
		) tbl3 on(tbl1.mobile_no=tbl3.mobile_no);
	
	drop table if exists data_vajapora.retained_holistic_today_stats_2;
	create table data_vajapora.retained_holistic_today_stats_2 as
	select 
		mobile_no,
		shop_name, 
		translate(case when ajker_becha is not null then ajker_becha else 0 end::text, '0123456789', '০১২৩৪৫৬৭৮৯') ajker_becha,
		translate(case when ajker_kena is not null then ajker_kena else 0 end::text, '0123456789', '০১২৩৪৫৬৭৮৯') ajker_kena,
		translate(case when ajker_khoroch is not null then ajker_khoroch else 0 end::text, '0123456789', '০১২৩৪৫৬৭৮৯') ajker_khoroch,
		translate(case when ajker_baki_aday is not null then ajker_baki_aday else 0 end::text, '0123456789', '০১২৩৪৫৬৭৮৯') ajker_baki_aday
	from data_vajapora.retained_holistic_today_stats; 

	-- dropping auxiliary tables 
	drop table if exists data_vajapora.help_a;
	drop table if exists data_vajapora.help_c;
	drop table if exists data_vajapora.retained_active_today_stats; 
	drop table if exists data_vajapora.retained_inactive_today_stats; 
	drop table if exists data_vajapora.retained_holistic_today_stats; 

END;
$function$
;

/*
select data_vajapora.fn_daily_retained_campaign_data_generation(); 

select count(*)
from data_vajapora.retained_active_today_stats_2; 
select count(*)
from data_vajapora.retained_inactive_today_stats_2; 
select count(*)
from data_vajapora.retained_holistic_today_stats_2; 
*/


/*
drop table if exists data_vajapora.retained_today; 

drop table if exists data_vajapora.help_a;
drop table if exists data_vajapora.help_c;

drop table if exists data_vajapora.retained_active_today_stats; 
drop table if exists data_vajapora.retained_inactive_today_stats; 
drop table if exists data_vajapora.retained_holistic_today_stats; 

drop table if exists data_vajapora.retained_active_today_stats_2; 
drop table if exists data_vajapora.retained_inactive_today_stats_2; 
drop table if exists data_vajapora.retained_holistic_today_stats_2; 
*/






















