/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1368853646
- Data: 
- Function: 
- Table:
- Instructions (Amyou Da): https://docs.google.com/spreadsheets/d/1cK9Ap07wlMeXmUk3QcoLVXUWpAVXuLJeCuHnSZbueKY/edit#gid=1954449328
- Format: https://docs.google.com/presentation/d/1pspwSrtwxoWgGSFVFJytuYSk6DIvmp6RF_cuNJ5gSnQ/edit#slide=id.p
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): Bapon's Doc - https://docs.google.com/document/d/1e5U-zxFfuT2Ad9TCopGeLA7Gqk_YSW5BNXTS2r3czqk/edit
*/

-- no. of packages sold 
select
	date(created_at) purchase_date, 
	count(id) packages_purchased, 
	count(case when pkg_name='টি১৫০' then id else null end) package_purchased_t150, 
	count(case when pkg_name='টি৭৫' then id else null end) package_purchased_t75, 
	count(case when pkg_name='টি২৫' then id else null end) package_purchased_t25
from public.payment_purchase
where status='PURCHASED'
group by 1 
order by 1; 

-- no. of users bought Tally messages 
select
	date(created_at) purchase_date, 
	count(distinct case when pkg_name='টি১৫০' then tallykhata_user_id else null end) merchants_purchased_t150, 
	count(distinct case when pkg_name='টি৭৫' then tallykhata_user_id else null end) merchants_purchased_t75, 
	count(distinct case when pkg_name='টি২৫' then tallykhata_user_id else null end) merchants_purchased_t25, 
	count(distinct tallykhata_user_id) merchants_purchased
from public.payment_purchase
where status='PURCHASED'
group by 1 
order by 1; 

-- Tally messages bought by segments
do $$

declare 
	var_date date:='2022-02-16'::date; 
begin  
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.pkg_bought_seg
		where report_date=var_date;
	
		insert into data_vajapora.pkg_bought_seg
		select 
			var_date report_date, 
			count(distinct tallykhata_user_id) merchants_bought_package, 
			count(distinct case when derived_tg='SPU' then tallykhata_user_id else null end) merchants_bought_package_spu, 
			count(distinct case when derived_tg='PU' then tallykhata_user_id else null end) merchants_bought_package_pu, 
			count(distinct case when derived_tg not in('PU', 'SPU') or derived_tg is null then tallykhata_user_id else null end) merchants_bought_package_others 
		from 
			(select tallykhata_user_id
			from public.payment_purchase
			where 
				status='PURCHASED'
				and date(created_at)=var_date
			) tbl1 
			
			inner join 
			
			(select mobile_number mobile_no, tallykhata_user_id
			from public.register_usermobile
			) tbl2 using(tallykhata_user_id)
		
			left join
		
			(select 
				mobile_no, 
				case 
					when mobile_no in(select mobile_no from tallykhata.tk_spu_aspu_data where pu_type in ('SPU','Sticky SPU') and report_date=var_date) then 'SPU'
					when tg ilike '%pu%' then 'PU' 
				end derived_tg
			from cjm_segmentation.retained_users 
			where report_date=var_date
			) tbl3 using(mobile_no); 
	
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select * 
from data_vajapora.pkg_bought_seg; 

-- Tagada sending channels 
select
	date(create_date) tagada_date, 
	count(id) tagada_used, 
	count(case when tagada_type='TAGADA_BY_SMS' then id else null end) tagada_by_own_message, 
	count(case when tagada_type='TAGADA_BY_FREE_SMS' then id else null end) tagada_by_tally_message,
	count(case when tagada_type='TAGADA_BY_SHARE' then id else null end) tagada_by_social_media
from public.tagada_log
where date(create_date)>='2022-02-01' and date(create_date)<current_date
group by 1
order by 1; 

-- Tally messages sent by segments
do $$

declare 
	var_date date:='2022-02-01'::date; 
begin  
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.tally_msg_sent_seg
		where report_date=var_date;
	
		insert into data_vajapora.tally_msg_sent_seg
		select 
			var_date report_date, 
			count(id) merchants_sent_tagada, 
			count(case when derived_tg='SPU' then id else null end) merchants_sent_tagada_spu, 
			count(case when derived_tg='PU' then id else null end) merchants_sent_tagada_pu, 
			count(case when derived_tg not in('PU', 'SPU') or derived_tg is null then id else null end) merchants_sent_tagada_others 
		from 
			(select mobile_no, id
			from public.tagada_log
			where date(create_date)=var_date
			) tbl1 
		
			left join
		
			(select 
				mobile_no, 
				case 
					when mobile_no in(select mobile_no from tallykhata.tk_spu_aspu_data where pu_type in ('SPU','Sticky SPU') and report_date='2022-02-20') then 'SPU'
					when tg ilike '%pu%' then 'PU' 
				end derived_tg
			from cjm_segmentation.retained_users 
			where report_date=var_date
			) tbl2 using(mobile_no); 
	
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select * 
from data_vajapora.tally_msg_sent_seg
order by 1; 

-- types of Tally message sent
do $$

declare 
	var_date date:='2022-02-01'::date; 
begin  
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.tally_msg_types
		where report_date=var_date;
	
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as
		select id, message_body
		from public.t_scsms_message_archive_v2
		where
			1=1
			and channel='TALLYKHATA_TXN'
			and bank_name='SURECASH'
			and lower(message_body) not like '%verification code%'
			and telco_identifier_id in(66, 64, 61, 62, 49, 67) 
			and upper(message_status) in('SUCCESS', '0')
			and date(request_time)=var_date; 
		
		drop table if exists data_vajapora.help_b; 
		create table data_vajapora.help_b as
		select 
			id, 
			translate(substring(message_body, length(message_body)-10, 12), '০১২৩৪৫৬৭৮৯', '0123456789') mobile_no, 
			message_body, 
			case 
				when message_body like '%অনুগ্রহ করে%' then 'tagada'
				when message_body like '%বাকি%' and message_body like 'প্রিয় গ্রাহক%' then 'purber_baki'
				
				when message_body like 'বেচা%' and message_body like '%পেলাম%' then 'supplier_kena_and_payment'
				when message_body like 'বেচা%' then 'supplier_kena'
				when message_body like 'পেলাম%' then 'supplier_payment'
				
				when message_body like 'কেনা%' and message_body like '%দিলাম%' then 'customer_sale_and_payment'
				when message_body like 'কেনা%' then 'customer_sale'
				when message_body like 'দিলাম%' then 'customer_payment'
				
				else 'custom'
			end sms_type
		from data_vajapora.help_a; 
		
		insert into data_vajapora.tally_msg_types
		select 
			var_date report_date, 
			count(id) tally_message_sent, 
			count(case when sms_type='tagada' then id else null end) tally_message_sent_tagada, 
			count(case when sms_type='purber_baki' then id else null end) tally_message_sent_purber_baki, 
			count(case when sms_type='supplier_kena_and_payment' then id else null end) tally_message_sent_supplier_kena_and_payment, 
			count(case when sms_type='supplier_kena' then id else null end) tally_message_sent_supplier_kena, 
			count(case when sms_type='supplier_payment' then id else null end) tally_message_sent_supplier_payment, 
			count(case when sms_type='customer_sale_and_payment' then id else null end) tally_message_sent_customer_sale_and_payment, 
			count(case when sms_type='customer_sale' then id else null end) tally_message_sent_customer_sale, 
			count(case when sms_type='customer_payment' then id else null end) tally_message_sent_customer_payment, 
			count(case when sms_type='custom' then id else null end) tally_message_sent_custom 
		from data_vajapora.help_b; 
		
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select *
from data_vajapora.tally_msg_types; 

/* live */

-- daily users exhausted Tally msg
select date(last_usage_datetime), count(tk_user_id) sms_exhausted_merchants
from 
	(select tk_user_id, free_sms_count+purchased_sms_count-used_sms_count remaining_sms_count, updated_at last_usage_datetime
	from public.register_smsquota
	where purchased_sms_count!=0
	) tbl1 
where remaining_sms_count=0
group by 1 
order by 1; 

-- users exhausted Tally msg
select count(tk_user_id) sms_exhausted_merchants
from 
	(select tk_user_id, free_sms_count+purchased_sms_count-used_sms_count remaining_sms_count, updated_at last_usage_datetime
	from public.register_smsquota
	where purchased_sms_count!=0
	) tbl1 
where remaining_sms_count=0; 

-- users exhausted Tally msg + not using for 15 days
select count(tk_user_id) sms_exhausted_merchants
from 
	(select tk_user_id, free_sms_count+purchased_sms_count-used_sms_count remaining_sms_count, updated_at last_usage_datetime
	from public.register_smsquota
	where purchased_sms_count!=0
	) tbl1 
where 
	remaining_sms_count=0
	and date(last_usage_datetime)<current_date-14; 

-- operator
with tbl1 as 
	(select tk_user_id, used_sms_count
	from public.register_smsquota
	where purchased_sms_count!=0
	)
	
select 
	mobile_operator, 
	count(tk_user_id) merchants_consumed_tally_msg, 
	sum(used_sms_count) total_consumed_tally_msg, 
	count(tk_user_id)*1.00/(select count(tk_user_id) from tbl1) merchants_consumed_tally_msg_pct, 
	sum(used_sms_count)*1.00/(select sum(used_sms_count) from tbl1) total_consumed_tally_msg_pct
from 
	tbl1 
	
	left join 
	
	(select tallykhata_user_id tk_user_id, left(mobile_number, 3) mobile_operator
	from public.register_usermobile
	) tbl2 using(tk_user_id)
group by 1
order by 1; 
