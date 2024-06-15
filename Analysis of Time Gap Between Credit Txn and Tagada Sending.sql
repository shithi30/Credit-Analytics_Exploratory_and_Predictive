/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1094063082
- Function: 
- Table:
- File: 
- Presentation: 
- Email thread: 
- Notes (if any): 
*/

do $$ 

declare
	var_date date:='2021-05-10'::date;  
begin 

	delete from data_vajapora.cred_txn_to_tagada_mins
	where tagada_date>=var_date; 
	
	raise notice 'New OP goes below:';
	
	loop
		raise notice 'Generating data for: %', var_date; 	
		
		-- merchants who registered on the day
		drop table if exists data_vajapora.help_b;
		create table data_vajapora.help_b as
		select mobile_no, date(reg_datetime) reg_date
		from data_vajapora.version_wise_days 
		where 
			app_version_name in('3.0.0') 
			and date(update_or_reg_datetime)=date(reg_datetime) 
			and date(reg_datetime)=var_date; 
		
		-- Tagadas and credit txns within 3 days of reg, sequenced
		drop table if exists data_vajapora.help_c;
		create table data_vajapora.help_c as
		select *, row_number() over(partition by merchant_mobile order by customer_mobile, created_at, act_type) act_seq
		from 
			(-- Tagadas within 3 days of reg
			select id, merchant_mobile, customer_mobile_no customer_mobile, created_at, 'tagada' act_type
			from public.notification_tagadasms 
			where 
				date in(var_date, var_date+1, var_date+2) 
				and merchant_mobile in(select mobile_no from data_vajapora.help_b)
				
			union all
			
			-- credit txns within 3 days of reg
			select auto_id id, mobile_no merchant_mobile, contact customer_mobile, created_timestamp created_at, 'credit' act_type
			from tallykhata.tallykhata_fact_info_final 
			where 
				txn_type='CREDIT_SALE'
				and created_datetime in(var_date, var_date+1, var_date+2) 
				and mobile_no in(select mobile_no from data_vajapora.help_b)
			) tbl1; 
		
		-- how long merchants are taking to shoot Tagada after credit txn, plus statistics of merchants who shot Tagada to Jer customers (can't track edit Jer cases)
		insert into data_vajapora.cred_txn_to_tagada_mins
		select *
		from 
			(select 
				var_date tagada_date,
				
				(select count(distinct mobile_no) from data_vajapora.help_b) regs,
				(select count(distinct merchant_mobile) from data_vajapora.help_c where act_type='credit') cred_txn_merchants,
				(select count(distinct merchant_mobile) from data_vajapora.help_c where act_type='tagada') tagada_merchants,
				count(distinct merchant_mobile) tagada_merchants_traced,
				
				count(distinct case when cred_txn_to_tagada_mins>=0 and cred_txn_to_tagada_mins<=1 then merchant_mobile else null end) tagada_within_1_min_merchants,
				count(distinct case when cred_txn_to_tagada_mins>=2 and cred_txn_to_tagada_mins<=5 then merchant_mobile else null end) tagada_within_2_to_5_mins_merchants,
				count(distinct case when cred_txn_to_tagada_mins>=6 and cred_txn_to_tagada_mins<=10 then merchant_mobile else null end) tagada_within_6_to_10_mins_merchants,
				count(distinct case when cred_txn_to_tagada_mins>=11 and cred_txn_to_tagada_mins<=15 then merchant_mobile else null end) tagada_within_11_to_15_mins_merchants,
				count(distinct case when cred_txn_to_tagada_mins>=16 and cred_txn_to_tagada_mins<=30 then merchant_mobile else null end) tagada_within_16_to_30_mins_merchants,
				count(distinct case when cred_txn_to_tagada_mins>=31 and cred_txn_to_tagada_mins<=60 then merchant_mobile else null end) tagada_within_31_to_60_mins_merchants,
				count(distinct case when cred_txn_to_tagada_mins>=61 then merchant_mobile else null end) tagada_within_more_than_60_mins_merchants
			from 
				(select 
					tbl1.merchant_mobile, 
					tbl1.customer_mobile, 
					tbl1.created_at cred_txn_datetime, 
					tbl2.created_at tagada_datetime,
					date_part('hour', tbl2.created_at-tbl1.created_at)*60+date_part('minute', tbl2.created_at-tbl1.created_at) cred_txn_to_tagada_mins
				from 
					data_vajapora.help_c tbl1
					inner join 
					data_vajapora.help_c tbl2 
					on(tbl1.merchant_mobile=tbl2.merchant_mobile and tbl1.customer_mobile=tbl2.customer_mobile and tbl1.act_seq=tbl2.act_seq-1) 
				where tbl1.act_type='credit' and tbl2.act_type='tagada'
				) tbl1
			) tbl1, 
			
			(select count(distinct merchant_mobile) tagada_merchants_added_customer_with_jer
			from 
				(-- merchant-customer pairs who used Tagada, but could not be mapped against any credit txn
				select *
				from 
					data_vajapora.help_c tbl1 
					left join
					(select 
						tbl1.merchant_mobile, 
						tbl1.customer_mobile, 
						tbl1.created_at cred_txn_datetime, 
						tbl2.created_at tagada_datetime,
						date_part('hour', tbl2.created_at-tbl1.created_at)*60+date_part('minute', tbl2.created_at-tbl1.created_at) cred_txn_to_tagada_mins
					from 
						data_vajapora.help_c tbl1
						inner join 
						data_vajapora.help_c tbl2 
						on(tbl1.merchant_mobile=tbl2.merchant_mobile and tbl1.customer_mobile=tbl2.customer_mobile and tbl1.act_seq=tbl2.act_seq-1) 
					where tbl1.act_type='credit' and tbl2.act_type='tagada'
					) tbl2 using(merchant_mobile, customer_mobile)
				where 
					tbl1.act_type='tagada'
					and tbl2.merchant_mobile is null
				) tbl1 
				
				inner join 
				
				(-- customers added with jer
				select distinct mobile_no merchant_mobile, contact customer_mobile, create_date::timestamp jer_cust_add_datetime, start_balance jer
				from public.account 
				where 
					type=2
					and start_balance!=0
					and left(contact, 3) not in('010', '011', '012')
				) tbl2 using(merchant_mobile, customer_mobile)
			) tbl2;
		
		-- control loop	
		var_date:=var_date+1;
		if var_date='2021-06-01'::date then exit;
		end if;
	end loop; 

end $$; 

select *
from data_vajapora.cred_txn_to_tagada_mins; 

