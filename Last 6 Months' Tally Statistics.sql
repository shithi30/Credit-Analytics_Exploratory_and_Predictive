/*
- Viz: https://docs.google.com/spreadsheets/d/1GhaSgdYm97a8N5IIK3KkeRcOGwic9xPB7Qf1QrenMxQ/edit#gid=242838554
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
	In last 6 months (monthly)
	
	- baki becha
	--- merchants #
	--- customers #
	--- TRT
	--- TRV
	
	- baki aday
	--- merchants #
	--- customers #
	--- TRT
	--- TRV
	
	- baki kena
	--- merchants #
	--- suppliers #
	--- TRT
	--- TRV
	
	- baki payment
	--- merchants #
	--- suppliers #
	--- TRT
	--- TRV
*/

do $$ 

declare 
	var_month int:=7; 
begin 
	raise notice 'New OP goes below:'; 
	loop
		-- raw data
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as 
		select mobile_no, txn_type, created_datetime, input_amount, is_suspicious_txn, contact, auto_id
		from tallykhata.tallykhata_fact_info_final 
		where 
			date_part('year', created_datetime)=2021
			and date_part('month', created_datetime)=var_month; 
	
		-- necessary metrics
		insert into data_vajapora.six_month_metrics
		select 
			to_char(created_datetime, 'YYYY-MM') year_month,
			-- baki becha
			count(distinct case when txn_type='CREDIT_SALE' then mobile_no else null end) baki_becha_merchants, 
			count(distinct case when txn_type='CREDIT_SALE' then contact else null end) baki_becha_customers, 
			count(distinct case when txn_type='CREDIT_SALE' then auto_id else null end) baki_becha_trt, 
			sum(case when txn_type='CREDIT_SALE' and is_suspicious_txn=0 then input_amount else 0 end) baki_becha_trv, 
			-- baki aday
			count(distinct case when txn_type='CREDIT_SALE_RETURN' then mobile_no else null end) baki_aday_merchants, 
			count(distinct case when txn_type='CREDIT_SALE_RETURN' then contact else null end) baki_aday_customers, 
			count(distinct case when txn_type='CREDIT_SALE_RETURN' then auto_id else null end) baki_aday_trt, 
			sum(case when txn_type='CREDIT_SALE_RETURN' and is_suspicious_txn=0 then input_amount else 0 end) baki_aday_trv,
			-- baki kena
			count(distinct case when txn_type='CREDIT_PURCHASE' then mobile_no else null end) baki_kena_merchants, 
			count(distinct case when txn_type='CREDIT_PURCHASE' then contact else null end) baki_kena_suppliers, 
			count(distinct case when txn_type='CREDIT_PURCHASE' then auto_id else null end) baki_kena_trt, 
			sum(case when txn_type='CREDIT_PURCHASE' and is_suspicious_txn=0 then input_amount else 0 end) baki_kena_trv, 
			-- baki payment
			count(distinct case when txn_type='CREDIT_PURCHASE_RETURN' then mobile_no else null end) baki_payment_merchants, 
			count(distinct case when txn_type='CREDIT_PURCHASE_RETURN' then contact else null end) baki_payment_suppliers, 
			count(distinct case when txn_type='CREDIT_PURCHASE_RETURN' then auto_id else null end) baki_payment_trt, 
			sum(case when txn_type='CREDIT_PURCHASE_RETURN' and is_suspicious_txn=0 then input_amount else 0 end) baki_payment_trv
		from data_vajapora.help_a
		group by 1; 
	
		raise notice 'Data generated for month: %', var_month; 
		var_month:=var_month+1; 
		if var_month=13 then exit; 
		end if; 
	end loop; 
end $$; 

select *
from data_vajapora.six_month_metrics; 
