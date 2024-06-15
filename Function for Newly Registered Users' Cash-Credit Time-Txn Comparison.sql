/*
- Viz: 
	- https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=706023309
	- https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=706023309
- Data: 
- Function: 
- Table: data_vajapora.comparative_new_reg_cash_credit_time, data_vajapora.comparative_new_reg_cash_credit_txn
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
	For new users, it would be interesting to compare Tally vs Cashbox usage:
	- recorded txns
	- time spent
	Ref: Newly Registered Users' Cash-Credit Time-Txn Comparison
*/

CREATE OR REPLACE FUNCTION data_vajapora.fn_comparative_new_reg_cash_credit()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

declare 
	var_date date:=current_date-7; 
begin 
	raise notice 'New OP goes below:'; 
	loop 
		-- delete existing entry for the day
		delete from data_vajapora.comparative_new_reg_cash_credit_time 
		where reg_date=var_date; 
		delete from data_vajapora.comparative_new_reg_cash_credit_txn 
		where reg_date=var_date; 
	
		-- new regs of the day
		drop table if exists data_vajapora.new_reg_help_a;
		create table data_vajapora.new_reg_help_a as
		select mobile_number mobile_no
		from public.register_usermobile 
		where date(created_at)=var_date;
		
		-- sequenced events of new regs on their reg day
		drop table if exists data_vajapora.new_reg_help_b;
		create table data_vajapora.new_reg_help_b as
		select *, row_number() over(partition by mobile_no order by event_timestamp asc) seq
		from 
			(select mobile_no, event_name, event_timestamp
			from tallykhata.tallykhata_sync_event_fact_final
			where event_date=var_date
			) tbl1 
			
			inner join 
			
			data_vajapora.new_reg_help_a tbl2 using(mobile_no); 
		
		-- txns on the reg day
		drop table if exists data_vajapora.new_reg_help_c;
		create table data_vajapora.new_reg_help_c as
		select mobile_no, id, txn_type
		from 
			public.journal tbl1 
			inner join 
			data_vajapora.new_reg_help_a tbl2 using(mobile_no)
		where date(create_date)=var_date;
	
		-- for txns
		insert into data_vajapora.comparative_new_reg_cash_credit_txn
		select 
			var_date reg_date, 
			
			-- new regs' cash txns
			count(distinct case when txn_type in(1, 2, 6, 7, 8) then mobile_no else null end) new_users_used_cash_txn,
			count(case when txn_type in(1, 2, 6, 7, 8) then id else null end) new_users_total_cash_txn,
			count(case when txn_type in(1, 2, 6, 7, 8) then id else null end)*1.00/
			count(distinct case when txn_type in(1, 2, 6, 7, 8) then mobile_no else null end) new_users_avg_cash_txn,
			
			-- new regs' tally txns
			count(distinct case when txn_type in(3, 4) then mobile_no else null end) new_users_used_tally_txn,
			count(case when txn_type in(3, 4) then id else null end) new_users_total_tally_txn,
			count(case when txn_type in(3, 4) then id else null end)*1.00/
			count(distinct case when txn_type in(3, 4) then mobile_no else null end) new_users_avg_tally_txn
		from data_vajapora.new_reg_help_c; 
	
		-- for time
		insert into data_vajapora.comparative_new_reg_cash_credit_time
		select 
			var_date reg_date,
			
			new_users_used_cash_event, 
			new_users_cash_event_time new_users_cash_event_sec, 
			new_users_avg_cash_event_sec,
			new_users_cash_event_time/(new_users_used_cash_event*60.00) new_users_avg_cash_event_min, 
			
			new_users_used_tally_event, 
			new_users_tally_event_time new_users_tally_event_sec, 
			new_users_avg_tally_event_sec,
			new_users_tally_event_time/(new_users_used_tally_event*60.00) new_users_avg_tally_event_min 
		from 
			(-- time spent on cash
			select 
				count(mobile_no) new_users_used_cash_event,
				sum(sec_spent_on_cash) new_users_cash_event_time,
				sum(sec_spent_on_cash)/(count(mobile_no)*1.00) new_users_avg_cash_event_sec
			from 
				(select 
					tbl1.mobile_no,
					sum
					(
						 date_part('hour', tbl2.event_timestamp-tbl1.event_timestamp)*3600
						+date_part('minute', tbl2.event_timestamp-tbl1.event_timestamp)*60
						+date_part('second', tbl2.event_timestamp-tbl1.event_timestamp)
					) sec_spent_on_cash
				from 
					(select mobile_no, event_name, event_timestamp, seq 
					from data_vajapora.new_reg_help_b 
					where event_name in 
						('cash',
						'cash_becha_entry',
						'cash_becha_entry_screen',
						'cash_box_adjustment',
						'cash_box_entry_list',
						'cash_box_open',
						'cash_box_report_details_view',
						'cash_box_report_view',
						'cash_hishab_view',
						'cash_kena_entry',
						'cash_kena_entry_screen',
						'cash_report_view',
						'khoroch_entry',
						'khoroch_entry_screen',
						'malik_dilo_entry',
						'malik_dilo_entry_screen',
						'malik_nilo_entry',
						'malik_nilo_entry_screen',
						'maliker_report_view'
						)
					) tbl1 
					
					inner join 
					
					data_vajapora.new_reg_help_b tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.seq=tbl2.seq-1)
				group by 1
				) tbl1 
			) tbl1,
			
			(-- time spent on tally
			select 
				count(mobile_no) new_users_used_tally_event,
				sum(sec_spent_on_tally) new_users_tally_event_time,
				sum(sec_spent_on_tally)/(count(mobile_no)*1.00) new_users_avg_tally_event_sec
			from 
				(select 
					tbl1.mobile_no,
					sum
					(
						 date_part('hour', tbl2.event_timestamp-tbl1.event_timestamp)*3600
						+date_part('minute', tbl2.event_timestamp-tbl1.event_timestamp)*60
						+date_part('second', tbl2.event_timestamp-tbl1.event_timestamp)
					) sec_spent_on_tally
				from 
					(select mobile_no, event_name, event_timestamp, seq 
					from data_vajapora.new_reg_help_b 
					where event_name like '%tally%'
					) tbl1 
					
					inner join 
					
					data_vajapora.new_reg_help_b tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.seq=tbl2.seq-1)
				group by 1
				) tbl1 
			) tbl2; 
		
		raise notice 'Data generated for: %', var_date; 
	
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if;
	end loop; 

	-- drop auxiliary tables
	drop table if exists data_vajapora.new_reg_help_a;
	drop table if exists data_vajapora.new_reg_help_b;
	drop table if exists data_vajapora.new_reg_help_c;
		
END;
$function$
;

-- select data_vajapora.fn_comparative_new_reg_cash_credit(); 

select *
from data_vajapora.comparative_new_reg_cash_credit_time; 

select *
from data_vajapora.comparative_new_reg_cash_credit_txn; 


