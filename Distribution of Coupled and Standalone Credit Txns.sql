/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1828728039
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
	Shithi, can we have another distribution based on # of transactions (run on lifetime credit entries)?
	Find % of single transactions vs dilam/pelam coupled. 
	
	~4% of total monthly credit txns are coupled.
*/

do $$ 

declare 
	var_seq int:=1; 
begin 
	raise notice 'New OP goes below:'; 
	
	-- months for statistics
	drop table if exists data_vajapora.help_c; 
	create table data_vajapora.help_c as
	select *, row_number() over(order by txn_month asc) seq 
	from 
		(select left(txn_date::text, 7) txn_month, max(txn_date) txn_month_last_date 
		from 
			(select generate_series(0, current_date-'2021-09-01'::date, 1)+'2021-09-01'::date txn_date
			) tbl1 
		group by 1
		) tbl1; 
	
	loop 
		delete from data_vajapora.coupled_standalone_credit_txns 
		where year_month=(select txn_month from data_vajapora.help_c where seq=var_seq); 
	
		-- all credit txns
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as
		select jr_ac_id 
		from tallykhata.tallykhata_fact_info_final  
		where 
			txn_type in('CREDIT_SALE', 'CREDIT_SALE_RETURN')
			and left(created_datetime::text, 7)=(select txn_month from data_vajapora.help_c where seq=var_seq); 
		
		-- coupled credit txns
		drop table if exists data_vajapora.help_b; 
		create table data_vajapora.help_b as
		select id jr_ac_id
		from public.journal 
		where 
			amount>0 and amount_received>0
			and left(create_date::text, 7)=(select txn_month from data_vajapora.help_c where seq=var_seq); 
		
		-- necessary statistics
		insert into data_vajapora.coupled_standalone_credit_txns
		select 
			(select txn_month from data_vajapora.help_c where seq=var_seq) year_month, 
			count(distinct tbl1.jr_ac_id) all_credit_txns,
			count(distinct case when tbl2.jr_ac_id is null then tbl1.jr_ac_id else null end) standalone_credit_txns, 
			count(distinct case when tbl2.jr_ac_id is not null then tbl1.jr_ac_id else null end) coupled_credit_txns
		from 
			data_vajapora.help_a tbl1 
			left join 
			data_vajapora.help_b tbl2 using(jr_ac_id); 
		
		commit; 
		raise notice 'Data generated for: %', (select txn_month from data_vajapora.help_c where seq=var_seq); 
		var_seq:=var_seq+1; 
		if var_seq=(select max(seq) from data_vajapora.help_c)+1 then exit; 
		end if; 
	end loop; 
end $$; 

select * 
from data_vajapora.coupled_standalone_credit_txns
order by 1; 
