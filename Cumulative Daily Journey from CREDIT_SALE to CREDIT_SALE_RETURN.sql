/*
- Viz: 297.png, 298.png
- Data: https://docs.google.com/spreadsheets/d/1lHHVV4vu1Wx5kC3WsT8bfgjd33GzSf3P7OFC4JOsk5Y/edit#gid=725917030
- Table:
- File: 
- Email thread: 
- Notes (if any): 

Findings:
- 50% credit sales are returned the day they were made
- 90% credits sales are returned by the 5th day
- 93% credits sales are returned by the 7th day

*/

drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select *
from
	(select mobile_no, contact, created_datetime, txn_type
	from tallykhata.tallykhata_fact_info_final 
	where 
		txn_type in('CREDIT_SALE', 'CREDIT_SALE_RETURN')
		and created_datetime>='2021-03-01' and created_datetime<='2021-03-31'
	) tbl1
	
	inner join 
		
	(select distinct contact
	from tallykhata.tallykhata_fact_info_final 
	where 
		txn_type in('CREDIT_SALE_RETURN')
		and created_datetime>='2021-03-01' and created_datetime<='2021-03-31'
	) tbl2 using(contact); 
	
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select *, row_number() over(partition by mobile_no order by contact asc, created_datetime asc) seq
from data_vajapora.help_a; 

drop table if exists data_vajapora.help_c; 
create table data_vajapora.help_c as
select tbl2.created_datetime-tbl1.created_datetime days_to_ret, count(*) ret_txns
from 
	data_vajapora.help_b tbl1
	inner join 
	data_vajapora.help_b tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.contact=tbl2.contact and tbl1.seq=tbl2.seq-1)
where tbl1.txn_type='CREDIT_SALE' and tbl2.txn_type='CREDIT_SALE_RETURN'
group by 1; 

select tbl1.days_to_ret, tbl1.ret_txns, tot_ret_txns, sum(tbl2.ret_txns) cum_ret_txns, sum(tbl2.ret_txns)/tot_ret_txns cum_ret_txns_pct
from 
	data_vajapora.help_c tbl1 
	inner join 
	data_vajapora.help_c tbl2 on(tbl1.days_to_ret>=tbl2.days_to_ret),
	
	(select sum(ret_txns) tot_ret_txns
	from data_vajapora.help_c
	) tbl3
group by 1, 2, 3
order by 1; 