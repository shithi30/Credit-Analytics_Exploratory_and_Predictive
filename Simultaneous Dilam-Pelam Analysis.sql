/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1828728039
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
*/

-- merchants entered simultaneous dilam-pelam
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select mobile_no, count(id) sim_dilam_pelam_entries
from public.journal 
where 
	amount>0 
	and amount_received>0
group by 1; 
	
-- merchants transacted >3 days
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select mobile_no, max(date_sequence) txn_days
from tallykhata.tallykhata_transacting_user_date_sequence_final 
group by 1
having max(date_sequence)>3; 

-- metrics to show
select 
	count(tbl1.mobile_no) merchants_transacted_3_or_more_days, 
	count(tbl2.mobile_no) merchants_entered_sim_dilam_pelam,
	count(tbl2.mobile_no)*1.00/count(tbl1.mobile_no) merchants_entered_sim_dilam_pelam_pct
from 
	data_vajapora.help_b tbl1 
	left join 
	data_vajapora.help_a tbl2 using(mobile_no); 

-- monthly merchant-wise statistics
drop table if exists data_vajapora.help_c; 
create table data_vajapora.help_c as
select mobile_no, left(create_date::text, 7) year_month, count(id) sim_dilam_pelam_entries
from public.journal 
where 
	amount>0 
	and amount_received>0
group by 1, 2; 

-- month-by-month simultaneous dilam-pelam tendencies (overall)
select 
	year_month, 
	count(mobile_no) merchants_entered_simultaneous_dilam_pelam,
	count(case when sim_dilam_pelam_entries>=1 and sim_dilam_pelam_entries<=5 then mobile_no else null end) merchants_entered_simultaneous_dilam_pelam_1_to_5, 
	count(case when sim_dilam_pelam_entries>=6 and sim_dilam_pelam_entries<=10 then mobile_no else null end) merchants_entered_simultaneous_dilam_pelam_6_to_10, 
	count(case when sim_dilam_pelam_entries>=11 and sim_dilam_pelam_entries<=15 then mobile_no else null end) merchants_entered_simultaneous_dilam_pelam_11_to_15, 
	count(case when sim_dilam_pelam_entries>=16 and sim_dilam_pelam_entries<=20 then mobile_no else null end) merchants_entered_simultaneous_dilam_pelam_16_to_20, 
	count(case when sim_dilam_pelam_entries>=21 and sim_dilam_pelam_entries<=25 then mobile_no else null end) merchants_entered_simultaneous_dilam_pelam_21_to_25, 
	count(case when sim_dilam_pelam_entries>25 then mobile_no else null end) merchants_entered_simultaneous_dilam_pelam_more_than_25
from 
	data_vajapora.help_c 
group by 1
order by 1; 

-- month-by-month simultaneous dilam-pelam tendencies (txn days>3)
select 
	year_month, 
	count(mobile_no) merchants_entered_simultaneous_dilam_pelam,
	count(case when sim_dilam_pelam_entries>=1 and sim_dilam_pelam_entries<=5 then mobile_no else null end) merchants_entered_simultaneous_dilam_pelam_1_to_5, 
	count(case when sim_dilam_pelam_entries>=6 and sim_dilam_pelam_entries<=10 then mobile_no else null end) merchants_entered_simultaneous_dilam_pelam_6_to_10, 
	count(case when sim_dilam_pelam_entries>=11 and sim_dilam_pelam_entries<=15 then mobile_no else null end) merchants_entered_simultaneous_dilam_pelam_11_to_15, 
	count(case when sim_dilam_pelam_entries>=16 and sim_dilam_pelam_entries<=20 then mobile_no else null end) merchants_entered_simultaneous_dilam_pelam_16_to_20, 
	count(case when sim_dilam_pelam_entries>=21 and sim_dilam_pelam_entries<=25 then mobile_no else null end) merchants_entered_simultaneous_dilam_pelam_21_to_25, 
	count(case when sim_dilam_pelam_entries>25 then mobile_no else null end) merchants_entered_simultaneous_dilam_pelam_more_than_25
from 
	data_vajapora.help_c tbl1 
	inner join 
	data_vajapora.help_b tbl2 using(mobile_no)
group by 1
order by 1; 







	
