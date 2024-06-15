/*
- Viz: 316.png
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 

আপনার এলাকার দৈনন্দিন ভোগ্যপণ্য এর মূল্য জানতে আজই ডাউনলোড করুন, টালিখাতা অ্যাপ |
This could be an interesting way of communication if we expand this analysis.

*/

drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select *, price*1.00/quantity price_per_unit
from 
	(select 
		description, 
		split_part(description, ' ', 3) fmcg, 
		translate(split_part(description, ' ', 1), '০১২৩৪৫৬৭৮৯', '0123456789')::numeric quantity,
		amount as price
	from public.journal 
	where 
		(description ~ '^[0-9\.] কেজি' or description ~ '^[০-৯\.] কেজি')
		and split_part(description, ' ', 3)!=''
		and split_part(description, ' ', 3) ~ '^[^0-9]+$'
		and split_part(description, ' ', 3) ~ '^[^০-৯]+$'
		and split_part(description, ' ', 4)=''
		and amount!=0
	) tbl1;

select fmcg, round(avg(price_per_unit)) avg_price_per_unit 
from data_vajapora.help_a
group by 1
order by 1; 

