/*
- Viz: 
- Data: 
- Function: 
- Table: test.business_type_optimization_test_2
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
*/

-- in live: import this as data_vajapora.bi_types_from_live
select mobile_no, business_type
from public.register_tallykhatauser
where business_type is not null and business_type!='';

-- pattern to match with: yesterday's valid grocery patterns
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select string_agg(description, ',') possible_patterns
from 
	(select mobile_no, description 
	from public.journal 
	where 
		description is not null and description!=''
		and date(create_date)=current_date-1
	) tbl1 
	
	inner join 
	
	(select mobile_no
	from public.register_tallykhatauser
	where business_type='GROCERY'
	) tbl2 using(mobile_no); 

-- latest valid description, of those who could be assigned BI-type from live
drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select *
from 
	(select mobile_no, max(id) id 
	from public.journal
	where description is not null and description!=''
	group by 1
	) tbl1 
	
	inner join 
	
	(-- users whose business type could not be found in live
	select mobile_number mobile_no
	from 
		test.business_type_optimization_test tbl1
		
		left join 
		
		(select concat('0', mobile_no) mobile_number, business_type business_type_live
		from data_vajapora.bi_types_from_live
		) tbl2 using(mobile_number)
	where business_type_live is null
	) tbl2 using(mobile_no)
	
	inner join 
	
	(select id, description 
	from public.journal
	) tbl3 using(id); 

-- all BI-types together: given, live, mined
drop table if exists data_vajapora.help_c;
create table data_vajapora.help_c as
select tbl1.mobile_number, caller, old_address, shop_name, business_type, interested_in_loan, trade_license, district, district_id, upazilla, upazilla_id, "number", business_type_live, business_type_mined		
from 
	test.business_type_optimization_test tbl1
	
	left join 
	
	(select concat('0', mobile_no) mobile_number, business_type business_type_live
	from data_vajapora.bi_types_from_live
	) tbl2 on(tbl1.mobile_number=tbl2.mobile_number)
	
	left join 

	(-- matched cases
	select mobile_no mobile_number, 'GROCERY' business_type_mined
	from data_vajapora.help_b
	where 
		   (select possible_patterns from data_vajapora.help_a) ilike concat('%', split_part(description, ' ', 1), '%')
		or (select possible_patterns from data_vajapora.help_a) ilike concat('%', split_part(description, ' ', 2), '%')
		or (select possible_patterns from data_vajapora.help_a) ilike concat('%', split_part(description, ' ', 3), '%')
		or (select possible_patterns from data_vajapora.help_a) ilike concat('%', split_part(description, ' ', 4), '%')
	) tbl3 on(tbl1.mobile_number=tbl3.mobile_number); 

-- final output table
drop table if exists test.business_type_optimization_test_2; 
create table test.business_type_optimization_test_2 as
select 
	mobile_number, 
	caller, 
	old_address, 
	shop_name, 
	case 
		when business_type_live is not null then business_type_live
		when business_type_mined is not null then business_type_mined
		else 'unknown'
	end business_type,
	interested_in_loan, 
	trade_license, 
	district, 
	district_id, 
	upazilla, 
	upazilla_id, 
	"number"
from data_vajapora.help_c; 

select *
from test.business_type_optimization_test_2; 

