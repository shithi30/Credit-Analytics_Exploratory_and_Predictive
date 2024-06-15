-- version-01: https://docs.google.com/spreadsheets/d/1ixqudNf1dKAk-0ww8wQ2V4wujxp4vrIUxpVZixzu-BU/edit#gid=1152704213
select 
	reg_date, 
	
	count(mobile_no) reg_users, 
	count(case when update_date is null then mobile_no else null end) reg_users_old_version, 
	count(case when update_date=reg_date then mobile_no else null end) reg_users_new_version_reg, 
	count(case when update_date!=reg_date then mobile_no else null end) reg_users_new_version_updated, 
	
	count(case when update_date is null and tg='PU' then mobile_no else null end) reg_users_old_version_pu, 
	count(case when update_date is null and tg='3RAU' then mobile_no else null end) reg_users_old_version_3rau, 
	count(case when update_date is null and tg='SPU' then mobile_no else null end) reg_users_old_version_su, 
	
	count(case when update_date=reg_date and tg='PU' then mobile_no else null end) reg_users_new_version_reg_pu, 
	count(case when update_date=reg_date and tg='3RAU' then mobile_no else null end) reg_users_new_version_reg_3rau, 
	count(case when update_date=reg_date and tg='SPU' then mobile_no else null end) reg_users_new_version_reg_su, 
	
	count(case when update_date!=reg_date and tg='PU' then mobile_no else null end) reg_users_new_version_updated_pu, 
	count(case when update_date!=reg_date and tg='3RAU' then mobile_no else null end) reg_users_new_version_updated_3rau, 
	count(case when update_date!=reg_date and tg='SPU' then mobile_no else null end) reg_users_new_version_updated_su
from 
	(select mobile_number mobile_no, date(created_at) reg_date 
	from public.register_usermobile 
	where date(created_at)>'20-Sep-22'
	) tbl1 
	
	left join 
	
	(select mobile mobile_no, app_version_number, date(updated_at) update_date
	from public.registered_users
	where 
	    device_status='active'
	    and app_version_number>111 
	) tbl2 using(mobile_no)
	
	left join 
	
	(select 
		mobile_no, 
		case 
			when tg like '3RAU%' then '3RAU'
			when tg like 'LTU%' then 'LTU'
			when tg like 'PU%' then 'PU'
			when tg like 'Z%' then 'Zombie' 
			when tg in('NT--') then 'NT'
			when tg in('NB0','NN1','NN2-6') then 'NN'
			when tg in('PSU') then 'PSU'
			when tg in('SPU') then 'SU'
			else null
		end tg
	from 
		(select mobile_no, max(tg) tg
		from cjm_segmentation.retained_users
		where report_date=current_date
		group by 1
		) tbl1
	) tbl3 using(mobile_no)
group by 1; 

-- version-02: https://docs.google.com/spreadsheets/d/1ibczIUSv3F2GVzukLCucDeDft9CUJ6-2PwJPCGNyGRk/edit#gid=0
do $$ 

declare 
	var_date date:='21-Sep-22'; 
begin 
	raise notice 'New OP goes below:'; 
	loop 
		delete from data_vajapora.reg_to_conv
		where reg_date=var_date; 
	
		insert into data_vajapora.reg_to_conv
		select 
			reg_date, 
			
			count(tbl1.mobile_no) reg_users, 
			count(case when (update_date is null or update_date!=reg_date) then tbl1.mobile_no else null end) reg_users_old_version, 
			count(case when update_date=reg_date then tbl1.mobile_no else null end) reg_users_new_version, 
			
			count(case when (update_date is null or update_date!=reg_date) and tbl4.mobile_no is not null then tbl1.mobile_no else null end) reg_users_old_version_pu, 
			count(case when (update_date is null or update_date!=reg_date) and tbl3.mobile_no is not null then tbl1.mobile_no else null end) reg_users_old_version_3rau, 
		
			count(case when update_date=reg_date and tbl4.mobile_no is not null then tbl1.mobile_no else null end) reg_users_new_version_pu, 
			count(case when update_date=reg_date and tbl3.mobile_no is not null then tbl1.mobile_no else null end) reg_users_new_version_3rau
		from 
			(-- registration
			select mobile_number mobile_no, date(created_at) reg_date 
			from public.register_usermobile 
			where date(created_at)=var_date
			) tbl1 
			
			left join 
			
			(-- update
			select mobile mobile_no, max(date(updated_at)) update_date
			from public.registered_users
			where 
			    device_status='active'
			    and app_version_number>111 
			group by 1
			) tbl2 using(mobile_no)
			
			left join 
			
			(-- 3RAU				
			select distinct mobile_no
			from tallykhata.regular_active_user_event
			where 
				rau_category=3
				and report_date>=var_date+7 and report_date<var_date+14
			) tbl3 using(mobile_no)
			
			left join 
			
			(-- PU			
			select distinct mobile_no
			from tallykhata.tk_power_users_10 
			where report_date>=var_date+30 and report_date<var_date+60
			) tbl4 using(mobile_no)
		group by 1;
			
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit;
		end if;
	end loop; 
end $$; 

select *
from data_vajapora.reg_to_conv; 