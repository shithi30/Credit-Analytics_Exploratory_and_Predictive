/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1741786494
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
	Tagada Message Consumption analysis 
	
	Our objective is to understand monthly Tagada mesasge consumption. Who are using it mostly, who is using less. (SPU or PU or others; and Grocery, Pharma or others). This will help to prepare the Paid Tagada SMS packages.
	
	@data team
	Kindly share below information and more which you find helpful.
	1. How many users use Tagada message monthly; 
	---Merchant segment wise distribution (SPU, PU, 3RAU etc)
	---Volumewise distribution
	
	2. How many users use regularly. For example, count those users and their usages who use every month in last 3 months.
	
	(Tagada message usage means use SMS quota, own SMS and Share using chat app successfully etc.)
*/

do $$ 

declare 
	var_date date:='2022-01-01'::date; 
begin 
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.tagada_consume_stats
		where report_date=var_date; 
	
		insert into data_vajapora.tagada_consume_stats
		select
			var_date report_date, 
			count(mobile_no) regular_tagada_users, 
			count(case when segment='SPU' then mobile_no else null end) regular_tagada_users_SPU,
			count(case when segment='3RAU' then mobile_no else null end) regular_tagada_users_3RAU,
			count(case when segment='LTU' then mobile_no else null end) regular_tagada_users_LTU,
			count(case when segment='NT' then mobile_no else null end) regular_tagada_users_NT,
			count(case when segment='NN' then mobile_no else null end) regular_tagada_users_NN,
			count(case when segment='PSU' then mobile_no else null end) regular_tagada_users_PSU,
			count(case when segment='PU' then mobile_no else null end) regular_tagada_users_PU,
			count(case when segment='Zombie' then mobile_no else null end) regular_tagada_users_Zombie,
			count(case when segment is null then mobile_no else null end) regular_tagada_users_uninstalled
		from 
			(select 
				merchant_mobile mobile_no, 
				count(case when "date">=var_date-30 and "date"<var_date-00 then id else null end) past_1_month_usage,
				count(case when "date">=var_date-60 and "date"<var_date-30 then id else null end) past_2_month_usage,
				count(case when "date">=var_date-90 and "date"<var_date-60 then id else null end) past_3_month_usage
			from public.notification_tagadasms
			group by 1
			) tbl1 
			
			left join 
			
			(select 
				mobile_no, 
				case 
					when mobile_no in
						(select mobile_no
						from tallykhata.tk_spu_aspu_data 
						where 
							pu_type='SPU'
							and report_date=var_date-1
						)
						then 'SPU'
					when tg in('3RAUCb','3RAU Set-A','3RAU Set-B','3RAU Set-C','3RAUTa','3RAUTa+Cb','3RAUTacs') then '3RAU'
					when tg in('LTUCb','LTUTa') then 'LTU'
					when tg in('NT--') then 'NT'
					when tg in('NB0','NN1','NN2-6') then 'NN'
					when tg in('PSU') then 'PSU'
					when tg in('PUCb','PU Set-A','PU Set-B','PU Set-C','PUTa','PUTa+Cb','PUTacs') then 'PU'
					when tg in('ZCb','ZTa','ZTa+Cb') then 'Zombie' 
				end segment
			from cjm_segmentation.retained_users 
			where report_date=var_date
			) tbl2 using(mobile_no) 
		where past_1_month_usage>0 and past_2_month_usage>0 and past_3_month_usage>0; 
			
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select 
	report_date,
	regular_tagada_users,
	regular_tagada_users_spu,
	regular_tagada_users_3rau,
	regular_tagada_users_ltu,
	regular_tagada_users_nt,
	-- regular_tagada_users_nn,
	regular_tagada_users_psu,
	regular_tagada_users_pu,
	regular_tagada_users_zombie,
	regular_tagada_users_uninstalled
from data_vajapora.tagada_consume_stats; 
