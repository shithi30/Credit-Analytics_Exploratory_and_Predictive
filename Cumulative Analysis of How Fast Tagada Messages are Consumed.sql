/*
- Viz: 306.png
- Data: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1725371224
- Table:
- File: 
- Email thread: 
- Notes (if any): 

Among the 1023 users who have used Tagada till now, 
- 52% have used <=4 messages
- 70% have used <=8 messages
- 80% have used <=12 messages
The faster this curve reaches plateau, the more the success of the choice '20'. 

*/

-- from table: public.register_smsquota
select 
	tbl1.used_sms_count,
	tbl1.users,
	total_users,
	sum(tbl2.users) cum_users,
	sum(tbl2.users)/total_users cum_users_pct
from 
	(select used_sms_count, count(tk_user_id) users
	from public.register_smsquota
	group by 1 
	) tbl1
	
	inner join 
	
	(select used_sms_count, count(tk_user_id) users
	from public.register_smsquota
	group by 1 
	) tbl2 on(tbl1.used_sms_count>=tbl2.used_sms_count),
	
	(select count(*) total_users
	from public.register_smsquota
	) tbl3
group by 1, 2, 3
order by 1; 

-- from table: public.notification_tagadasms (preferred, regularly updated)
do $$

declare
	var_lim int:=20;
	var_yr int:=2021;
	var_mon int:=5;
begin
	-- monthly consumed Tagada SMS per user
	drop table if exists data_vajapora.help_b;
	create table data_vajapora.help_b as
	select 
		date_part('year', created_at) sms_year,
		date_part('month', created_at) sms_month,
		tk_user_id,
		count(id) used_sms
	from 
		(select created_at, id, tallykhata_user_id tk_user_id
		from public.notification_tagadasms
		) tbl1 
	group by 1, 2, 3
	having count(distinct id)<=var_lim;
	
	-- cumulative monthly consumption of Tagada-messages
	drop table if exists data_vajapora.help_a;
	create table data_vajapora.help_a as
	select 
		tbl1.used_sms,
		tbl1.users,
		total_users,
		sum(tbl2.users) cum_users,
		sum(tbl2.users)/total_users cum_users_pct
	from 
		(select used_sms, count(tk_user_id) users
		from data_vajapora.help_b
		where sms_year=var_yr and sms_month=var_mon
		group by 1 
		) tbl1
		
		inner join 
		
		(select used_sms, count(tk_user_id) users
		from data_vajapora.help_b
		where sms_year=var_yr and sms_month=var_mon
		group by 1 
		) tbl2 on(tbl1.used_sms>=tbl2.used_sms),
		
		(select count(tk_user_id) total_users
		from data_vajapora.help_b
		where sms_year=var_yr and sms_month=var_mon
		) tbl3
	group by 1, 2, 3
	order by 1; 
end $$; 

select *
from data_vajapora.help_a;
