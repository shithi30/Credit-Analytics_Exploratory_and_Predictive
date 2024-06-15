/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1795858145
- Data: 
- Function: 
- Table:
- Instructions: % of transaction incresed due to tally message purchase campaign
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
*/

-- campaigns of interest
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select 
	*, 
	case 
		when message_type='POPUP_MESSAGE' then (select id from notification_popupmessage where push_message_id=message_id)
		else message_id 
	end notification_id 
from data_vajapora.all_sch_stats
where campaign_id in('TM220825-01', 'TM220908-01'); 

-- events of interest
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select * 
from 
	(select 
		id, mobile_no, 
		event_date, event_timestamp, event_name,
		bulk_notification_id, notification_id
	from tallykhata.tallykhata_sync_event_fact_final
	where 
		event_date>=(select min(schedule_date) from data_vajapora.help_a) and event_date<current_date-1
		and (notification_id, bulk_notification_id) in(select notification_id, bulk_notification_id from data_vajapora.help_a)
	) tbl1
		
	inner join 
	
	(select notification_id, bulk_notification_id, max(campaign_id) campaign_id
	from data_vajapora.help_a
	group by 1, 2
	) tbl2 using(notification_id, bulk_notification_id); 

-- desired stats

-- seperate
select 
	campaign_id, message_id, 
	message_type, message, 
	message_received, message_opened, link_tapped, opened_via_notification
from 
	(select 
		campaign_id, 
		max(message_id) message_id, 
		max(message_type) message_type, 
		max(message) message
	from data_vajapora.help_a 
	group by 1
	) tbl1
	
	left join 

	(select 
		campaign_id, 
		count(distinct case when event_name like '%_message_received' then mobile_no else null end) message_received, 
		count(distinct case when event_name like '%_message_open' then mobile_no else null end) message_opened, 
		count(distinct case when event_name in('in_app_message_link_tap', 'inbox_message_action') then mobile_no else null end) link_tapped,
		count(distinct case when event_name in('app_opened_from_notification') then mobile_no else null end) opened_via_notification
	from data_vajapora.help_b
	group by 1
	) tbl2 using(campaign_id)
	
union all
	
-- merged
select 
	'merged' campaign_id, null message_id, 
	'-' message_type, '-' message, 
	message_received, message_opened, link_tapped, opened_via_notification
from 
	(select 
		count(distinct case when event_name like '%_message_received' then mobile_no else null end) message_received, 
		count(distinct case when event_name like '%_message_open' then mobile_no else null end) message_opened, 
		count(distinct case when event_name in('in_app_message_link_tap', 'inbox_message_action') then mobile_no else null end) link_tapped,
		count(distinct case when event_name in('app_opened_from_notification') then mobile_no else null end) opened_via_notification
	from data_vajapora.help_b
	) tbl1; 

-- impact
do $$ 

declare 
	var_date date; 
begin 
	drop table if exists data_vajapora.help_c; 
	create table data_vajapora.help_c as
	select event_date, mobile_no, min(event_timestamp) fst_msg_seen_time
	from data_vajapora.help_b 
	where event_name='in_app_message_open'
	group by 1, 2; 

	drop table if exists data_vajapora.help_d; 
	create table data_vajapora.help_d as
	select purchase_date, mobile_no, purchase_time
	from 
		(select tallykhata_user_id, date(created_at) purchase_date, min(created_at) purchase_time 
		from public.payment_purchase
		where status='PURCHASED'
		group by 1, 2
		) tbl1 
		
		inner join 
		
		(select tallykhata_user_id, mobile_number mobile_no 
		from public.register_usermobile 
		) tbl2 using(tallykhata_user_id); 

	var_date:=(select min(event_date) from data_vajapora.help_c); 
	
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.tally_msg_campaign_impact 
		where report_date=var_date; 
	
		insert into data_vajapora.tally_msg_campaign_impact
		select 
			var_date report_date,
			
			count(distinct mobile_no) merchants_recorded_txn, 
			count(distinct case when txn_timestamp>fst_msg_seen_time then mobile_no else null end) merchants_recorded_txn_after_seeing_msg, 
			count(distinct case when txn_timestamp>fst_msg_seen_time and txn_timestamp<=fst_msg_seen_time+interval '15 minutes' then mobile_no else null end) merchants_recorded_txn_in_15_mins_seeing_msg, 
			count(distinct case when txn_timestamp>purchase_time then mobile_no else null end) merchants_recorded_txn_after_buying_pkg, 
			
			count(auto_id) txns_recorded, 
			count(case when txn_timestamp>fst_msg_seen_time then auto_id else null end) txns_recorded_after_seeing_msg, 
			count(case when txn_timestamp>fst_msg_seen_time and txn_timestamp<=fst_msg_seen_time+interval '15 minutes' then auto_id else null end) txn_recorded_in_15_mins_seeing_msg, 
			count(case when txn_timestamp>purchase_time then auto_id else null end) txns_recorded_after_buying_pkg 
		from 
			(select mobile_no, auto_id, txn_timestamp 
			from tallykhata.tallykhata_fact_info_final 
			where created_datetime=var_date
			) tbl1 
			
			left join 
			
			(select mobile_no, fst_msg_seen_time 
			from data_vajapora.help_c 
			where event_date=var_date
			) tbl2 using(mobile_no)
			
			left join 
			
			(select mobile_no, purchase_time 
			from data_vajapora.help_d
			where purchase_date=var_date
			) tbl3 using(mobile_no); 
	
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop;
end $$; 

select * 
from data_vajapora.tally_msg_campaign_impact; 
