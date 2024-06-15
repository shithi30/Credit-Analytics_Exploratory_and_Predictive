/*
- Viz: 
- Data: 
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: Tagada Users In 4.1 or Lower
- Notes (if any): run in live
*/

-- in latest version
select mobile_no
from
	(select mobile_no, count(id) tagada_sent 
	from public.tagada_log 
	group by 1
	having count(id)>9
	) tbl1 
	
	inner join 
	
	(select
		distinct u.mobile as mobile_no,
		u.app_version_name as app_version
	from
		public.registered_users as u
	left join (
		select
			nf.device_id, nf.app_status
		from
			public.notification_fcmtoken nf
		where
			lower(nf.app_status) = 'uninstalled' ) as tbl_1 on
		u.device_id = tbl_1.device_id
	where
		u.device_status = 'active'
		and tbl_1.device_id is null
	) tbl2 using(mobile_no)
where app_version='4.1'; 

-- not in latest version
select mobile_no
from
	(select mobile_no, count(id) tagada_sent 
	from public.tagada_log 
	group by 1
	having count(id)>9
	) tbl1 
	
	inner join 
	
	(select
		distinct u.mobile as mobile_no,
		u.app_version_name as app_version
	from
		public.registered_users as u
	left join (
		select
			nf.device_id, nf.app_status
		from
			public.notification_fcmtoken nf
		where
			lower(nf.app_status) = 'uninstalled' ) as tbl_1 on
		u.device_id = tbl_1.device_id
	where
		u.device_status = 'active'
		and tbl_1.device_id is null
	) tbl2 using(mobile_no)
where app_version!='4.1'; 
