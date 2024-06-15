/*
- Viz: https://docs.google.com/spreadsheets/d/1KPNMaxyr5OOdu3C4778CvRKbLD7WCXIJhnQuxSsIJAI/edit#gid=824781036
- Data: https://docs.google.com/spreadsheets/d/1NQ17yJjfX3j9ubvatCGh_JWl4urwwGtQBH4xmIoga0I/edit?pli=1#gid=985464252
- Function: 
- Table:
- File: 
- Presentation: 
- Email thread: 
- Notes (if any): 

- On an avg., daily 3.5% PUs are using Tagada SMS
- On an avg., PUs are consuming 4 Tagada SMS per head
- On an avg., 72% of daily Tagada SMS users are PUs

*/

-- fraction of Tagada being used by PUs
select 
	tagada_date,
	
	count(distinct id) tagada_sms_sent,
	count(distinct merchant_mobile) tagada_users,
	
	count(distinct case when pu_mobile is not null then id else null end) pu_tagada_sms_sent,
	count(distinct pu_mobile) pu_tagada_users,
	
	ceil(count(distinct case when pu_mobile is not null then id else null end)*1.00/count(distinct pu_mobile)) per_pu_avg_tagada_sms,
	
	count(distinct case when pu_mobile is not null then id else null end)*1.00/count(distinct id) pu_tagada_sms_sent_pct,
	count(distinct pu_mobile)*1.00/count(distinct merchant_mobile) pu_tagada_users_pct
from 
	(select date tagada_date, id, merchant_mobile
	from notification_tagadasms
	where date>='2021-05-09' and date<current_date
	) tbl1 
	
	left join 
	
	(select mobile_no pu_mobile, report_date
	from tallykhata.tk_power_users_10 
	where report_date>='2021-05-09'
	) tbl2 on(tbl1.merchant_mobile=tbl2.pu_mobile and tbl1.tagada_date=tbl2.report_date)
group by 1
order by 1 asc; 
	
-- fraction of PUs using Tagada
select 
	report_date,
	count(distinct pu_mobile) pus,
	count(distinct merchant_mobile) pus_sent_tagada,
	count(distinct merchant_mobile)*1.00/count(distinct pu_mobile) pus_sent_tagada_pct
from 
	(select mobile_no pu_mobile, report_date
	from tallykhata.tk_power_users_10 
	where report_date>='2021-05-09'
	) tbl2
	
	left join 
	
	(select date tagada_date, id, merchant_mobile
	from notification_tagadasms
	where date>='2021-05-09' and date<current_date
	) tbl1 on(tbl1.merchant_mobile=tbl2.pu_mobile and tbl1.tagada_date=tbl2.report_date)
group by 1 
order by 1 asc; 
