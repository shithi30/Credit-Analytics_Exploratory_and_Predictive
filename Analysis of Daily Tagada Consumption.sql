/*
- Viz: https://docs.google.com/spreadsheets/d/1SWUuc0jI6_34f62X5tgB5L8vpA9J762KU1mxzlL_CtQ/edit#gid=1644194126
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
*/

select date tagada_date, count(distinct merchant_mobile) tagada_users, count(id) tagada_shot 
from public.notification_tagadasms
where date>=current_date-60 and date<current_date
group by 1
order by 1;

-- Total Tagada consumed: 1,363,096
select count(*)
from public.notification_tagadasms;

-- Tagada quota completed merchants: 33,046
select merchant_mobile, count(id) tagada_consumed 
from public.notification_tagadasms 
group by 1
having count(id)>=20; 