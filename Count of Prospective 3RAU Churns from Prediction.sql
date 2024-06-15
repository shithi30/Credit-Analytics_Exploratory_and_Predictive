/*
- Viz: 
- Data: 
- Function: 
- Table:
- File: 
- Path: E:\SureCash Work\Daily Churn Prediction 2
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
	- 2481 3RAUs were predicted to get churned (reduce >=30% active days in the next 14 days) among 19045 prospective churns from 2021-08-25's DAUs
	- data_vajapora.pred_churn was pushed to DWH from the resultant pred_churn.csv file
*/

select count(distinct mobile_no) prospective_3rau_churns
from 
	(select mobile_no
	from tallykhata.tallykhata_regular_active_user
	where 
		rau_category=3
		and rau_date='2021-08-25'::date
	group by 1
	) tbl1 
	
	inner join 
	
	(select concat('0', pred_churn::text) mobile_no
	from data_vajapora.pred_churn
	) tbl2 using(mobile_no);
