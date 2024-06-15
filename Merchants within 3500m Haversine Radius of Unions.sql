/*
- Viz: 
- Data: 
- Function: 
- Table:
- File: Location 2.ipynb
- Path: http://localhost:8888/notebooks/
- Presentation: 
- Email thread: 
- Notes (if any): Location.ipynb verifies that SQL dist is equal to haversine dist
*/

/* script to run daily for the new users, takes about 7-8 mins max */

-- union-wise centers
drop table if exists data_vajapora.union_centers;
create table data_vajapora.union_centers as
select union_name, replace(union_name, '''', '-') trans_union_name, avg(lat) lat1, avg(lng) lon1
from 
    (select tallykhata_user_id, union_name
    from data_vajapora.tk_users_location_sample_final
    ) tbl1

    inner join 

    (select tallykhata_user_id, lat::numeric, lng::numeric
    from tallykhata.tallykhata_client_location_pre_info
    ) tbl2 using(tallykhata_user_id)
group by 1; 

-- recently registered users, who have coordinates, but are not assigned to any union-cluster
drop table if exists data_vajapora.new_user_coordinates;
create table data_vajapora.new_user_coordinates as
select tallykhata_user_id, lat, lng
from 
	(select tallykhata_user_id, date(created_at) reg_date
	from public.register_usermobile 
	where date(created_at)>=current_date-15
	) tbl1 
	
	inner join 
	    
	(select tallykhata_user_id, lat::numeric, lng::numeric
	from tallykhata.tallykhata_client_location_pre_info
	) tbl2 using(tallykhata_user_id)
	
	left join 
	
	(select distinct tallykhata_user_id 
	from data_vajapora.merchants_within_union_radius
	) tbl3 using(tallykhata_user_id)
where tbl3.tallykhata_user_id is null; 

-- assign new merchants to union-clusters
insert into data_vajapora.merchants_within_union_radius
select tallykhata_user_id, lat2 user_lat, lon2 user_lng, union_name dest_union, dist_meters, now() data_generation_timestamp
from 
	(select *, 2*atan2(sqrt(a), sqrt(1-a))*r dist_meters
	from 
		(select *, sin(dlat/2)*sin(dlat/2)+cos(lat1*pi()/180)*cos(lat2*pi()/180)*sin(dlon/2)*sin(dlon/2) a
		from 
			(select *, lat2*pi()/180-lat1*pi()/180 dlat, lon2*pi()/180-lon1*pi()/180 dlon
			from 
				(select *
				from 
					data_vajapora.union_centers tbl1, 
					
					(select tallykhata_user_id, 6371000 r, lat lat2, lng lon2
					from data_vajapora.new_user_coordinates
					) tbl2
				) tbl3
			) tb4
		) tbl5
	) tbl6
where dist_meters<=3500;

-- drop temporary tables
drop table if exists data_vajapora.union_centers;
drop table if exists data_vajapora.new_user_coordinates;

/* in case no merchant is found within 3500m radius of a union */

-- no merchant found within 3500m of a union
select *
from data_vajapora.merchants_within_union_radius
where dest_union='Amadi'; -- change

-- verify that, there is indeed no merchant with 3500m of the union 
select tallykhata_user_id, lat2 user_lat, lon2 user_lng, union_name dest_union, dist_meters, now() data_generation_timestamp
from 
    (select *, 2*atan2(sqrt(a), sqrt(1-a))*r dist_meters
    from 
        (select *, sin(dlat/2)*sin(dlat/2)+cos(lat1*pi()/180)*cos(lat2*pi()/180)*sin(dlon/2)*sin(dlon/2) a
        from 
            (select *, lat2*pi()/180-lat1*pi()/180 dlat, lon2*pi()/180-lon1*pi()/180 dlon
            from 
                (select *
                from 
                    (-- center of the union 
                    select *
                    from data_vajapora.union_centers
                    where union_name='Amadi' -- change
                    ) tbl1, 

                    (-- users at the union 
                    select tallykhata_user_id, 6371000 r, lat lat2, lng lon2
		    from 
			(select tallykhata_user_id, union_name
			from data_vajapora.tk_users_location_sample_final
			) tbl1
						
			inner join 
						
			(select tallykhata_user_id, lat::numeric, lng::numeric
			from tallykhata.tallykhata_client_location_pre_info
			) tbl2 using(tallykhata_user_id)
		   where union_name='Amadi' -- change
		   ) tbl2		
                ) tbl3
            ) tb4
        ) tbl5
    ) tbl6
where dist_meters<=3500;

/* misc queries */

-- summary of users who landed/did not land unions
select 
	count(distinct tbl1.tallykhata_user_id) total_merchants, 
	count(distinct case when tbl2.tallykhata_user_id is not null then tbl1.tallykhata_user_id else null end) assigned_union,
	count(distinct case when tbl2.tallykhata_user_id is null then tbl1.tallykhata_user_id else null end) not_assigned_union
from 
	data_vajapora.user_coordinates tbl1 
	left join 
	data_vajapora.merchants_within_union_radius tbl2 using(tallykhata_user_id); 

-- union that landed no merchants
select union_name
from data_vajapora.union_centers
where union_name not in(select distinct dest_union from data_vajapora.merchants_within_union_radius); 

-- merchants who landed no unions
select distinct tbl1.*
from 
	data_vajapora.user_coordinates tbl1 
	left join 
	data_vajapora.merchants_within_union_radius tbl2 using(tallykhata_user_id)
where tbl2.tallykhata_user_id is null; 

-- cases where no unions were assigned
select count(distinct tbl1.*) 
from 
	data_vajapora.user_coordinates tbl1 
	left join 
	data_vajapora.merchants_within_union_radius tbl2 using(tallykhata_user_id)
where tbl2.tallykhata_user_id is null; 

-- merchants within 3500m of 1 union
insert into data_vajapora.merchants_within_union_radius
select tallykhata_user_id, lat2 user_lat, lon2 user_lng, union_name dest_union, dist_meters, now() data_generation_timestamp
from 
	(select *, 2*atan2(sqrt(a), sqrt(1-a))*r dist_meters
	from 
		(select *, sin(dlat/2)*sin(dlat/2)+cos(lat1*pi()/180)*cos(lat2*pi()/180)*sin(dlon/2)*sin(dlon/2) a
		from 
			(select *, lat2*pi()/180-lat1*pi()/180 dlat, lon2*pi()/180-lon1*pi()/180 dlon
			from 
				(select *
				from 
					(select *
					from data_vajapora.union_centers
					where union_name='Betka' -- change
					) tbl1, 
					
					(select tallykhata_user_id, 6371000 r, lat lat2, lng lon2
					from data_vajapora.user_coordinates
					) tbl2
				) tbl3
			) tb4
		) tbl5
	) tbl6
where dist_meters<=3500;

-- template
select lat1, lon1, lat2, lon2, r, 2*atan2(sqrt(a), sqrt(1-a))*r dist_meters
from 
	(select *, sin(dlat/2)*sin(dlat/2)+cos(lat1*pi()/180)*cos(lat2*pi()/180)*sin(dlon/2)*sin(dlon/2) a
	from 
		(select *, lat2*pi()/180-lat1*pi()/180 dlat, lon2*pi()/180-lon1*pi()/180 dlon
		from 
			(select lat1, lon1, lat2, lon2, 6371000 r
			from 
				(select 52.2296756 lat1, 21.0122287 lon1
				from data_vajapora.help_a 
				order by random() 
				limit 1
				) tbl1, 
				
				(select 52.406374 lat2, 16.9251681 lon2
				from data_vajapora.help_a 
				order by random() 
				limit 1
				) tbl2 
			) tbl3
		) tb4
	) tbl5; 

-- merchants within 200m of Khilgaon
select tallykhata_user_id, lat2, lon2, dist_meters
from 
	(select *, 2*atan2(sqrt(a), sqrt(1-a))*r dist_meters
	from 
		(select *, sin(dlat/2)*sin(dlat/2)+cos(lat1*pi()/180)*cos(lat2 *pi()/180)*sin(dlon/2)*sin(dlon/2) a
		from 
			(select *, lat2*pi()/180-lat1*pi()/180 dlat, lon2*pi()/180-lon1*pi()/180 dlon
			from 
				(select 
					tallykhata_user_id, 
					6371000 r, 23.752057890638582 lat1, 90.42477069938678 lon1, -- Khilgaon
					lat lat2, lng lon2
				from data_vajapora.help_a 
				) tbl3
			) tb4
		) tbl5
	) tbl6
where dist_meters<=200;
