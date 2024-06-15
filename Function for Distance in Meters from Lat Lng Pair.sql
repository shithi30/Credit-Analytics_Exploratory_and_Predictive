CREATE OR REPLACE FUNCTION data_vajapora.lat_long_dist_meters(lat1 numeric, lon1 numeric, lat2 numeric, lon2 numeric)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$

declare
	r numeric:=6371000; 	
	dlat numeric;
	dlon numeric;
	a numeric; 
	dist_meters numeric;
begin
	dlat:=lat2*pi()/180-lat1*pi()/180;
	dlon:=lon2*pi()/180-lon1*pi()/180;
	a:=sin(dlat/2)*sin(dlat/2)+cos(lat1*pi()/180)*cos(lat2 *pi()/180)*sin(dlon/2)*sin(dlon/2); 
	dist_meters=2*atan2(sqrt(a), sqrt(1-a))*r; 
	
	return dist_meters; 
end;
$function$
;
