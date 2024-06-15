/*
- Viz: 
- Data: 
	- MTB: https://docs.google.com/spreadsheets/d/19iWJJIzf9quiQuLq38ZWlKOeBpGAI46-WUNSQQdq7FU/edit#gid=1259592260
	- BBL: https://docs.google.com/spreadsheets/d/19iWJJIzf9quiQuLq38ZWlKOeBpGAI46-WUNSQQdq7FU/edit#gid=1031166956
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
*/

select 
	mobile_no, loc, shop_name, registration_date, 
	
	case when added_customers is not null then added_customers else 0 end added_customers, 
	case when added_suppliers is not null then added_suppliers else 0 end added_suppliers, 
	
	sales_jul, sales_aug, sales_sep, 
	credit_sales_jul, crredit_sales_aug, credit_sales_sep, 
	credit_sales_ret_jul, crredit_sales_ret_aug, credit_sales_ret_sep, 
	credit_purchase_jul, credit_purchase_aug, credit_purchase_sep, 
	supplier_payment_jul, supplier_payment_aug, supplier_payment_sep, 
	
	recorded_trt_jul, recorded_trt_aug, recorded_trt_sep, 
	recorded_trv_jul, recorded_trv_aug, recorded_trv_sep
from 
	(select 
		mobile mobile_no, 
		concat(district_name, ', ', upazilla_name, ', ', union_name, ', ', city_corporation_name) loc
	from tallykhata.tallykhata_clients_location_info
	where mobile in('01777330688', '01627451816', '01953533326', '01620805353', '01751545248', '01717070254', '01640669098', '01611442283', '01971081043', '01626914745', '01782936088', '01799017649', '01685978777', '01997055421', '01783030123', '01933210508', '01675838230', '01818666749', '01684252871', '01871984310', '01751025117', '01712416141', '01973863333', '01912720744', '01824749488', '01768078078', '01752693353', '01711136931', '01719982697', '01840708188', '01732681999', '01761159835', '01534571268', '01760073140', '01779973460', '01971103710', '01812308616', '01920922657', '01923301569', '01317032917', '01911269405', '01783848854', '01786406061', '01789924864', '01684228015', '01718244958', '01711178257', '01924868859', '01911083914', '01913930595', '01922703529', '01716739953', '01792984762', '01860041274', '01716773084')                                                                   
	) tbl1 
	
	left join 
	
	(select mobile mobile_no, case when shop_name is null then merchant_name else shop_name end shop_name, registration_date
	from tallykhata.tallykhata_user_personal_info 
	where mobile in('01777330688', '01627451816', '01953533326', '01620805353', '01751545248', '01717070254', '01640669098', '01611442283', '01971081043', '01626914745', '01782936088', '01799017649', '01685978777', '01997055421', '01783030123', '01933210508', '01675838230', '01818666749', '01684252871', '01871984310', '01751025117', '01712416141', '01973863333', '01912720744', '01824749488', '01768078078', '01752693353', '01711136931', '01719982697', '01840708188', '01732681999', '01761159835', '01534571268', '01760073140', '01779973460', '01971103710', '01812308616', '01920922657', '01923301569', '01317032917', '01911269405', '01783848854', '01786406061', '01789924864', '01684228015', '01718244958', '01711178257', '01924868859', '01911083914', '01913930595', '01922703529', '01716739953', '01792984762', '01860041274', '01716773084')                                                                   
	) tbl2 using(mobile_no)
	
	left join 
	
	(select mobile_no, count(contact) added_customers
	from public.account 
	where 
		type in(2)
		and mobile_no in('01777330688', '01627451816', '01953533326', '01620805353', '01751545248', '01717070254', '01640669098', '01611442283', '01971081043', '01626914745', '01782936088', '01799017649', '01685978777', '01997055421', '01783030123', '01933210508', '01675838230', '01818666749', '01684252871', '01871984310', '01751025117', '01712416141', '01973863333', '01912720744', '01824749488', '01768078078', '01752693353', '01711136931', '01719982697', '01840708188', '01732681999', '01761159835', '01534571268', '01760073140', '01779973460', '01971103710', '01812308616', '01920922657', '01923301569', '01317032917', '01911269405', '01783848854', '01786406061', '01789924864', '01684228015', '01718244958', '01711178257', '01924868859', '01911083914', '01913930595', '01922703529', '01716739953', '01792984762', '01860041274', '01716773084')                                                                   
		and is_active is true
	group by 1
	) tbl3 using(mobile_no)
	
	left join 
	
	(select mobile_no, count(contact) added_suppliers
	from public.account 
	where 
		type in(3)
		and mobile_no in('01777330688', '01627451816', '01953533326', '01620805353', '01751545248', '01717070254', '01640669098', '01611442283', '01971081043', '01626914745', '01782936088', '01799017649', '01685978777', '01997055421', '01783030123', '01933210508', '01675838230', '01818666749', '01684252871', '01871984310', '01751025117', '01712416141', '01973863333', '01912720744', '01824749488', '01768078078', '01752693353', '01711136931', '01719982697', '01840708188', '01732681999', '01761159835', '01534571268', '01760073140', '01779973460', '01971103710', '01812308616', '01920922657', '01923301569', '01317032917', '01911269405', '01783848854', '01786406061', '01789924864', '01684228015', '01718244958', '01711178257', '01924868859', '01911083914', '01913930595', '01922703529', '01716739953', '01792984762', '01860041274', '01716773084')                                                                   
		and is_active is true
	group by 1
	) tbl4 using(mobile_no)
	
	left join 
	
	(select 
		mobile_no,
		
		sum(case when date_part('year', created_datetime)=2021 and date_part('month', created_datetime)=7 and txn_type in('CASH_SALE', 'CREDIT_SALE') then input_amount else 0 end) sales_jul,
		sum(case when date_part('year', created_datetime)=2021 and date_part('month', created_datetime)=8 and txn_type in('CASH_SALE', 'CREDIT_SALE') then input_amount else 0 end) sales_aug,
		sum(case when date_part('year', created_datetime)=2021 and date_part('month', created_datetime)=9 and txn_type in('CASH_SALE', 'CREDIT_SALE') then input_amount else 0 end) sales_sep, 
	
		sum(case when date_part('year', created_datetime)=2021 and date_part('month', created_datetime)=7 and txn_type in('CREDIT_SALE') then input_amount else 0 end) credit_sales_jul,
		sum(case when date_part('year', created_datetime)=2021 and date_part('month', created_datetime)=8 and txn_type in('CREDIT_SALE') then input_amount else 0 end) crredit_sales_aug,
		sum(case when date_part('year', created_datetime)=2021 and date_part('month', created_datetime)=9 and txn_type in('CREDIT_SALE') then input_amount else 0 end) credit_sales_sep,
		
		sum(case when date_part('year', created_datetime)=2021 and date_part('month', created_datetime)=7 and txn_type in('CREDIT_SALE_RETURN') then input_amount else 0 end) credit_sales_ret_jul,
		sum(case when date_part('year', created_datetime)=2021 and date_part('month', created_datetime)=8 and txn_type in('CREDIT_SALE_RETURN') then input_amount else 0 end) crredit_sales_ret_aug,
		sum(case when date_part('year', created_datetime)=2021 and date_part('month', created_datetime)=9 and txn_type in('CREDIT_SALE_RETURN') then input_amount else 0 end) credit_sales_ret_sep,
	
		sum(case when date_part('year', created_datetime)=2021 and date_part('month', created_datetime)=7 and txn_type in('CREDIT_PURCHASE') then input_amount else 0 end) credit_purchase_jul,
		sum(case when date_part('year', created_datetime)=2021 and date_part('month', created_datetime)=8 and txn_type in('CREDIT_PURCHASE') then input_amount else 0 end) credit_purchase_aug,
		sum(case when date_part('year', created_datetime)=2021 and date_part('month', created_datetime)=9 and txn_type in('CREDIT_PURCHASE') then input_amount else 0 end) credit_purchase_sep,
		
		sum(case when date_part('year', created_datetime)=2021 and date_part('month', created_datetime)=7 and txn_type in('CASH_PURCHASE', 'CREDIT_PURCHASE_RETURN') then input_amount else 0 end) supplier_payment_jul,
		sum(case when date_part('year', created_datetime)=2021 and date_part('month', created_datetime)=8 and txn_type in('CASH_PURCHASE', 'CREDIT_PURCHASE_RETURN') then input_amount else 0 end) supplier_payment_aug,
		sum(case when date_part('year', created_datetime)=2021 and date_part('month', created_datetime)=9 and txn_type in('CASH_PURCHASE', 'CREDIT_PURCHASE_RETURN') then input_amount else 0 end) supplier_payment_sep,
		
		count(case when date_part('year', created_datetime)=2021 and date_part('month', created_datetime)=7 then auto_id else null end) recorded_trt_jul,
		count(case when date_part('year', created_datetime)=2021 and date_part('month', created_datetime)=8 then auto_id else null end) recorded_trt_aug,
		count(case when date_part('year', created_datetime)=2021 and date_part('month', created_datetime)=9 then auto_id else null end) recorded_trt_sep,
		
		sum(case when date_part('year', created_datetime)=2021 and date_part('month', created_datetime)=7 then input_amount else 0 end) recorded_trv_jul,
		sum(case when date_part('year', created_datetime)=2021 and date_part('month', created_datetime)=8 then input_amount else 0 end) recorded_trv_aug,
		sum(case when date_part('year', created_datetime)=2021 and date_part('month', created_datetime)=9 then input_amount else 0 end) recorded_trv_sep
	from tallykhata.tallykhata_fact_info_final 
	where mobile_no in('01777330688', '01627451816', '01953533326', '01620805353', '01751545248', '01717070254', '01640669098', '01611442283', '01971081043', '01626914745', '01782936088', '01799017649', '01685978777', '01997055421', '01783030123', '01933210508', '01675838230', '01818666749', '01684252871', '01871984310', '01751025117', '01712416141', '01973863333', '01912720744', '01824749488', '01768078078', '01752693353', '01711136931', '01719982697', '01840708188', '01732681999', '01761159835', '01534571268', '01760073140', '01779973460', '01971103710', '01812308616', '01920922657', '01923301569', '01317032917', '01911269405', '01783848854', '01786406061', '01789924864', '01684228015', '01718244958', '01711178257', '01924868859', '01911083914', '01913930595', '01922703529', '01716739953', '01792984762', '01860041274', '01716773084')                                                                   
	group by 1	
	) tbl5 using(mobile_no); 
