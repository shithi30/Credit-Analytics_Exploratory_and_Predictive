/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1xFKm-h2mTz4LMWt9lRtFHRfFkgTf7IhrUCfcf4X-l-I/edit#gid=0
- Function: 
- Table:
- Instructions: 
- Format: https://docs.google.com/document/d/17RU52fMbGbCdekRuWgXxjuiVC8rVyEH6/edit
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
	Drive: https://drive.google.com/drive/u/1/folders/1w8AeOyqDNm_KIcgbEE527zg--RW_0n92
	Questions: https://docs.google.com/document/d/1JMdFRKR8APuNhbnLEuyJczngyI27OYgo/edit#

	# Title: Role of clinical skill centers in undergraduate dental education: Stakeholders view
	# Why: to know present status, how to improve
	# Situation: 
	- real patient, real device, simulated patient, simulated device 
		- ase kina, teacher porate pare kina, student ra scope pay kina
	resonse from 1 to 5 
	# Training: 
	- real er jonno: train korte hobe, asses korte hobe, simulation e valo korte hobe
	- training er jonno real: patient, device er req. komate hobe 
	- repeatedly, own pace e sujog dite hobe
	- 23 unclear 
	- simulation e empathy develop kore na, symptom/behavior real world ke represent kore na
	- costly infrastructure, trained trainer/staff
	- policy, resistance to change
	resonse from -2 to 2
	# Need: plan, motivation, fund, resource, curriculam, nomuna, training, consultants
	resonse from -2 to 2
	# Require Advanced Devices: 
	- manikin, simulator, lab
	- VR: work stations, simulator, learning environment 
	resonse from -2 to 2

	
*/

-- data cleaning
drop table if exists data_vajapora.help_d; 
create table data_vajapora.help_d as
select 
	id, "respondent type", "if CSC exists", query, 
	case 
		when response in('N', 'IA', 'opt', 'su', 'ab', 'SDA', 'DA', 'NANDA', 'A', 'SA') then response 
		else 'No Response' 
	end response, 
	case 
		when response='N' then 0
		when response='IA' then 1
		when response='opt' then 2
		when response='su' then 3
		when response='ab' then 4
		
		when response='SDA' then 1
		when response='DA' then 2
		when response='NANDA' then 3
		when response='A' then 4
		when response='SA' then 5
	
		else null
	end response_numeric
from 
	(select
		"ID" id, "Q1respondent" as "respondent type", "Q2skill_center" as "if CSC exists", 
		unnest(array['Q15', 'Q16', 'Q17', 'Q18', 'Q19', 'Q20', 'Q21', 'Q22', 'Q23', 'Q24', 'Q25', 'Q26', 'Q27', 'Q28', 'Q29', 'Q30', 'Q31', 'Q32', 'Q33', 'Q34', 'Q35', 'Q36', 'Q37', 'Q38', 'Q39', 'Q40', 'Q41', 'Q42', 'Q43', 'Q44', 'Q45', 'Q46', 'Q03', 'Q04', 'Q05', 'Q06', 'Q07', 'Q08', 'Q09', 'Q10', 'Q11', 'Q12', 'Q13', 'Q14']) query,
	    unnest(array["Q15", "Q16", "Q17", "Q18", "Q19", "Q20", "Q21", "Q22", "Q23", "Q24", "Q25", "Q26", "Q27", "Q28", "Q29", "Q30", "Q31", "Q32", "Q33", "Q34", "Q35", "Q36", "Q37", "Q38", "Q39", "Q40", "Q41", "Q42", "Q43", "Q44", "Q45", "Q46", "Q3tr", "Q4tr", "Q5tr", "Q6Tr", "Q7tr", "Q8tr", "Q9tr", "Q10tr", "Q11tr", "Q12tr", "Q13Tr", "Q14tr"]) as response
	from data_vajapora.csc_analyses_3
	) tbl1;

/*select * 
from data_vajapora.help_d;*/ 

-- table-01
select 
	"respondent type", 
	count(distinct id) freq, 
	round(count(distinct id)*1.00/(select count(distinct id) from data_vajapora.help_d), 2) freq_pct
from data_vajapora.help_d
group by 1; 

-- table-02
select 
	"if CSC exists", 
	count(distinct id) freq, 
	round(count(distinct id)*1.00/(select count(distinct id) from data_vajapora.help_d), 2) freq_pct
from data_vajapora.help_d 
group by 1; 

-- table-03 Q3 to Q14

-- situation: N IA OPT SU AB

/*select 
	query, 
	response,
	count(id)::text query_respondents
from data_vajapora.help_d 
where query in('Q03', 'Q04', 'Q05', 'Q06', 'Q07', 'Q08', 'Q09', 'Q10', 'Q11', 'Q12', 'Q13', 'Q14')
group by 1, 2
order by 1, 2;*/ 

select
	query, 
	concat("N", ' (', round("N"::int*100.00/respondents, 2), '%)') "N",
	concat("IA", ' (', round("IA"::int*100.00/respondents, 2), '%)') "IA",
	concat("opt", ' (', round("opt"::int*100.00/respondents, 2), '%)') "opt",
	concat("su", ' (', round("su"::int*100.00/respondents, 2), '%)') "su",
	concat("ab", ' (', round("ab"::int*100.00/respondents, 2), '%)') "ab",
	"No Response", 
	concat(respondents, ' (100%)') respondents,
	"score=mean±std.dev"
from 
	(select * 
	from crosstab
		('select 
			query, 
			response,
			count(id)::text query_respondents
		from data_vajapora.help_d 
		where query in(''Q03'', ''Q04'', ''Q05'', ''Q06'', ''Q07'', ''Q08'', ''Q09'', ''Q10'', ''Q11'', ''Q12'', ''Q13'', ''Q14'')
		group by 1, 2
		order by 1, 2'
	) tbl1 ("query" text, "ab" text, "IA" text, "N" text, "No Response" text, "opt" text, "su" text)) tbl1 
	
	inner join 
	
	(select 
		query, 	
		count(id) respondents,
		concat(round(avg(response_numeric), 4), ' ± ', round(stddev(response_numeric), 4)) "score=mean±std.dev"
	from data_vajapora.help_d 
	where 
		query in('Q03', 'Q04', 'Q05', 'Q06', 'Q07', 'Q08', 'Q09', 'Q10', 'Q11', 'Q12', 'Q13', 'Q14')
		and response_numeric is not null
	group by 1
	) tbl2 using(query);

-- table-04 Q15 to Q32
-- table-05 Q33 to Q39
-- table-06 Q40 to Q46

-- training, need, devices: SDA DA NANDA A SA

/*select query, response, query_respondents
from 
	(select * 
	from 
		(select concat('Q', generate_series(15, 46)) query) tbl1, 
		(select unnest(string_to_array(concat('A', '_','DA','_','NANDA','_','No Response','_','SA','_','SDA'), '_')) response) tbl2
	) tbl1 
	
	left join 
	
	(select 
		query, 
		response,
		count(id)::text query_respondents
	from data_vajapora.help_d 
	where query not in('Q03', 'Q04', 'Q05', 'Q06', 'Q07', 'Q08', 'Q09', 'Q10', 'Q11', 'Q12', 'Q13', 'Q14')
	group by 1, 2
	) tbl2 using(query, response)
order by 1, 2; */ 

select 
	query, 
	concat("SDA", ' (', round("SDA"::int*100.00/respondents, 2), '%)') "SDA",
	concat("DA", ' (', round("DA"::int*100.00/respondents, 2), '%)') "DA",
	concat("NANDA", ' (', round("NANDA"::int*100.00/respondents, 2), '%)') "NANDA",
	concat("A", ' (', round("A"::int*100.00/respondents, 2), '%)') "A",
	concat("SA", ' (', round("SA"::int*100.00/respondents, 2), '%)') "SA",
	"No Response", 
	concat(respondents, ' (100%)') respondents,
	"score=mean±std.dev"
from 
	(select * 
	from crosstab
		('select query, response, query_respondents
		from 
			(select * 
			from 
				(select concat(''Q'', generate_series(15, 46)) query) tbl1, 
				(select unnest(string_to_array(concat(''A'', ''_'',''DA'',''_'',''NANDA'',''_'',''No Response'',''_'',''SA'',''_'',''SDA''), ''_'')) response) tbl2
			) tbl1 
			
			left join 
			
			(select 
				query, 
				response,
				count(id)::text query_respondents
			from data_vajapora.help_d 
			where query not in(''Q03'', ''Q04'', ''Q05'', ''Q06'', ''Q07'', ''Q08'', ''Q09'', ''Q10'', ''Q11'', ''Q12'', ''Q13'', ''Q14'')
			group by 1, 2
			) tbl2 using(query, response)
		order by 1, 2'
		) tbl1 ("query" text, "A" text, "DA" text, "NANDA" text, "No Response" text, "SA" text, "SDA" text)
	) tbl1 
	
	inner join 
	
	(select 
		query, 
		count(id) respondents,
		concat(round(avg(response_numeric), 4), ' ± ', round(stddev(response_numeric), 4)) "score=mean±std.dev"
	from data_vajapora.help_d 
	where 
		query not in('Q03', 'Q04', 'Q05', 'Q06', 'Q07', 'Q08', 'Q09', 'Q10', 'Q11', 'Q12', 'Q13', 'Q14')
		and response_numeric is not null
	group by 1
	) tbl2 using(query);

-- Cronbach's Alpha Reliability test
# download and install the right RTools version
install.packages("tmvnsim", dependencies = TRUE)
install.packages("psych", dependencies = TRUE)

getwd()
setwd("C:/Users/progoti/Downloads")

df_cronbach <- read.csv("CSC Analyses - Alpha.csv", header = TRUE, sep = ",")
head(df_cronbach)

df_cronbach_table3 <- data.frame(df_cronbach$Q03, df_cronbach$Q04, df_cronbach$Q05, df_cronbach$Q06, df_cronbach$Q07, df_cronbach$Q08, df_cronbach$Q09, df_cronbach$Q10, df_cronbach$Q11, df_cronbach$Q12, df_cronbach$Q13, df_cronbach$Q14)
df_cronbach_table4 <- data.frame(df_cronbach$Q15, df_cronbach$Q16, df_cronbach$Q17, df_cronbach$Q18, df_cronbach$Q19, df_cronbach$Q20, df_cronbach$Q21, df_cronbach$Q22, df_cronbach$Q23, df_cronbach$Q24, df_cronbach$Q25, df_cronbach$Q26, df_cronbach$Q27, df_cronbach$Q28, df_cronbach$Q29, df_cronbach$Q30, df_cronbach$Q31, df_cronbach$Q32)
df_cronbach_table5 <- data.frame(df_cronbach$Q33, df_cronbach$Q34, df_cronbach$Q35, df_cronbach$Q36, df_cronbach$Q37, df_cronbach$Q38, df_cronbach$Q39)
df_cronbach_table6 <- data.frame(df_cronbach$Q40, df_cronbach$Q41, df_cronbach$Q42, df_cronbach$Q43, df_cronbach$Q44, df_cronbach$Q45, df_cronbach$Q46)

head(df_cronbach_table3)
head(df_cronbach_table4)
head(df_cronbach_table5)
head(df_cronbach_table6)

library(psych)
alpha(df_cronbach_table3)
alpha(df_cronbach_table4)
alpha(df_cronbach_table5)
alpha(df_cronbach_table6)