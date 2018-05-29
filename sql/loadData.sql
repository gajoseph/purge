truncate table test.tab1 cascade;

/*
Insert into test.tab1 
select * from test.tab1 
*/

insert into test.tab1 
select 'tab1' ||lpad(a::varchar, 5, '0'), rpad(a::varchar, 9, '0'), current_timestamp - interval '1 Month' as  updt_timestmp
from 
generate_series(1,15) a;


/*
Insert into test.tab2
select * from test.tab2
*/


insert into test.tab2
select 'tab2_' ||lpad(a::varchar, 10, '0'), rpad(a::varchar, 2*2, '0'), 'tab1' ||lpad(((1+ random() * 1+ (a % 14))::int)::varchar, 5, '0'),  current_timestamp - interval '1 Month' as  updt_timestmp
--, 1+ random() * (a % 15)
from 
generate_series(0,15) a;

---- another set 
/*
insert into test.tab2
select 'tab2_' ||lpad(a::varchar, 10, '0'), rpad(a::varchar, 2*2, '0'), 'tab1' ||lpad(((1+ random() * 1+ (a % 14))::int)::varchar, 5, '0'),  current_timestamp - interval '1 Month' *   (1+ random() * 1+ (a % 14))::int  as  updt_timestmp
--, 1+ random() * (a % 15)
from 
generate_series(101, 200) a;
*/


insert into test.tab3
select 'tab3_' ||lpad(a::varchar, 10, '0'), rpad(a::varchar, 2*2, '0'), 'tab1' ||lpad(((1+ random() * 1+ (a % 14))::int)::varchar, 5, '0'),  current_timestamp - interval '1 Month' *   (1+ random() * 1+ (a % 14))::int  as  updt_timestmp
--, 1+ random() * (a % 15)
from 
generate_series(0,15) a;



insert into test.tab4
select 'tab4_' ||lpad(a::varchar, 10, '0'), rpad(a::varchar, 2*2, '0'), 
'tab3_' ||lpad(((1+ random() * 1+ (a % 14))::int)::varchar, 10, '0')
,'tab2_' ||lpad(((1+ random() * 1+ (a % 14))::int)::varchar, 10 , '0')

,  current_timestamp - interval '1 Month' *   (1+ random() * 1+ (a % 14))::int  as  updt_timestmp
--, 1+ random() * (a % 15)
from 
generate_series(0, 15) a;

/*test.tab12*/

insert into test.tab12
select 'tab12_' ||lpad(a::varchar, 10, '0'), rpad(a::varchar, 2*2, '0'), 'tab1' ||lpad(((1+ random() * 1+ (a % 14))::int)::varchar, 5, '0'),  current_timestamp - interval '1 Month' as  updt_timestmp
from 
generate_series(0,15) a;

/*
select * From test.tab4tab400001
*/


insert into test.tab6
select 'tab6_' ||lpad(a::varchar, 10, '0'), rpad(a::varchar, 2*2, '0'), 'tab4_' ||lpad(((1+ random() * 1+ (a % 14))::int)::varchar, 10, '0'),  current_timestamp - interval '1 Month' as  updt_timestmp
from 
generate_series(0,15) a;
update test.tab12 set updt_timestmp = now() where tab12col1= 'tab12_0000000015';


update test.tab4 set updt_timestmp = now() where tab4col1= 'tab4_0000000000'
;

update test.tab12 set updt_timestmp = now() where tab12col1= 'tab12_0000000015';

select * from test.tab12 where tab12col1 = 'tab12_0000000015'


select * from test.tab4 where tab4col1= 'tab4_0000000000'
update test.tab4 set updt_timestmp = now() where tab4col1= 'tab4_0000000000'


update test.tab12 set updt_timestmp = now() where tab12col1= 'tab12_0000000015'



select  * from test.tab12 where tab12col1 = 'tab12_0000000015'

select * from test.tab1 where col1='tab100002'
select * from test.tab2 where tab2col3='tab100002' -- 2
select * from test.tab3 where tab3col3='tab100002' --1,15




select * from test.tab4  where tab3col1 in (select tab3col1  from test.tab3 where tab3col3='tab100003' )--1,15)
union 
select * from test.tab4  where tab2col1 in (select tab2col1  from test.tab2 where tab2col3='tab100003' )--1,15)


select * from test.tab41  where tab3col1 in (select tab3col1  from test.tab3 where tab3col3='tab100003' )--1,15)
union 
select * from test.tab41  where tab2col1 in (select tab2col1  from test.tab2 where tab2col3='tab100003' )--1,15)



select * From test.tab6 where tab4col1 in (
select tab4col1 from test.tab4  where tab3col1 in (select tab3col1  from test.tab3 where tab3col3='tab100003' )--1,15)
union 
select tab4col1 from test.tab4  where tab2col1 in (select tab2col1  from test.tab2 where tab2col3='tab100003' )--1,15)
)

'tab6col1'	'tab6col2'	'tab4col1'	'updt_timestmp'
'tab6_0000000000'	'0000'	'tab4_0000000001'	'2018-04-19 16:12:35.731'
'tab6_0000000013'	'1300'	'tab4_0000000014'	'2018-04-19 16:12:35.731'
'tab6_0000000014'	'1400'	'tab4_0000000001'	'2018-04-19 16:12:35.731'





drop table if exists test.tab41   ;
select * into test.tab41   from test.tab4 --2, 15, 14


select * from test.tab41

'tab4col1'	'tab4col2'	'tab3col1'	'tab2col1'	'updt_timestmp'
'tab4_0000000013'	'1300'	'tab3_0000000015'	'tab2_0000000015'	'2017-02-17 16:02:56.479'





select * from test.tab12 where tab12col1 = 'tab12_0000000015'

select * from test.tab1 where col1='tab100002'
select * from test.tab2 where tab2col3='tab100002' -- 2
select * from test.tab3 where tab3col3='tab100002' --1,15


select a.*, tab2col1, tab3col1 from test.tab1 a 
join test.tab2 b on a.col1 = tab2col3 
join test.tab3 c on a.col1 = tab3col3 

where col1='tab100002'

select * from test.tab4  where tab3col1 in (select tab3col1  from test.tab3 where tab3col3='tab100002' )--1,15)
union 
select * from test.tab4  where tab2col1 in (select tab2col1  from test.tab2 where tab2col3='tab100002' )--1,15)



'tab3col1'	'tab2col1'
'tab3_0000000014'	'tab2_0000000014'
'tab3_0000000014'	'tab2_0000000015'


select * From test.tab12

update test.tab4 set 


Select * From TEST.TAB1  where updt_timestmp <= '2018-04-21'


ec2-54-146-232-177.compute-1.amazonaws.com
User name	Administrator
Password	
vlWOZ=KtA5fzF($iq=8Nk9ym7eTFbmaf