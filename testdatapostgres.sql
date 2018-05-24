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

/*
updating tab12 so that all the asscoiated data won't be deleted. 

*/
update test.tab12 set updt_timestmp = now() where tab12col1= 'tab12_0000000015';
