<b>IPURGE</B>
1) purge a database based on conditions -- based on last updated date or modified date -- which is customizable in the NLP.properties

How the program works is as follows  

<hr>
<p align="left">
  <img src="https://github.com/gajoseph/purge/blob/master/testschemaER.jpg" width="700"/>
</p>
<hr>

Level 0 table are: tab1 
Level 1 child tables are : tab12, tab2, tab3
Level 2 child table/s are : tab4 Child of (tab3, tab2)

1) Process take the eligibale rows from tab1, top level paarent; fecthes only rows sepcified in property ALL.TAB.BATCH.SIZE=10; 
2) Then, associated rows from child tables  are pulled for example (tab1.col1 = tab12.tab1col1) or (tab12.tab1col1 is null or updatedatetime <=ALL.TAB.FIELD.VALUE=2018-04-22); till it covers all the child tables; 
2) If child rows are not eligible to delete; process will recursively update all associated parent rows and set the status as not deletable; this is the upward process 
3) once all the eligible rows are collected; its starts deleting from bottom level to level up—it also check if the child rows can be deleted based on a linked to parent key id 
4) the deletes are fired by spinning up threads froma thread pool of 3 – is configurable 

Use case 
1)	if a row in tab12 is updated w/ todays date its parent row in tab1 can’t be deleted; 
2)	tab1's child rows in tab2 and tab3 can't be deleted
3)	if one child row is not deleted able then the common parent can’t be deleted 
4)	Also, tab4 has 2 parent tables; a row in tab4 that has a parent row in tab2  and can't be deleted; then the other parent ie tab3 row cannot be also deleted;
5)	Also, the parent rows all all the way top can't be deleted as well as  all the child rows associated w/ each parent

