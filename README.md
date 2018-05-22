## iPURGE
This program automatically purges(deletes) data in a database.The data is purged based on a how old it is. Retention of data is based on condition applied to last updated date / last modified data field/column in a table. If table row meets the conidtion then it 
is a cndidate to delete. 
1. To apply the condition set the property`ALL.TAB.FIELD`; for example `ALL.TAB.FIELD=updt_timestmp` means all the table has an updt_timestmp and `ALL.TAB.FIELD.VALUE=2018-04-22
ALL.TAB.FIELD.VALUE.FORMAT=yyyy-MM-dd
ALL.TAB.FIELD.COMP.OPR=<=` and all rows with update_timestmp <=2018-04-22 are ready to be deleted.
2. To skips table options starting w/ ending w/ containing **name** can be sepcified using properties in the property file 
    ```
    ALL.TAB.EXCLUDE.BEGIN.WITH=
    ALL.TAB.EXCLUDE.ENDS.WITH=_LU
    ALL.TAB.EXCLUDE.CONTAINS=
    TAB.EXCLUDE.LIST=A,tab12
```
3. To skip a list of tables
    ```
    TAB.EXCLUDE.LIST=A,tab12
    ```
4. To specify custom filters for some/ all tables 
    ``` 
    CUSTOM.TAB.FILTER.FILE.NAME= `path_to_filenamethatcontainscustomerfiltersforatable`
        example: tab3,updt_timestmp,2018-04-19,AND 1<>1
  ```
### How the program works is as follows  

<hr>
<p align="left">
  <img src="https://github.com/gajoseph/purge/blob/master/testschemaER.jpg" width="700"/>
</p>
<hr>
<br>

<b>Level 0 table are:</b> tab1 <br>
<b>Level 1 child tables are:</b> tab12, tab2, tab3 <br>
<b>Level 2 child table/s are:</b> tab4 Child of (tab3, tab2)<br>

1) Process take the eligibale rows from tab1, top level paarent; fecthes only rows sepcified in property  <u>ALL.TAB.BATCH.SIZE=10 </u>; 
2) Then, associated rows from child tables  are pulled for example (tab1.col1 = tab12.tab1col1) or (tab12.tab1col1 is null or updatedatetime <= <u>ALL.TAB.FIELD.VALUE=2018-04-22 </u>); till it covers all the child tables; 
2) If child rows are not eligible to delete; process will recursively update all associated parent rows and set the status as not deletable; this is the upward process 
3) once all the eligible rows are collected; its starts deleting from bottom level to level up—it also check if the child rows can be deleted based on a linked to parent key id 
4) the deletes are fired by spinning up threads froma thread pool of 3 – is configurable 

<b>Use case:: </b>
1)	if a row in tab12 is updated w/ todays date its parent row in tab1 can’t be deleted; 
2)	tab1's child rows in tab2 and tab3 can't be deleted
3)	if one child row is not deleted able then the common parent can’t be deleted 
4)	Also, tab4 has 2 parent tables; a row in tab4 that has a parent row in tab2  and can't be deleted; then the other parent ie tab3 row cannot be also deleted;
5)	Also, the parent rows all all the way top can't be deleted as well as  all the child rows associated w/ each parent

