## iPURGE
This program automatically purges(deletes) data in a database.The purged done based on how old the data is. Retention of data is based on condition applied to last updated date / last modified data field/column in a table. When a row in a table meets the condition then it is selected for deletion. After that other rules are also applied before it is finally deleted

1. To apply the condition set the property`ALL.TAB.FIELD`; for example `ALL.TAB.FIELD=updt_timestmp` means all the table has an updt_timestmp and `ALL.TAB.FIELD.VALUE=2018-04-22
ALL.TAB.FIELD.VALUE.FORMAT=yyyy-MM-dd
ALL.TAB.FIELD.COMP.OPR=<=` and all rows with update_timestmp <=2018-04-22 are ready to be deleted.
2. To skips table options starting w/ ending w/ containing **name** can be sepcified using the following properties in the property file 
    ```
    ALL.TAB.EXCLUDE.BEGIN.WITH=
    ALL.TAB.EXCLUDE.ENDS.WITH=_LU
    ALL.TAB.EXCLUDE.CONTAINS=
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
5. To sepcify custom join for tables 
    ```
    CUSTOM.TAB.JOIN.FILE.NAME= `path_to_fileContainingcustomjoins`
        example: tab5,tab2col1,tab2,tab2col1 **column tab2col1 in tab5 is joined to tab2col2 in tab2**
    ```
### How the program works is as follows  

----

<p align="left">
  <img src="https://github.com/gajoseph/purge/blob/master/testschemaER.jpg" width="700"/>
</p>
----

In the above picture the tables can be broadly classified as level0,1 2..Leveln tables where level 0 being the top level table meaning these are not dependant on any other table and level 1 the next child of level 0 table and so on.Listed below are tables by their levels
    ```   
    Level 0 table are: tab1 
    Level 1 child tables are: tab12, tab2, tab3
    Level 2 child table/s are: tab4 Child of (tab3, tab2)
    ```
   
1. Program deletes the rows in a table in bacthes. Batch size is set via **ALL.TAB.BATCH.SIZE**. The program take the eligibale rows from tab1, in this case top level parent; and  fetches only {n} of rows sepcified in property  **ALL.TAB.BATCH.SIZE=10** 
2. Then, associated rows from child tables are pulled for example (tab1.col1 = tab12.tab1col1) or (tab12.tab1col1 is null or updatedatetime <= **ALL.TAB.FIELD.VALUE=2018-04-22**; and continues until it covers all the child tables; 
2. If child rows are not eligible to delete; process will **recursively update all associated parent rows and set the status as not deletable**; this is the upward process 
3. once all the eligible rows are collected; its starts deleting from bottom level up to top level-- it also check if the child rows can be deleted based on a linked to parent key id. 
4. the deletes are fired by spinning up threads from a thread pool of 3 – is configurable. not done 
5. Once the 1st batch of data is deleted; program repeats steps {1..4} in the next batch/s till all the rows are deleted.

### Use case:: 
1)	if a row in tab12 is updated w/ todays date its parent row in tab1 can’t be deleted; 
2)	tab1's child rows in tab2 and tab3 can't be deleted
3)	if one child row is not deleted able then the common parent can’t be deleted 
4)	Also, tab4 has 2 parent tables; a row in tab4 that has a parent row in tab2  and can't be deleted; then the other parent ie tab3 row cannot be also deleted;
5)	Also, the parent rows all all the way top can't be deleted as well as  all the child rows associated w/ each parent

