<b>IPURGE</B>
1) purge a database based on conditions -- based on last updated date or modified date -- which is customizable in the NLP.properties

How the program works is assume this example 



<p align="left">
  <img src="https://github.com/gajoseph/purge/blob/master/testschemaER.jpg" width="700"/>
  
</p>

tab1 parent
 tab12 child level 1     tab2 child level1 ; tab3 level 1
                         tab4 Child of (tab3, tab2)

Test case when tab12 is changed
1) parent associated in Tab1 can't be deleted
2) tab1's child rows in tab2 and tab3 also can't be deleted
3) tab4 is an association of tab(3,2); of tab2 rows that is tab1' child is associated with another non qualified row ; then that's parent in tab1 can't be deleted
4) also, tab4 has 2 parent tables; a rows in tab4 has a parent row in tab2  that can't be deleted; then the other parent ie tab3 row cannot be also deleted;
also, the parent;; to all the awy top can't be deleted as well as all the child rows associated w/ each parent
tab4 row detail
tab4Pk1,tab2pk1,tab3pk1
tab4Pk1L is pK of tab4;tab2pk1: is a Fk in tab4 from tab2;tab3pk1 : is a FK in tab4 from tab3
if tab2pk1 can't be deleted; tab4Pk1 can't be deleted; that means tab3pk1  also can't be deleted; tab3's parent(tab1) also can't be deleted; if that parent key in tab1 tables has downstream rows then those also can't be deleted; this is done by recursive calls
=============================================================================================================================================================
