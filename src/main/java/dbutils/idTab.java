/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbutils;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.stream.Collectors;

import static dbutils.idrive.lPropertyReader;
import static dbutils.idrive.lSumBJCLogger;

/**
 *
 * @author tgaj2
 * record stuc will look like this
 * HB
 * Name: HB
 *
 */
public class idTab extends tfield{
    public fkids  parentId_pkids;
    public boolean noRows2Delete;// set this flag if qry returns nothing to delete
    public boolean isProcesedByaParent= false ;
    public boolean hasDelStatGenByAnotherParent= false ;


public idTab(String stabName) {
        super();
        this.setName(stabName);// schema name has to be stored
        parentId_pkids = new fkids();
        noRows2Delete= true;

    }


/*this is to give a pointer to parent.
*
* */
    public ids getfkid(String fkParentValue){
        return parentId_pkids.getFkObjforaPK(fkParentValue);

    }

    public boolean addidTab_ParentPK_Pkids(String fkParentValue , String pkChildValue, fkTable fk, boolean isDeletAble , ids pids ){
        //need to some cheks here if fk is null
/*
        if (fk ==null) {

            fk = new fkTable();
            fk.PKColumn = new contraintcolumn();
            fk.FkColumn = new contraintcolumn();
            fk.FkColumn.field = objDBts.objToSchema.gettable(this.getName()).getPKField();
            fk.PKColumn.CON_TABLE = this.getName();
            fk.PKColumn.CON_SCHEMA = objDBts.objToSchema.getName();// new to get this from caling function
        }

*/
        boolean alreadypresent = parentId_pkids.addfkid(fkParentValue , pkChildValue, fk, isDeletAble,  pids  );


    return alreadypresent;
    }

    public boolean setLoadedFlagForFKCol(fkTable fk){
        if (parentId_pkids.fkids().isEmpty())
                this.noRows2Delete = true;
        return parentId_pkids.setLoadedFlagForFKCol(fk);
    }


    public boolean findMatch( String PkColumnname ){
     //   if (parentId_pkids.fkids().isEmpty()) //means top level
        return parentId_pkids.findPkCol(PkColumnname );

    }


    public boolean addidTab_ParPK_ChldPkids_frmrsRow(ResultSet objrsrecQry , itable itab, fkTable fk   ) throws SQLException {
        boolean isDeleteable= true;
        tfield tfld;
        Date sDbFildVal;
        String sDf = lPropertyReader.getProperty("ALL.TAB.FIELD.VALUE.FORMAT");
        if ( sDf.equalsIgnoreCase(""))
        sDf= "yyyy-MM-dd";
        /*
        * Fixed the date issue  use yyyy-MM-dd*/
        for (int i =3; i<=objrsrecQry.getMetaData().getColumnCount(); i++)
        {
            try {
                tfld =itab.FieldByName( objrsrecQry.getMetaData().getColumnName(i).toUpperCase());
                sDbFildVal = objrsrecQry.getDate(i);
                if (tfld.getName()!= "")
                    if  ( (sDbFildVal ==null)
                            || (     sDbFildVal.getTime()  >=
                                    (new java.text.SimpleDateFormat(sDf)).parse(tfld.getFilterValue()).getTime()
                                )
                            )
                    {
                       System.out.println( "Db : " + objrsrecQry.getDate(i) + " Time = " + objrsrecQry.getDate(i).getTime()  + " Converted " + (new java.util.Date(sDbFildVal.getTime()))+ " Filter Value "
                               + tfld.getFilterValue() +" Time " +  (new java.text.SimpleDateFormat(sDf)).parse(tfld.getFilterValue()).getTime() + " Converted time"
                      )
                       ; isDeleteable= false;
                        tfld.finalize();
                        tfld= null;
                        break;
                    }

            } catch (Throwable throwable) {
                lSumBJCLogger.WriteErrorStack("",throwable );
            }
        }
        //return addidTab_ParentPK_Pkids (objrsrecQry.getString("FK"), objrsrecQry.getString("PK"), fk,isDeleteable );
        return isDeleteable;
        // find the paren and update // recursivly


    }


    public boolean isAlreadyAddedParentPKs(fkTable fk )
    {// cgeck if the PK(parent) and Fk(Child) already added
        return parentId_pkids.isParentPksAlreadyAddedtoItab( fk );
    }


    public String getPrepStatSQL(idTab pidTab, contraintcolumn PKColumn){ //NEED CHANGES pass the PKColumn
        String sids = "";
        String SqlpreparedStat= "";

        SqlpreparedStat= " ";
        for (fkid fk : this.parentId_pkids.fkids() ) {

            sids = sids + fk.Pks.stream().map(ids -> ids.Pkids.stream().map(Object::toString).collect(Collectors.joining("','")))
                    .collect(Collectors.joining("','", "'", "'"));

            //SqlpreparedStat = "(" + SqlpreparedStat.substring(0, SqlpreparedStat.length() - 1) + ")"; saving for prepared statement

        }
        if (!sids.equalsIgnoreCase("")) {
            SqlpreparedStat = SqlpreparedStat + ":" + "(" + sids + ")";
        }
        return SqlpreparedStat;
    }

    public fkid canPkDeleted( String pkKeyid){

        return this.parentId_pkids.canPkDeletedinFk(pkKeyid);


    }


 protected void finalize() throws Throwable {
    try {
        parentId_pkids.finalize();
         super.finalize();
       
    }
       catch (Exception e ){
           
       	}
       catch (Error e ){
       	}

    finally { 
    	super.finalize();     }
    }    
        
}

