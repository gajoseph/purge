package dbutils;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.io.IOException;

import static dbutils.idrive.lSumBJCLogger;
import static dbutils.idrive.lPropertyReader;
import static dbutils.ipurge.objDBts;

public class idTabs extends tfield {

    private int fieldcount=0;
    private List<idTab> idtabs;
    public String scurrParent= "";
    public String SqlpreparedStat = "";
    public List<String>  Filterlines;



    public idTabs () {
        super();
        fieldcount=0;
        idtabs    = new ArrayList<idTab>();
        try {

            if (!lPropertyReader.getProperty("CUSTOM.TAB.FILTER.FILE.NAME").equalsIgnoreCase(""))
                Filterlines = Files.readAllLines(Paths.get(lPropertyReader.getProperty("CUSTOM.TAB.FILTER.FILE.NAME")));
        }
        catch (IOException e) {
            System.out.println(e);
        }

    }


    public List<idTab> getidtabs(){

        return idtabs;

    }

    public void updFilterValue(itable itab)
    {
        /* this proc will update and set the filter values; ideally there will be another proc that might call this to popluate from poreprty file/ input file tablename, fieldname, value
            use field by name
        */
        try {
            tfield tfl = itab.FieldByName(lPropertyReader.getProperty("ALL.TAB.FIELD"));
            tfl.setFilterValue(lPropertyReader.getProperty("ALL.TAB.FIELD.VALUE"));

        } catch (Throwable throwable) {
            lSumBJCLogger.WriteErrorStack("Throwable From FieldByName", throwable);
        }


    }
    public idTab getitTabByforlooop(String sTabName ){
        idTab lidTab = null;

        for (idTab tab : idtabs)
            if (tab.getName().equalsIgnoreCase(sTabName)){
                //if (tab.isAlreadyAddedParentPKs(fkTab))                    return tab;
                lidTab =  tab;
                break;
            }
            else

                lidTab = initidTab(sTabName);
        return lidTab;

    }

    public idTab getidTab(String sTabName ){

        idTab lidTab = null;
        if (idtabs.isEmpty())  lidTab =  new idTab(sTabName);
        List<idTab> tabs = doesTabExistforTabName( sTabName);

        if (tabs.isEmpty())
            lidTab = initidTab(sTabName);
         else
            lidTab =  tabs.get(0);

        return lidTab;

    }

    public idTab getIdTabByName(String sTabName )
    {
        List<idTab> tabs = doesTabExistforTabName( sTabName);
        idTab lidTab = null;
        if (!tabs.isEmpty())
            lidTab =  tabs.get(0);

        return lidTab;
    }


    public idTab initidTab(String sTabName){

        idTab initidTab =   new idTab(sTabName); // create a new instance for child tbale eg housebill--> houseBillSplit; here creating a new insatcne of houseBillSplit to store all ids
        updFilterValue(objDBts.objToSchema.gettable(sTabName));
        return initidTab ;
    }

    public List<idTab>  doesTabExistforTabName(String sTabName){
        return idtabs.stream().filter(idTab -> idTab.getName().equalsIgnoreCase(sTabName)).collect(Collectors.toList());

    }

    public idTab  getidTabByPkColumn(contraintcolumn objpkColumn)
    { //need to rewiet this need to put chekc for which primary kety it si asscoaietd w/

        idTab returnidTab  = null;
        List<idTab> tabs = doesTabExistforTabName(objpkColumn.CON_TABLE);
        if (tabs.isEmpty())
            return null;
        else
            return tabs.get(0);
/*
        for (idTab t: tabs)

            if (t.findMatch(objpkColumn.field.getName())
                    )

            {
                returnidTab = t;
                break;
            }
        return returnidTab;
        */
    }


    public idTab getidTabbyTabName_pkName(String sTabName, String tabPkColName){
        /*
        * returns idTab based on a tablename and Pk Col name
        * */
        idTab returnidTab  = null;
        List<idTab> tabs = doesTabExistforTabName( sTabName);

        for (idTab t: tabs)
            if (t.findMatch(tabPkColName)) {
                returnidTab = t;
                break;
            }
        return returnidTab;

    }

    /*
    *   objDBts.objToSchema.gettable(FKTableName).getPKField().getName()
                        , objDBts.objToSchema.getName()
                        , FKTableName
                        , pidTab
                        , FKTableColname
                        , iRowCnt
                        , fktab.FkColumn
                        , fktab.PKColumn
                        , fktab
    * */


    public idTab addTabData(idTab pidTab  ,contraintcolumn PKColumn,String sPkTabColName ,String sSchemaName  ,String sTabName
    ,int iCurrentDepth, String sFKTableColname, fkTable fkTab,  idTab objidTab )
    {
        ResultSet objrsrecQry;
        PreparedStatement _selStatement;
        String ssql;    // sql to get PK and FKS
        String sPrintHeader = "";
        int sPrintHeaderLen = 0;
        ids pids=null;

        getPrepStatSQL(pidTab, PKColumn);// of the format (?,?,?,?):1,2,4,5,  not need to generate if the parent is same
        ssql = getDbFetch_Limit(sPkTabColName,sSchemaName, sTabName , pidTab, sFKTableColname, iCurrentDepth); // added limit in case we want to run in batches limit 10
        /* if the parent de4ons't have rows deletable then child alos can't be deleted */
        System.out.println("Sql" + ssql );
        try {
            _selStatement = objDBts.conn1.prepareStatement(ssql);
            objrsrecQry = _selStatement.executeQuery(); // get the data from child table

            if (objrsrecQry != null) {
                sPrintHeader = sPrintHeader + String.format("\n|%40s | %40s|", sPkTabColName, sFKTableColname);
                sPrintHeaderLen = sPrintHeader.length();
                sPrintHeader = lSumBJCLogger.sTabPrint(sPrintHeader, sPrintHeaderLen, "-", "|");
                System.out.print(sPrintHeader);
                //objidTab = new idTab(sTabName);
            }
            else
            {
                System.out.print(" Query EMPTY");


            }
            while (objrsrecQry.next())
            {    // Set the header to print
                sPrintHeader = String.format("\n|%40s | %40s|", objrsrecQry.getString("PK"), objrsrecQry.getString("FK"));
                System.out.print(sPrintHeader);

                boolean isDeleteAble = objidTab.addidTab_ParPK_ChldPkids_frmrsRow(  objrsrecQry
                                                                                    , objDBts.objToSchema.gettable(sTabName)
                                                                                    , fkTab);// this wil, also update if the status delete or not based on the condition
                if (pidTab !=null) {
                    pids =  pidTab.getfkid(objrsrecQry.getString("FK"));
                }

                /* pass the pids to the function */
                boolean alreadypresent = objidTab.addidTab_ParentPK_Pkids(
                        objrsrecQry.getString("FK")
                        , objrsrecQry.getString("PK")
                        , fkTab
                        , isDeleteAble
                        , pids );

                if (alreadypresent)
                    break;

                if (pidTab != null)         // asscoiate the child rows
                    if (!isDeleteAble)      //updateDeletableStatus(fkTab.PKColumn , objrsrecQry.getString("FK"), false);// need to update all the parents
                        pids.deleteable= isDeleteAble;
                        /*updateDeletableStatus( pidTab, objrsrecQry.getString("FK"), false, fkTab); */

            }
            boolean bsetLoadedFlagForFKCol = objidTab.setLoadedFlagForFKCol(fkTab);
            //System.out.println( "Fisnihed setting bsetLoadedFlagForFKCol for " +objidTab.getName() + " for " + fkTab.PKColumn.CON_TABLE + fkTab.PKColumn.field.getName() );

        }
        catch (SQLException ex) {
            lSumBJCLogger.WriteErrorStack("addTabs " + ssql, ex);
        }
        finally {
            objrsrecQry = null;
            _selStatement = null;
            sPrintHeader = sPrintHeader + lSumBJCLogger.printLine(sPrintHeaderLen, "-", "|");

        }

        return objidTab;


    }

    public idTab addTabs(
            String sPkTabColName    /* primary Key of the table*/
            , String sSchemaName    /* Schema Name*/
            , String sTabName       /* Table name */
            , idTab pidTab          /* parent table id to which child tables PKs are added into */
            , String sFKTableColname    /* Child tables Fk Columname to Parent tables PK */
            , int iCurrentDepth
            , contraintcolumn FkColumn /* Fk Coumn object */
            , contraintcolumn PKColumn /* Pk Column object */
            , fkTable  fkTab           /* fk Table*/
            )
    {
        idTab objidTab =null;

        //Check if the table has been added
        objidTab = getidTab(sTabName);
       /* if (objidTab.isAlreadyAddedParentPKs(fkTab))
            return objidTab; // alreadyu we have added no need to go down any more what if thisis coming from doff path anotyjer parent
        */

        /*Load the data and return the ID
        * */
        objidTab  = addTabData(pidTab  ,PKColumn,sPkTabColName ,sSchemaName  ,sTabName  ,iCurrentDepth, sFKTableColname, fkTab,  objidTab );

        if (!(objidTab.parentId_pkids  == null)) {// if there list is epty only we need to
                pidTab = objidTab ;
                idtabs.add(objidTab );

        }
        return objidTab;
    }


    public String getDbFetch_Limit(
            String sPkTabColName // Pk of the table(child/topmost paremt)
            , String sSchemaName
            , String sTabName // Tab name
            , idTab pidTab // parent table id to which child tables PKs are added into
            , String sFKTableColname // Child tables Fk to Parent tables PK
            , int iCurrentDepth
            )
    {
        String[] Values = new String[2];
        String ssql = "";
        int iRowCount= Integer.parseInt(lPropertyReader.getProperty("ALL.TAB.BATCH.SIZE"));
        String stabFilterline= "";
        String[] atabFilterline;
        String ALL_TAB_FIELD = lPropertyReader.getProperty("ALL.TAB.FIELD");
        String ALL_TAB_FIELD_VALUE = lPropertyReader.getProperty("ALL.TAB.FIELD.VALUE");
        String sOCond = "";

        for (String str : Filterlines){
            if (str.contains(sTabName.toLowerCase()+ ",")) {
                stabFilterline = str;
                System.out.println("stabFilterline = " + stabFilterline );
                break;
            }
        }

        // split the line into an array
        if (!stabFilterline.equalsIgnoreCase(""))
        {
            atabFilterline = stabFilterline.split(",");

            if (!atabFilterline[1].equalsIgnoreCase(""))
                ALL_TAB_FIELD = atabFilterline[1];
            if (!atabFilterline[2].equalsIgnoreCase(""))
                ALL_TAB_FIELD_VALUE = atabFilterline[2];
            if (!atabFilterline[3].equalsIgnoreCase(""))
                sOCond =  atabFilterline[3];
        }



            if (SqlpreparedStat.contains(":"))
            {
            Values = SqlpreparedStat.split(":");
            ssql = String.format("Select %s as PK, %s as FK, %s  From %s.%s WHERE (%s in %s or ( %s is null and %s %s '%s' %s ))"
                    , sPkTabColName, sFKTableColname,  lPropertyReader.getProperty("ALL.TAB.FIELD")
                    , objDBts.objToSchema.getName(), sTabName, sFKTableColname, Values[1]
                    , sFKTableColname // sFKTableColname
                    , ALL_TAB_FIELD
                    , lPropertyReader.getProperty("ALL.TAB.FIELD.COMP.OPR")
                    , ALL_TAB_FIELD_VALUE
                    , sOCond
            );
        } else {
            /*
            * Top level tables FK will be its own PK
            * */
            ssql = String.format("Select %s as PK, %s as FK From %s.%s  where %s %s '%s' %s "//
                    , sPkTabColName
                    , sPkTabColName // here we are pulling PK as 2 columns making the FK as null
                    , objDBts.objToSchema.getName(), sTabName
                    , ALL_TAB_FIELD
                    , lPropertyReader.getProperty("ALL.TAB.FIELD.COMP.OPR")
                    , ALL_TAB_FIELD_VALUE
                    , sOCond
            );
            if (pidTab!=null)
                if (pidTab.noRows2Delete) // if there is not FK--- parent has no PKs ready to be deleted
                ssql = String.format("Select %s as PK, %s as FK From %s.%s  where %s %s '%s'  and %s is null  %s"//
                        , sPkTabColName
                        , sPkTabColName // here we are pulling PK as 2 columns making the FK as null
                        , objDBts.objToSchema.getName(), sTabName
                        , ALL_TAB_FIELD
                        , lPropertyReader.getProperty("ALL.TAB.FIELD.COMP.OPR")
                        , ALL_TAB_FIELD_VALUE
                        , sFKTableColname
                        , sOCond
                );



            // only for top level we are adding the limit; adding limit to shildren won't work
            if (dbtype.db.POSTGRES.name().equals("POSTGRES"))
                ssql = ssql + String.format(" Limit %d", iRowCount);
            else if (dbtype.db.DB2.name().equals("DB2"))
                ssql = ssql + String.format(" Fetch first %d rows only ", iRowCount);
            else
                ssql = ssql + String.format(" Limit %d", iRowCount);

        }
        return ssql;

    }



    public boolean isDeletAble(ResultSet  objrsrecQry , String tabName) throws SQLException{
        // Need a beter way thought passign the recordset and getting the correct values
        boolean isDeleteable= true;
        itable itab = objDBts.objToSchema.gettable(tabName);
        tfield tfld;

        for (int i =3; i<=objrsrecQry.getMetaData().getColumnCount(); i++)
        {
            try {
                tfld =itab.FieldByName( objrsrecQry.getMetaData().getColumnName(i).toUpperCase());
                if (tfld.getName()!= "")
                    if  ( (objrsrecQry.getDate(i)==null)
                            || objrsrecQry.getDate(i).getTime() <=
                            (new java.text.SimpleDateFormat("YYYY/MM/DD")).parse(tfld.getFilterValue()).getTime() )
                    {
                        isDeleteable= true;
                        tfld.finalize();
                        tfld= null;
                        break;
                    }

            } catch (Throwable throwable) {
                lSumBJCLogger.WriteErrorStack("Throwable From FieldByName", throwable);


                ;
            }
        }

        itab= null;
        return isDeleteable;

    }

    public String getPrepStatSQL(idTab pidTab, contraintcolumn PKColumn){
        String sids = "";
        if (pidTab!= null){
            // check parent is same as previuos childs parent
            if (!(pidTab.getName().equalsIgnoreCase(scurrParent)))
            {
                scurrParent =pidTab.getName();
                SqlpreparedStat= "";
                SqlpreparedStat = pidTab.getPrepStatSQL(pidTab, PKColumn);
            }
        }
        return SqlpreparedStat;
    }




    public boolean CanDeletableStatus_new(idTab chldTab ){
        /*
         * for a given idTab(most likesly child; check parIds which is is of parent's ids object check and see if it deletable; recursively go all the way to the top level
         * Parent could have mutiple childrens and after loading a child another child's records might not be able to delete and when that happens; parent's deletable status =false;)*/
        boolean CanDeletableStatus =true;
        idTab  Objparidtab=null;
        try {
            if (chldTab !=null)
                for (fkid fk1: chldTab.parentId_pkids.fkids()) {
                    for (ids id : fk1.Pks) {
                        if (id.deleteable == true)
                            if (id.parIds != null)
                                CanChlPKDeletedbasedonParentFK_new(id, id.parIds  );//
                    }
                }
        }
        catch (Exception e) {
            lSumBJCLogger.WriteErrorStack("unexpected invocation exception CanDeletableStatus_new:", e);
        } catch (Error e) {
            lSumBJCLogger.WriteErrorStack("unexpected invocation exception CanDeletableStatus_new:", e);
        } finally {////System.out.println("End of Funciton FieldBYname ");

        }
        return true;
    }


    public void CanChlPKDeletedbasedonParentFK_new(ids ChldIDs, ids parIds ){
        /*
         * from the previous function we go the parent key; now traverse thru all the parents and see if it can be deleted */
        //idTab  Objparidtab=null;
        if (!parIds.isDeleteable()){
            ChldIDs.deleteable= false;
        }
        else // go recuroviles
            if (parIds.parIds !=null)// reached top level
                CanChlPKDeletedbasedonParentFK_new(ChldIDs, parIds.parIds  );

    }


    public idTab getidTabByTabName( String tabName ){

        Optional<idTab>  t= idtabs.stream()
                .filter(u -> u.getName().equalsIgnoreCase(tabName))
                .findFirst();
        if (t.isPresent())    return t.get();
        else return                   null;

    }

    public void zzupdateDeletableStatus(idTab chldTab, String chldFKvalue, boolean isDeleteAble, fkTable fk ){
        /*
         *
         * */
        String sprntidTabName = "";
        try {

            if (chldTab !=null)
                for (fkid fk1 : chldTab.parentId_pkids.fkids() )
                    for(ids myli : fk1.Pks)
                        if (myli.FkID.equalsIgnoreCase(chldFKvalue)) {
                            myli.deleteable = isDeleteAble;
                            // call recursived
                            if (fk1.PKColumn.CON_TABLE!="null")
                                zzupdateDeletableStatus( getidTabByTabName(fk1.PKColumn.CON_TABLE)
                                        , myli.FkID
                                        , isDeleteAble
                                        ,null);
                            break;
                        }
        }
        catch (Exception e) {
            lSumBJCLogger.WriteErrorStack("Excpetion  updateDeletableStatus", e);
        } catch (Error e) {
            lSumBJCLogger.WriteErrorStack("", e);
        } finally {////System.out.println("End of Funciton FieldBYname ");

        }
    }

    public void CanChlPKDeletedbasedonParentFK(String Parentid, contraintcolumn Objpkcolumn, ids ChldIDs, idTab Objparidtab){
        /*
         * from the previous function we go the parent key; now traverse thru all the parents and see if it can be deleted */
        //idTab  Objparidtab=null;
        boolean breturn = true;
        if (Objpkcolumn==null ) return;
        //Objparidtab = getidTabByPkColumn(Objpkcolumn);// TAKE TO PARENT FUNCTION
        fkid fk = Objparidtab.canPkDeleted(Parentid);// returns and fid

        if (fk.Pks!=null)// we fnd something
        {
            ids parentId = fk.Pks.get(0);
            if (!parentId.isDeleteable()){
                ChldIDs.deleteable= false;
            }
            else // go recuroviles
                if (fk.PKColumn.CON_TABLE !="null")// reached top level
                    CanChlPKDeletedbasedonParentFK(parentId.FkID, fk.PKColumn, parentId, getidTabByPkColumn(fk.PKColumn));
        }
        //fk.finalize();
    }


    public boolean zzCanDeletableStatus(idTab chldTab ){
        /*
         * for a given idTab(most likesly child; find its parent's PKColumn object and check if parents delete status is true;
         * Parent could have mutiple childrens and after loading a child another child's records might not be able to delete and when that happens; parent's deleteable status =false;)*/
        boolean CanDeletableStatus =true;
        idTab  Objparidtab=null;
        try {
            /*
            FKTableName = fktab.FkColumn.CON_TABLE;
                PKTableName = fktab.PKColumn.CON_TABLE;
                FKTableColname =  fktab.FkColumn.field.getName();
                PKTableColname =  fktab.PKColumn.field.getName();
             */
            if (chldTab !=null)
                for (fkid fk1: chldTab.parentId_pkids.fkids()) {
                    Objparidtab = getidTabByPkColumn(fk1.PKColumn);
                    for (ids id : fk1.Pks) {
                        if (id.deleteable == true)
                            if (fk1.PKColumn.CON_TABLE != "null")
                                CanChlPKDeletedbasedonParentFK(id.FkID, fk1.PKColumn, id, Objparidtab);// call this which is reset so i will have to check again if its still true then delete

                    }
                }
        }
        catch (Exception e) {
            throw new RuntimeException("unexpected invocation exception: " + e.getMessage());
        } catch (Error e) {
            throw new Error("unexpected Error occured : " + e.getMessage());
        } finally {////System.out.println("End of Funciton FieldBYname ");

        }
        return true;
    }



}
