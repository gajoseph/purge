/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbutils;

import java.sql.DatabaseMetaData;

import bj.BJCLogger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static dbutils.idrive.lSumBJCLogger;
import java.sql.JDBCType;
import java.sql.PreparedStatement;

/**
 * @author TGAJ2
 */
public class Dbtables {

    /*
     These could be copied to impelmenation side
     */
    public static String tablename;
    public String tablocationType = "";
    public ischema objToSchema; // referecme from the calling class
    public ischema objFrmSchema; // referecme from the calling class
    public Connection conn1;// reference
    public boolean bprintFK = true;
    public List<itable> tabList;

    public Dbtables() {
        super();
    }

    public String joinCon(itable it, String s2Sch) {
        String a = "";
        if (it.sJoincondition.indexOf("JOIN") > 0) {
            a = "(" + it.sJoincondition + ")";
        } else {
            a = it.sJoincondition;
        }
       //a = s2Sch + "." + it.getName();

        System.out.println("ddddddd  + " + a + " has join cond " + it.sJoincondition);
        return a;
    }

    public String PKConditionNOtEQ( itable frtab, String salais){
        StringBuffer PKConditionNOtEQ = new StringBuffer().append("  ");
        for( tfield tf : frtab.getPKFields()) 
        {
            if (tf.isPrimary())
               PKConditionNOtEQ.append( " and "  ).append(salais).append(".").append(tf.getName());
               PKConditionNOtEQ.append( " <> "  ).append(frtab.getName()).append(".").append(tf.getName()); 
               PKConditionNOtEQ.append("\n");
            
        }
    
    return PKConditionNOtEQ.toString();
    }
    
    
    public String loopthurFindOthChilds(fkTable pkTab, itable frtab)                                
    {
        StringBuffer loopthurFindOthChilds = new StringBuffer().append("  ");
        int itableCnt = 0;
        
        for (itable itab : objToSchema.gettables()) 
        {
            if (frtab.getName() != itab.getName())
            for (fkTable fktab : itab.fktables.gettables()) 
            {
                
                if (fktab.PKColumn.CON_TABLE.equalsIgnoreCase(pkTab.PKColumn.CON_TABLE)) // found the another chuild 
                { itableCnt++;
                    loopthurFindOthChilds.append(" \n LEFT JOIN ");// left join 
                    loopthurFindOthChilds.append( objFrmSchema.getName()).append("."); 
                    loopthurFindOthChilds.append(fktab.getName());
                    loopthurFindOthChilds.append(" ");
                    loopthurFindOthChilds.append(fktab.getName()).append( "_").append( fktab.FkColumn.field.getName());// alias 
                    loopthurFindOthChilds.append(" ON "  + itab.getName() + "_" + fktab.FkColumn.field.getName());
                    loopthurFindOthChilds.append("."  + fktab.FkColumn.field.getName());
                    loopthurFindOthChilds.append("=");
                   loopthurFindOthChilds.append( fktab.PKColumn.CON_TABLE +
                           "_" + fktab.FkColumn.field.getName() + ".");
                   loopthurFindOthChilds.append( fktab.PKColumn.field.getName());
                   // add the AND COnidtion for data 
                   loopthurFindOthChilds.append( " \t AND ");
                   loopthurFindOthChilds.append( itab.getName()).append( "_").append( fktab.FkColumn.field.getName())
                                                        .append(".updt_timestmp < '2016-01-01'"  ) ;
                   loopthurFindOthChilds.append( "\n ");
                   // RECURISVELY CALL THIS 
                   System.out.println("fktab " + fktab.getName()  + " itab " + objToSchema.TableByName(pkTab.PKColumn.CON_TABLE).getName());
                   loopthurFindOthChilds(fktab,   
                           objToSchema.TableByName(pkTab.PKColumn.CON_TABLE)
                           
                              );
                           
                }
            }
            
        }
        
        loopthurFindOthChilds.insert(0, 
                objFrmSchema.getName() + "."+ pkTab.PKColumn.CON_TABLE +" " +pkTab.PKColumn.CON_TABLE + "_" + pkTab.FkColumn.field.getName() + " \n " );
        if (itableCnt > 1 ) {
            loopthurFindOthChilds.insert(0, "\n JOIN  ("  ) ;
            loopthurFindOthChilds.append( " )  \n "      ) ;
        }
        else 
            loopthurFindOthChilds.insert(0, " \n JOIN  "  ) ;
                
        
    
        return loopthurFindOthChilds.toString();
    }
    
    
// get JOIN CLuae when there is limit pull 
// filter from the destination Db and get only data for those rows 
// Select * from tab1 a left join    
    public String getRecuriveFKs1(itable itab, String s2Sch, int iRowCnt) {
        String strJoin = "";
        String CON_TABLE = ""; // check if there is referecne to the same table of 2 columns 
        String strchldJOinCond = "";
        String sfltrCond = "";
        StringBuilder sWhrCond = new StringBuilder("");
        String inclause = "";
        String FKTableName = "";// is the table table which refrences the primary/Unique  key on another table 
        String PKTableName = ""; // is the table name which is refrenced by a FK table name 
        String FKTableColname = "";
        String PKTableColname = "";
        
        StringBuilder sbJoin = new StringBuilder("");

        if (itab.fktables != null) 
        {
            for (fkTable fktab : itab.fktables.gettables()) 
            {
                FKTableName = fktab.FkColumn.CON_TABLE;
                PKTableName = fktab.PKColumn.CON_TABLE;
                FKTableColname =  fktab.FkColumn.field.getName();
                PKTableColname =  fktab.PKColumn.field.getName();
                inclause = "";
                
                if (fktab.hasDups) 
                {
                    strJoin = strJoin + " JOIN ("
                            + objFrmSchema.getName() + "."
                            +  joinCon(objToSchema.gettable(PKTableName) , s2Sch)  
                            + loopthurFindOthChilds(fktab, itab) 
                            + "\n ) "
                            + "  /*Condition */  " //+ PKTableName
                            + " ON " + FKTableName + "."
                            + FKTableColname + " =  "
                             +PKTableName + "_" + FKTableColname
                            + "."+ FKTableColname;
                } 
                else 
                {
                    sbJoin.append(loopthurFindOthChilds(fktab, itab) ).append(" /*condition*/ " );
                    sbJoin.append( " ON " ).append( FKTableName) .append( ".").append( FKTableColname );
                    sbJoin.append( " =  ").append( PKTableName).append("_").append(FKTableColname).append( ".").append(PKTableColname);
                    sbJoin.append( " AND ").append(PKTableName).append("_" ).append(FKTableColname).append(".updt_timestmp < '2016-01-01'");
                    sbJoin.append( " AND " ).append( FKTableName).append(  ".updt_timestmp < '2016-01-01'");                                  
                    sbJoin.append( " \n AND NOT EXISTS \n");
                    sbJoin.append( "( select 1 from  ");
                                        sbJoin.append( objFrmSchema.getName()) .append( ".");
                                        sbJoin.append(  itab.getName() ).append(  " ").append(  FKTableColname); // alias 
                                        sbJoin.append(  " WHERE " );
                                        sbJoin.append(  FKTableColname);
                                        sbJoin.append(  "." ).append(FKTableColname).append("=");
                                        sbJoin.append(FKTableName).append( ".");
                                        sbJoin.append( FKTableColname);
                                               //" add the PK <> PK "
                                        sbJoin.append( " AND " ).append(FKTableColname).append(".updt_timestmp < '2016-01-01' ");
                                        sbJoin.append(  PKConditionNOtEQ(itab, FKTableColname));
                    sbJoin.append(  "\n ) " );
                    sbJoin.append(  "  ");
                }
            }    // end for         
            // join the select and where clauses 
           // strJoin = strJoin + " " + sWhrCond.toString();
        }
        else 
        {
            //itab.sJoincondition =s2Sch + "." + itab.getName();
            //  System.out.println("Recurisvelu calling but missede *****" +  itab.getName() );

        }
        strJoin = sbJoin.toString();
        if (strJoin.trim() != "") {
            itab.sJoincondition = s2Sch + "." + itab.getName() + " " + "  " + strJoin + "   " + "  /* "
                    + itab.getName() + " */";
        } else {
            itab.sJoincondition = s2Sch + "." + itab.getName();
        }
        
        
        
        return strJoin;
    }

   //get limit # of FKs from Fk table 
/*select  a.transportation_mode_type_code ,  b.type_code, *   From tms.manifest a
     left join tms.transportation_mode_type_lu  b on a.transportation_mode_type_code = b.type_code 
     where (transportation_mode_type_code in( 'EXPRESS') or  a.transportation_mode_type_code is null)
        
     */
    public String getleftjoinSwhereCond(String sFrgnTablName, String sFrgnTabCol, int iRowCnt) {
        StringBuilder sSelFrgnTab = new StringBuilder(" ");
        String sWhrCond = "";
        List<String> FKSQL = new ArrayList<String>();

        ResultSet getFrgnTabColValue = null;
        PreparedStatement _selStatement = null;

        try {
            sSelFrgnTab.append("Select ").append(sFrgnTablName).append(".").append(sFrgnTabCol).append(" from ");
            sSelFrgnTab.append(objFrmSchema.getName()).append(".").append(sFrgnTablName);
            // Check the Db type 
            if (dbtype.db.POSTGRES.name().equals("POSTGRES")) {
                sSelFrgnTab.append(" Limit ").append(iRowCnt);
            } else if (dbtype.db.DB2.name().equals("DB2")) {
                sSelFrgnTab.append(" fetch first  ").append(iRowCnt).append(" rows only ");
            } else {
                sSelFrgnTab.append(" Limit ").append(iRowCnt);
            }
            //      
            _selStatement = conn1.prepareStatement(sSelFrgnTab.toString());
            getFrgnTabColValue = _selStatement.executeQuery();

            while (getFrgnTabColValue.next()) {
                FKSQL.add(getFrgnTabColValue.getString(1));
            }

            sWhrCond = FKSQL.stream().map(Object::toString)
                    .collect(Collectors.joining("','"));
            if (!sWhrCond.equals("")) {
                sWhrCond = "'" + sWhrCond + "'";

            }

        } catch (Error e) {
            lSumBJCLogger.WriteErrorStack("", e);
        } catch (SQLException ex) {
            //Logger.getLogger(Dbtables.class.getName()).log(Level.SEVERE, null, ex);
            lSumBJCLogger.WriteErrorStack("", ex);
        } finally {
            return sWhrCond;

        }

    }

    void getCreatePrivs(ResultSet srcTabPks, String sTableName, itable src, itable des) throws Throwable {

        StringBuilder sPkname = new StringBuilder(" ");
        int i = 0;
        try {
            while (srcTabPks.next()) {
                i++;
                sPkname.append("GRANT ");
                sPkname.append(srcTabPks.getString("PRIVILEGE"));
                sPkname.append(" ON ");
                sPkname.append(des.OwnerName);
                sPkname.append(".");
                sPkname.append(sTableName).append(" TO ").append(srcTabPks.getString("GRANTEE")).append(";\n");

                des.setGrants(sPkname.toString());// could be mutiple grants
                //desttable.setGrants(sPkname.toString());

            }
        } catch (Exception ex) {
            lSumBJCLogger.WriteErrorStack("", ex);

        } catch (Error ex) {
            lSumBJCLogger.WriteErrorStack("", ex);
        } finally {
            lSumBJCLogger.WriteLog("==================================================================");

        }
    }

    void getCreatePksDDL(ResultSet srcTabPks, String sTableName, itable src, itable des) throws Throwable {
        String sPkDDL = "(";
        String sPkColname = "";
        String sPK_NAME = "";
        int i = 0;
        try {
            while (srcTabPks.next()) {
                i++;

                sPkColname = srcTabPks.getString("COLUMN_NAME");
                sPK_NAME = srcTabPks.getString("PK_NAME");

                sPkDDL = sPkDDL + sPkColname + ",";

                if (sPK_NAME != null) {
                    src.FieldByName(sPkColname).setPrimary(true);
                    des.FieldByName(sPkColname).setPrimary(true);
                }
                //              lSumBJCLogger.WriteLog(sPkname);
            }
        } catch (Exception ex) {
            lSumBJCLogger.WriteErrorStack("", ex);

        } catch (Error ex) {
            lSumBJCLogger.WriteErrorStack("", ex);
        } finally {
            //lSumBJCLogger.WriteLog(this.GetDDL());
            if (sPkDDL.equalsIgnoreCase("(")) {
                // no primary key

                sPkDDL = "***** NO PK DEFINED ****";

            } else {
                            //need to loop thu the fields collection and get the primary keys 

                sPkDDL = sPkDDL.substring(0, sPkDDL.lastIndexOf(","));
                sPkDDL = "Alter TABLE " + des.OwnerName + "." + des.getName()
                        + "\t ADD CONSTRAINT "
                        + "\t" + sPK_NAME
                        + " PRIMARY KEY "
                        + " " + sPkDDL + ");";
                des.PKDDL = sPkDDL;
                des.PK_NAME = sPK_NAME;
            }
        }
    }

    public void setToDbCreateTable(ResultSet srcTabCol, String sSchema, String DbTabLocationType,
            itable Src, itable des) {

        int i = 0;
        int jbctype= 0;
        try {
            while (srcTabCol.next()) {

				//if (DbTabLocationType.equalsIgnoreCase("SOURCE"))
                {
                    i++;

                    Src.AddField(srcTabCol.getString("COLUMN_NAME"), srcTabCol.getShort("DATA_TYPE") + "" //srcTabCol.getString("TYPE_NAME")
                            , srcTabCol.getString("COLUMN_DEF"), i, srcTabCol.getInt("NULLABLE"), srcTabCol.getInt("COLUMN_SIZE"), srcTabCol.getString("REMARKS")
                            , srcTabCol.getInt("DECIMAL_DIGITS")
                    );
                    jbctype = srcTabCol.getShort("DATA_TYPE");
                     
                    
                    //  if (DbTabLocationType.equalsIgnoreCase("DESTINATION"))
                    des.AddField(srcTabCol.getString("COLUMN_NAME"), srcTabCol.getShort("DATA_TYPE") + "" // tricky took 30 minutes to resolve the enum CRAP
                            , srcTabCol.getString("COLUMN_DEF"), i, srcTabCol.getInt("NULLABLE"), srcTabCol.getInt("COLUMN_SIZE")
                            , srcTabCol.getString("REMARKS")
                            , srcTabCol.getInt("DECIMAL_DIGITS")
                    );

                    System.out.println(
                            srcTabCol.getString("COLUMN_NAME") + "=COLUMN_NAME "
                            + srcTabCol.getString("TYPE_NAME") + "=TYpename : "
                            + srcTabCol.getString("DATA_TYPE") + "=DATA_TYPE : "
                            + srcTabCol.getString("COLUMN_SIZE") + "=COLUMN_SIZE  : "
                    );
                }
            }
        } catch (Exception ex) {
            lSumBJCLogger.WriteErrorStack("setToDbCreateTable: " + srcTabCol + ":" +  sSchema, ex);

        } finally {
            lSumBJCLogger.WriteLog("des.GetDDL()");

        }
    }

    void getCreateIdxDDL(ResultSet srcTabPks, String sTableName,
            itable Src, itable des
    ) throws Throwable {
        int i = 0;
        tfield a;
        try {
            des.Indexes = new indexes();
            while (srcTabPks.next()) {
                i++;

                String crap = "";
                switch (srcTabPks.getShort("TYPE")) {
                    case DatabaseMetaData.tableIndexClustered:
                        crap = ">>tableIndexClustered";
                    case DatabaseMetaData.tableIndexHashed:
                        crap = ">>tableIndexHashed";
                    case DatabaseMetaData.tableIndexOther:
                        crap = "tableIndexOther";
                    case DatabaseMetaData.tableIndexStatistic:
                        crap = "tableIndexStatistic";
                    default:
                        crap = "tableIndexOther";
                }

                lSumBJCLogger.WriteLog("---" + srcTabPks.getString("INDEX_NAME")
                        + " <INdex Column>  " + srcTabPks.getString("COLUMN_NAME")
                        + " <IDX TYPE>  " + srcTabPks.getString("TYPE")
                        + " <IDX TYPE STRING >  " + srcTabPks.getShort("TYPE")
                        + " CARDINALITY " + srcTabPks.getString("CARDINALITY")
                        + "<IDX TYPE>" + crap
                );
				// Base on carinality we could see how many rows are there in the table

                //   desttable.FieldByName(srcTabPks.getString("COLUMN_NAME"))
                if (srcTabPks.getString("INDEX_NAME") != null) {
                    if (des.Indexes.IndexExists(srcTabPks.getString("INDEX_NAME"))) {// index exists then get field details from table -- so index is on those fields
                        a = des
                                .FieldByName(
                                        srcTabPks
                                        .getString("COLUMN_NAME")
                                );
                        //a.setName(sTableName);
                        des.Indexes.getIndex(
                                srcTabPks.getString("INDEX_NAME")
                        ).AddIndexField(
                                a, srcTabPks.getString("ORDINAL_POSITION"), srcTabPks.getString("ASC_OR_DESC")
                        );
                    } else {
                        // no index
                        iindex d = new iindex();
                        d.OwnerName = sTableName;
                        d.setName(srcTabPks.getString("INDEX_NAME"));

                        d.setType(srcTabPks.getString("NON_UNIQUE"));
                        d.CARDINALITY = srcTabPks.getString("CARDINALITY");
                        d.FILTER_CONDITION = srcTabPks.getString("FILTER_CONDITION");
                        d.IDXTYPE = crap;
                        d.PAGES = srcTabPks.getString("PAGES");

                        d.AddIndexField(des.FieldByName(srcTabPks.getString("COLUMN_NAME")), srcTabPks.getString("ORDINAL_POSITION"), srcTabPks.getString("ASC_OR_DESC")
                        );
                        des.Indexes
                                .AddIndex(
                                        d
                                );

                    }
                }
            }
        } catch (Exception ex) {

            lSumBJCLogger.WriteErrorStack("", ex);
        } catch (Error ex) {
            lSumBJCLogger.WriteErrorStack("", ex);
        } finally {
            lSumBJCLogger.WriteLog("");

        }
    }

    // ran into an issue where from db2 was getting view deaild
    public boolean isDbtable(String sSchemaName, String sTabName) {
        boolean isDbtable = false;
        ResultSet tabexists = null;
        String lTabName = sTabName;
        
        if (objToSchema.getDbType()== dbtype.db.POSTGRES ) 
            lTabName = lTabName.toLowerCase();
        
        try {
            tabexists = conn1.getMetaData()
                    .getTables(null, sSchemaName// from schema anme
                            , lTabName, new String[]{"TABLE", "ALIAS"}
                    );
            isDbtable = tabexists.next();
            tabexists.close();
        } catch (SQLException ex) {
            //Logger.getLogger(Dbtables.class.getName()).log(Level.SEVERE, null, ex);
            lSumBJCLogger.WriteErrorStack("", ex);
        } finally {

            tabexists = null;

        }
        return isDbtable;
    }

    void GetChildTables(ResultSet srcChldTabs, String sTableName, String sToSchema, itable src, itable des) {
        int i = 0;
        lSumBJCLogger.WriteLog("Child tables \n\n");
        des.fktables = new fkTables();

        try {
            while (srcChldTabs.next()) {
                i++;
                fkTable fk = new fkTable();
                lSumBJCLogger.WriteLog("   " + srcChldTabs.getString("FKCOLUMN_NAME")
                        + des.FieldByName(srcChldTabs.getString("FKCOLUMN_NAME")).getName()
                );
                //fk.FkColumn.field = desttable.FieldByName(srcChldTabs.getString("FKCOLUMN_NAME"));
                //fk.Update_rule = srcChldTabs.getShort("UPDATE_RULE");
                fk.Update_rule = dbtype.reference_option.values()[srcChldTabs.getInt("UPDATE_RULE")] ;
                fk.Delete_rule = dbtype.reference_option.values()[srcChldTabs.getInt("DELETE_RULE")] ;
                
                
                //fk.Delete_rule = srcChldTabs.getShort("DELETE_RULE");
                fk.deferrability = "" + srcChldTabs.getShort("DEFERRABILITY");

                fk.AddFKField(des.FieldByName(srcChldTabs.getString("FKCOLUMN_NAME")), sTableName, srcChldTabs.getString("FKTABLE_SCHEM"), srcChldTabs.getString("FK_NAME")
                );
                // if can;t fin dparentr load the parent table and its details
                lSumBJCLogger.WriteLog("Add parenttable  in Schema:"
                        + srcChldTabs.getString("PKTABLE_SCHEM") + "."
                        + srcChldTabs.getString("PKTABLE_NAME")
                        + " to " + sToSchema
                        + " \t Table referencing:" + sTableName
                        + "\t <UPDATE_RULE>" + fk.Update_rule + "</UPDATE_RULE>"
                        + "\t <DELETE_RULE>" + fk.Delete_rule + "</DELETE_RULE>"
                        + "\t <DEFERRABILITY>" + fk.deferrability + "</DEFERRABILITY>"
                        + "\n UUU=" + srcChldTabs.getString("UPDATE_RULE")
                );

                if (isDbtable(srcChldTabs.getString("PKTABLE_SCHEM"), srcChldTabs.getString("PKTABLE_NAME")))// check if FK table and not a view ; db2 gave views back
                {
                    getTabDetails(srcChldTabs.getString("PKTABLE_SCHEM"), sToSchema, srcChldTabs.getString("PKTABLE_NAME")
                    );
                    itable a;
                    a = objToSchema.TableByName(srcChldTabs.getString("PKTABLE_NAME"));
                    tfield t;
                    t = a.FieldByName(srcChldTabs.getString("PKCOLUMN_NAME"));
                    // compare if the datatypes; if not same print comments
                    fk.AddPKField(t, srcChldTabs.getString("PKTABLE_NAME"), srcChldTabs.getString("PKTABLE_SCHEM"), srcChldTabs.getString("PK_NAME")
                    );
                    if (!Objects.equals(t.getType(), des.FieldByName(fk.FkColumn.field.getName()).getType())) {

                        fk.hasIssues = true;
                        fk.shasIssues = "PK and FK mismatch:: " + t.GetDDL()
                                + " --  FK details "
                                + (des.FieldByName(fk.FkColumn.field.getName()).GetDDL());
                    }

                    des.fktables.AddFkTable(fk);
					// After this Generate the JOIN SQL

                }

            }
        } catch (Exception ex) {
            lSumBJCLogger.WriteErrorStack("", ex);
        } catch (Error ex) {
            lSumBJCLogger.WriteErrorStack("", ex);
        } catch (Throwable ex) {
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
        } finally {
            lSumBJCLogger.WriteLog("End Child tables \n\n");

        }

    }

    public void getTabDetails(String sFrmSchema, String s2Schema, String sTable) throws SQLException, Throwable {
        ResultSet rsColumns = null;
        String strTabCrtStat = "";

        if (!objToSchema.TableByName(sTable).getName().equalsIgnoreCase("")) {
            lSumBJCLogger.WriteLog("***** ALREADY LOADED *****" + objToSchema.TableByName(sTable));
            return;
        }

        tablename = sTable;
        itable srctable = new itable();
        itable desttable = new itable();

        srctable.setName(sTable);
        desttable.setName(sTable);

        desttable.setName(sTable);
        srctable.setDbType(        objFrmSchema.getDbType() ) ;// setting the Db source from schema 
        
        desttable.setDbType(       objToSchema.getDbType()     );
        
        
        
        desttable.OwnerName = s2Schema;
        //--Geo--C-- get the Columns for a table in that schema
        rsColumns = conn1.getMetaData()
                .getColumns(null, sFrmSchema, sTable, null);

        setToDbCreateTable(rsColumns, "", "", srctable, desttable
        );

        getCreatePrivs(conn1.getMetaData()
                .getTablePrivileges(null, sFrmSchema, sTable), sTable, srctable, desttable
        );
        // this is pull PHK info from src and set them up in DestTable
        getCreatePksDDL(conn1.getMetaData().getPrimaryKeys(null, sFrmSchema, sTable), sTable // tablename
                , srctable, desttable
        );

        getCreateIdxDDL(conn1.getMetaData().getIndexInfo(null, sFrmSchema, sTable, false, true), sTable // tablename
                , srctable, desttable
        );

        GetChildTables(conn1.getMetaData().getImportedKeys(null, sFrmSchema, sTable
        ), sTable // tablename
                , s2Schema // need this to add the child tables into the list
                , srctable, desttable
        );

        objFrmSchema.AddTable(sTable, " ", srctable);

        objToSchema.AddTable(sTable, " ", desttable);

        lSumBJCLogger.setSYSTEM_LOG_OUT(true);

        strTabCrtStat
                = "\n\n "
                + "/*============================================================================\n"
                + "-- TABLE :" + s2Schema + "." + sTable + "\n"
                + "============================================================================*/"
                + "\n\n"
                + "create table  " + s2Schema + "." + sTable
                + "\n (" + desttable.getTableDDL().substring(0, desttable.getTableDDL().length() - 2) + "\n);\n\n"
                + "/*============================================================================ \n"
                + "-- PRIMARY KEY CONTRAINT:" + s2Schema + "." + sTable + "\n"
                + "============================================================================*/ \n"
                + desttable.PKDDL
                + "\n"
                + "/*============================================================================ \n"
                + "-- GRANT :" + s2Schema + "." + sTable + "\n"
                + "============================================================================*/ \n"
                + desttable.Grants
                + "\n"
                + "/*============================================================================ \n"
                + "-- FOREIGN KEY CONSTRAINT :" + s2Schema + "." + sTable + "\n"
                + "============================================================================*/ \n";

        List<String> FKSQL
                = desttable.fktables.gettables().stream()
                .map(e -> ((fkTable) e).GetDDL(s2Schema))
                .collect(Collectors.toList());

        String strFKSQL = "";

        if (FKSQL != null) {
            strFKSQL = FKSQL.stream().map(Object::toString)
                    .collect(Collectors.joining(";\n "));
        }
        if (bprintFK) // print only if this is set 
        {
            strTabCrtStat = strTabCrtStat + "\n" + strFKSQL;
        } else {
            strTabCrtStat = strTabCrtStat + "\n  "
                    + "/* ================= Commenting out FKS ================ "
                    + "\n"
                    + strFKSQL
                    + "\n"
                    + "\n\n =================**END**Duplicate index and primary key ================ "
                    + "*/";
        }

		//lSumBJCLogger.WriteLog(" new method FK::" + strFKSQL.toString());
        String sidxSQL = "";
        int hj = 0;
        tfield tfpk = desttable.getPKField();
        for (iindex idx : desttable.Indexes.getindexes()) {
            if (idx.getName().equalsIgnoreCase(desttable.PK_NAME.toUpperCase())) {
                sidxSQL = sidxSQL
                        + "\n"
                        + "/* =================Duplicate index and primary key ================ "
                        + "\n"
                        + "Table:" + desttable.getName()
                        + "\t PK:" + desttable.PK_NAME
                        + "\tINDEX:" + desttable.PK_NAME + "\n"
                        + "--" + idx.GetDDL(s2Schema) + "\t\n*/"
                        + "\n\n/* =================**END**Duplicate index and primary key ================ "
                        + "*/";
            } else //if (tfpk.getName().equalsIgnoreCase(idx.getIndexField(tfpk.getName()).indxField.getName()))
            {
                List<String> idxFldsNames = idx.getIndexFields(tfpk.getName()) //.stream()
                        // .map(e -> ((idxFld) e).indxField.getName())
                        // .collect(Collectors.toList());
                        ;
                List<String> pkFldsNames = desttable.getPKFields()
                        .stream()
                        .map(e -> ((tfield) e).getName())
                        .collect(Collectors.toList());
                if (idxFldsNames.size() == pkFldsNames.size() && idxFldsNames.containsAll(pkFldsNames)) //System.out.println("   PK: " + tfpk.getName() + " :  "+ idx.getIndexField(tfpk.getName()).getName() );
                //if (the columns are smae and have different index name print that out as duplicate )
                {
                    sidxSQL = sidxSQL + "\n"
                            + "/* =================PK fields usede in another index ================ "
                            + "\n"
                            + "Table:" + desttable.getName()
                            + "\tPKField:" + tfpk.getName()
                            + "\t"
                            + "INDEX:" + idx.getName() + "\n"
                            + "--" + idx.GetDDL(s2Schema) + "\t\n*/"
                            + "\n\n/* =================**END**================ "
                            + "*/";
                } else {
                    sidxSQL = sidxSQL + "\n " + idx.GetDDL(s2Schema);
                }
                pkFldsNames.clear();
                idxFldsNames.clear();
                pkFldsNames = null;
                idxFldsNames = null;

            }

        }

        strTabCrtStat = strTabCrtStat + sidxSQL;

//    lSumBJCLogger.WriteLog(strTabCrtStat + "\n\n"
//            + ""
//            + "---------------------------------------------------------------------------------------------------------------------------------------");
        lSumBJCLogger.WriteOut(strTabCrtStat + "\n\n"
                + ""
                + "---------------------------------------------------------------------------------------------------------------------------------------");

        lSumBJCLogger.setSYSTEM_LOG_OUT(false);

    }

    public String strDropStatements(String stabName) {
        String DropDDL = "";
        itable itab = objToSchema.TableByName(stabName);

        if (itab.fktables != null && !itab.hascreatedDropDDL) {
            itab.hascreatedDropDDL = true;
            DropDDL = DropDDL + " Drop table " + itab.getName() + ";\n";
            for (fkTable fktab : itab.fktables.gettables()) {
                DropDDL = strDropStatements(fktab.PKColumn.CON_TABLE) + ";" + DropDDL;

            }
        }

        return DropDDL;
    }

    public String strDropStatRec(String stabName) {
        String sDropAllTables = "";
        /* when the app reeads the tabs in2 memory it reads and pull the dependant tables recursively
         so when DROP is generated do it in the reverse way. FIRST IN LAST OUT;
    
         */
        for (itable itab : objToSchema.gettables()) {
//        System.out.println(" processing " + itab.getName() + " \t hascreatedDropDDL: " +itab.hascreatedDropDDL);
            if (!itab.hascreatedDropDDL) {
                sDropAllTables = "\n" + strDropStatements(itab.getName()) + sDropAllTables;
            }
            //System.out.println(sDropAllTables);
        }
        return sDropAllTables;
    }

    /*
     *
     */
    public void prntTableswithIssues() {

        // print tables with no PKS
        String strPrintMe = "";
        List<String> TabNoPKs
                = objFrmSchema.gettables().stream()
                .filter(e -> ((itable) e).getPKField().getName().equalsIgnoreCase("UNKNOW")) // veryf cooll George
                .map(e -> ((itable) e).getName())
                .collect(Collectors.toList());

        if (TabNoPKs != null) {
            strPrintMe = TabNoPKs.stream()
                    .map(Object::toString)
                    .collect(Collectors.joining(";\n "));
        }

        printMsgs(strPrintMe, "Table With NO PKS");

        strPrintMe = "";
        List<String> TabNoIdxs
                = objFrmSchema.gettables().stream()
                .filter(e -> ((itable) e).Indexes != null)
                .map(e -> ((itable) e).getName())
                .collect(Collectors.toList());

        strPrintMe = TabNoIdxs.stream()
                .map(Object::toString)
                .collect(Collectors.joining(";\n "));

        printMsgs(strPrintMe, "Table With NO IDXS ");

        if (!TabNoPKs.isEmpty()) {
            TabNoPKs.clear();
        }

        if (!TabNoIdxs.isEmpty()) {
            TabNoIdxs.clear();
        }

        List<itable> tabWFldiss = getiTabWthFldsNoCommOrUnknowDataTYpe();
        strPrintMe = "";

        for (itable tab : tabWFldiss) {
            strPrintMe = strPrintMe + tab.getName() + "\t "
                    + tab.getfields().stream()
                    .map(fld -> fld.GetDDL())
                    .collect(Collectors.joining("\t"));

            strPrintMe = strPrintMe + "\n";
        }
        printMsgs(strPrintMe, "Table With fld of unknow datatype and no Comments");

		// lSumBJCLogger.setSYSTEM_LOG_OUT(false);
        if (!tabWFldiss.isEmpty()) {
            tabWFldiss.clear();
        }

    }

    public void printMsgs(String strPrintme, String strTitle) {
        String StrPrint = "";
        if (strPrintme != "") {
            StrPrint = "\n";
            StrPrint = StrPrint
                    + "/*====================" + strTitle + " =========================*/\n"
                    + strPrintme
                    + "\n "
                    + "/*==================== End " + strTitle + " ==========================*/";
            lSumBJCLogger.WriteOut(StrPrint);
        }
    }

    public List<itable> getiTabWthFldsNoCommOrUnknowDataTYpe() /*
     --Geo--C Get tables where flds have no comments and UNKNOW type when trying to map from source 
     Fixed the duplicate printing 
     */ {
        List<itable> TabNoIdxs = new ArrayList<itable>();
        itable ltab = null;
        tfield sfld = null;
        for (itable itab : objToSchema.gettables()) {
            for (tfield fld : itab.getfields()) {
                if ((fld.getComment().equalsIgnoreCase("")) || (fld.getType() == "OTHER")) {
                    if (ltab == null) {
                        ltab = new itable();
                        ltab.setName(itab.getName());
                        ltab.AddPKField(fld);
                    } else {
                        ltab.AddPKField(fld);
                    }
                }
            }
            if (ltab != null) {
                TabNoIdxs.add(ltab);
                ltab = null;
            }
        }
        return TabNoIdxs;

    }

    protected void finalize() throws Throwable {

        super.finalize();
    }

}

/*
 desttable.Indexes.getindexes().forEach
 ((key) -> 
 {
 if (((iindex) key).getName().equalsIgnoreCase(desttable.PK_NAME.toUpperCase())
 ) 
 {   IDXSQL= "";  
 lSumBJCLogger.WriteLog(" Duplicate index and primary key ================ "
 + "\n"
 + "PK:" + desttable.PK_NAME
 + "\n"
 + "INDEX:" + desttable.PK_NAME + "\n"
 + "" + ((iindex) key).GetDDL(s2Schema) + " "
 );
                    
 } else 
 {
 IDXSQL="";
 lSumBJCLogger.WriteLog("\n " + ((iindex) key).GetDDL(s2Schema)
 );
 }

 }
                
                
 );
 */

 
    /*
    
    desttable.fktables.gettables().forEach
        ((key) -> 
            { String asd="";
                //lSumBJCLogger.WriteLog
(" new method" + ((fkTable) key).GetDDL(s2Schema));
             asd= asd + ((fkTable) key).GetDDL(s2Schema);
            
             
            }
        );
    */
