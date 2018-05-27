/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbutils;

import java.sql.DatabaseMetaData;

import static dbutils.idrive.lSumBJCLogger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

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
    public ischema objToSchema; // referecme from the calling class; /* shld this be a collection in future */
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
    
    
    public String GetRecParents(itable itab, String s2Sch, int iRowCnt){
        String ParentSelect = ""; 
        
        
        
        
        return ParentSelect;
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

    void getCreatePrivs(ResultSet srcTabPks, String sTableName, itable src, itable des) {

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
        String sPrintHeader= "";
        int sPrintHeaderLen= 0;
        tfield  tdesfld;

        
        // print the header 
        
        
        
        sPrintHeader = sPrintHeader+ String.format("\n|%20s | %10s | %5s | %20s|%1s|%7s |%10s | %100s|"
                        , "Column Name", "Type", "Data_type","Default", "Size","Null", "Destination Type", "DDL" );
        sPrintHeaderLen = sPrintHeader.length();
        sPrintHeader = lSumBJCLogger.sTabPrint(sPrintHeader, sPrintHeaderLen, "-", "|");
        
        lSumBJCLogger.setSYSTEM_LOG_OUT(true);
        //lSumBJCLogger.WriteLog(sPrintHeader);
        
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
                    tdesfld = des.AddField(srcTabCol.getString("COLUMN_NAME"), srcTabCol.getShort("DATA_TYPE") + "" // tricky took 30 minutes to resolve the enum CRAP
                            , srcTabCol.getString("COLUMN_DEF"), i, srcTabCol.getInt("NULLABLE"), srcTabCol.getInt("COLUMN_SIZE")
                            , srcTabCol.getString("REMARKS")
                            , srcTabCol.getInt("DECIMAL_DIGITS")
                    );
                    sPrintHeader = sPrintHeader + String.format("\n|%20s | %10s | %5s | %20s|%1s|%7s |%10s | %50s|"
                            , srcTabCol.getString("COLUMN_NAME")
                            , srcTabCol.getString("TYPE_NAME")
                            , srcTabCol.getString("DATA_TYPE") 
                            , srcTabCol.getString("COLUMN_DEF")
                            ,  srcTabCol.getInt("NULLABLE")        
                            , srcTabCol.getString("COLUMN_DEF") + " "+ srcTabCol.getInt("DECIMAL_DIGITS")
                            , tdesfld.getType()
                            , tdesfld.GetDDL()
                            );
                    
                    
                    
                }
            }
        } catch (Exception ex) {
            lSumBJCLogger.WriteErrorStack("setToDbCreateTable: " + srcTabCol + ":" +  sSchema, ex);

        } finally {
            //lSumBJCLogger.WriteLog("des.GetDDL()");
            sPrintHeader = sPrintHeader + lSumBJCLogger.printLine( sPrintHeaderLen, "-", "|");
            lSumBJCLogger.WriteLog(String.format(
                    "Src TableName: %s.%s \t %s", sSchema, Src.getName(), objFrmSchema.getName(), des.getName())
                            +sPrintHeader);

        }
        lSumBJCLogger.setSYSTEM_LOG_OUT(false);
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
        String lsSchemaName =sSchemaName;
        
        if (objToSchema.getDbType()== dbtype.db.POSTGRES ) {
            lTabName = lTabName.toLowerCase();
            lsSchemaName = lsSchemaName.toLowerCase();
        }
        
        try {
            tabexists = conn1.getMetaData()
                    .getTables(null, lsSchemaName// from schema anme
                            , lTabName, new String[]{"TABLE", "AL IAS"} // do we need to pull if it is an alias 
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
        des.dptables = new fkTables();// storing the child
        lSumBJCLogger.setSYSTEM_LOG_OUT(true);
        try {
            while (srcChldTabs.next()) {
                i++;
                
                fkTable fk = new fkTable();// to stire parent
                fkTable dp = new fkTable(); // store child table
                
                lSumBJCLogger.WriteLog("   " + srcChldTabs.getString("FKCOLUMN_NAME")
                        + des.FieldByName(srcChldTabs.getString("FKCOLUMN_NAME")).getName()
                );
                //fk.FkColumn.field = desttable.FieldByName(srcChldTabs.getString("FKCOLUMN_NAME"));
                //fk.Update_rule = srcChldTabs.getShort("UPDATE_RULE");
                fk.Update_rule = dbtype.reference_option.values()[srcChldTabs.getInt("UPDATE_RULE")] ;
                fk.Delete_rule = dbtype.reference_option.values()[srcChldTabs.getInt("DELETE_RULE")] ;
                
                dp.Update_rule = dbtype.reference_option.values()[srcChldTabs.getInt("UPDATE_RULE")] ;
                dp.Delete_rule = dbtype.reference_option.values()[srcChldTabs.getInt("DELETE_RULE")] ;
                
                
                
                //fk.Delete_rule = srcChldTabs.getShort("DELETE_RULE");
                fk.deferrability = "" + srcChldTabs.getShort("DEFERRABILITY");
                dp.deferrability = "" + srcChldTabs.getShort("DEFERRABILITY");
                
                fk.AddFKField(des.FieldByName(srcChldTabs.getString("FKCOLUMN_NAME")), sTableName, srcChldTabs.getString("FKTABLE_SCHEM"), srcChldTabs.getString("FK_NAME")
                );
                
                
                dp.AddFKField(des.FieldByName(srcChldTabs.getString("FKCOLUMN_NAME")), sTableName, srcChldTabs.getString("FKTABLE_SCHEM"), srcChldTabs.getString("FK_NAME")
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
                    dp.AddPKField(t, srcChldTabs.getString("PKTABLE_NAME"), srcChldTabs.getString("PKTABLE_SCHEM"), srcChldTabs.getString("PK_NAME")                    );
                    
                
                    if (!Objects.equals(t.getType(), des.FieldByName(fk.FkColumn.field.getName()).getType())) {

                        fk.hasIssues = true;
                        fk.shasIssues = "PK and FK mismatch:: " + t.GetDDL()
                                + " --  FK details "
                                + (des.FieldByName(fk.FkColumn.field.getName()).GetDDL());
                    }

                    des.fktables.AddFkTable(fk);
                    // child relation ship; B has FK from A; so A neds to Know abt B; Need to check if this already being added 
                    // Appachan <--Parent-- Joe; Appachan <-Parent- Jee; So when code handles Joe Link from Apachan to --> Joe is added 
                    
                    
                        
                    List<String> FKSQL = a.dptables.gettables().stream()
                                       .map(e -> e.PKColumn.CON_TABLE + ";" + e.PKColumn.field.getName()+ ";"
                                                   + e.FkColumn.CON_TABLE +";"+  e.FkColumn.field.getName()
                                       )
                                       .collect(Collectors.toList());
                    if (FKSQL.contains(dp.PKColumn.CON_TABLE + ";"+ dp.PKColumn.field.getName()+ ";" + dp.FkColumn.CON_TABLE + ";" + dp.FkColumn.field.getName())
                            ) 
                    { lSumBJCLogger.WriteLog( "++++++++++++++++++++++++++++++++++++++++++ PRSENT+++++++++++++++++++++++++++++++++++++++++++++"
                            +  String.format("\n%s Adding Parent=%s; Parent_Key= %s; Child=%s; Child_key=%s", a.getName(),
                                dp.PKColumn.CON_TABLE, dp.PKColumn.field.getName(),
                                dp.FkColumn.CON_TABLE, dp.FkColumn.field.getName()
                                ) 
                            + "\n++++++++++++++++++++++++++++++++++++++++++ PRSENT+++++++++++++++++++++++++++++++++++++++++++++"
                            ) ;
                    }    
                    else 
                    {      
                        lSumBJCLogger.WriteLog(
                                 "************************************ADDING **************************************" + 
                        String.format("\n%s Adding Parent=%s; Parent_Key= %s; Child=%s; Child_key=%s", a.getName(),
                                dp.PKColumn.CON_TABLE, dp.PKColumn.field.getName(),
                                dp.FkColumn.CON_TABLE, dp.FkColumn.field.getName()
                                ) 
                        );  
                        a.dptables.AddFkTable(dp);
                        System.out.println("************************************ADDED **************************************");
                        
                    }    
                        
					// After this Generate the JOIN SQL

                }
                

            }
        } catch (Exception ex) {
            lSumBJCLogger.WriteErrorStack("", ex);
        } catch (Error ex) {
            lSumBJCLogger.WriteErrorStack("", ex);
        } catch (Throwable ex) {
            //Logger.getLogger(this.getClass().getName().log(Level.SEVERE, null, ex));
            //lSumBJCLogger.WriteErrorStack("", ex);
        } finally {
            lSumBJCLogger.WriteLog("End Child tables \n\n");
            lSumBJCLogger.setSYSTEM_LOG_OUT(false);

        }

    }

    public void getTabDetails(String sFrmSchema, String s2Schema, String sTable) throws Throwable {
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
                .map(e -> e.GetDDL(s2Schema))
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
                        .map(e -> e.getName())
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
                .filter(e -> e.getPKField().getName().equalsIgnoreCase("UNKNOW")) // veryf cooll George
                .map(e -> e.getName())
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
                .filter(e -> e.Indexes != null)
                .map(e -> e.getName())
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
