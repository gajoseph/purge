/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 This Copies the struct of DB to another DB 
 postgres to postgres is good 
 Db2 to postgres underway 

 */
package dbutils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import static dbutils.idrive.lPropertyReader;
import static dbutils.idrive.lSumBJCLogger;
import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author TGAJ2
 */
public class iCopy extends idrive {

    public String SRC_DB_URL;
    //public _selResultSet
    public String DES_DB_URL;
    public String SRC_DB_USER;
    public String SRC_DB_PASSWORD;
    public String DES_DB_USER;
    public String DES_DB_PASSWORD;
    public String SRCdbDriver;
    public String DESdbDriver;
    public Dbtables objDBts;
    protected iDataloc objSrcloc;
    protected iDataloc objDesloc;
    ResultSet objSelrs = null;
    PreparedStatement _selStatement = null;

    ResultSet objIns = null;
    PreparedStatement _insStatement = null;

    //public _selResultSet insert resultset
    public iCopy(String sSrcUrl, String sDesUrl) {

        // Get the URL from Propert file
        SRC_DB_URL = lPropertyReader.getProperty("SRC.DB.URL");
        DES_DB_URL = lPropertyReader.getProperty("DES.DB.URL");

        SRC_DB_USER = lPropertyReader.getProperty("SRC.DB.USER");
        SRC_DB_PASSWORD = lPropertyReader.getProperty("SRC.DB.PASSWORD");

        DES_DB_USER = lPropertyReader.getProperty("DES.DB.USER");
        DES_DB_PASSWORD = lPropertyReader.getProperty("DES.DB.PASSWORD");

        // Get Db driver info property file
        SRCdbDriver = lPropertyReader.getProperty("SRC.dbDriver");
        DESdbDriver = lPropertyReader.getProperty("DES.dbDriver");

        try {
            //Initaite Src Db location object
            Class.forName(SRCdbDriver);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(iCopy.class.getName()).log(Level.SEVERE, null, ex);
        }
        objSrcloc = new iDataloc(SRC_DB_URL, SRCdbDriver, SRC_DB_USER, SRC_DB_PASSWORD);
        _connSrc = objSrcloc._conn;

        try {
            //Initaite Des Db location object
            Class.forName(DESdbDriver);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(iCopy.class.getName()).log(Level.SEVERE, null, ex);
        }
        objDesloc = new iDataloc(DES_DB_URL, DESdbDriver, DES_DB_USER, DES_DB_PASSWORD);
        _connDest = objDesloc._conn;

        // initiate the dbtables object
        objDBts = new Dbtables();

    }

    protected void finalize() throws Throwable {
        try {
            objSrcloc.finalize();
            objDesloc.finalize();
            objDBts.finalize();

        } catch (Exception e) {
            lSumBJCLogger.WriteErrorStack("Finalize Exception", e);

        } catch (Error e) {
            lSumBJCLogger.WriteErrorStack("Finalize Error ", e);

        } finally {
            objIns = null;
            _selStatement = null;

            objSrcloc = null;
            objDesloc = null;

            super.finalize();
        }
    }

    public void getDb2TabBySchema(String sFrmSchema, String sToSchema) {
        ResultSet rstables;

        try {

            //try to get the schema 
            DatabaseMetaData dbmd = objSrcloc._conn.getMetaData();
            // rstables = ((com.ibm.db2.jcc.DB2DatabaseMetaData) dbmd).getSchemas();
            //----------------------------------------------------------------------------
            // junk
            rstables = dbmd.getSchemas();
            while (rstables.next()) {

                lSumBJCLogger.WriteLog("Schenma = " + rstables.getString(1) + " 2 " + rstables.getString(2)
                );
            }

            rstables = dbmd.getCatalogs();
            while (rstables.next()) {

                lSumBJCLogger.WriteLog("Cat = " + rstables.getString(1)
                );
            }

            //----------------------------------------------------------------------------
            // junk
        } catch (SQLException ex) {
            //Logger.getLogger(testiDrive_1.class.getName()).log(Level.SEVERE, null, ex);
            lSumBJCLogger.WriteErrorStack("GetShcmea ", ex);

        } catch (Exception ex) {
            //Logger.getLogger(testiDrive_1.class.getName()).log(Level.SEVERE, null, ex);
            lSumBJCLogger.WriteErrorStack("GetShcmea ", ex);
        } catch (Throwable ex) {
            Logger.getLogger(this.getName()).log(Level.SEVERE, null, ex);
        } finally {

        }

    }

    public void GetDBPostgresmeta(String strFrmSchema, String strtoSchema) {
        try {
            lSumBJCLogger.WriteLog("Source conn1");
            getTabBySchema(strFrmSchema, strtoSchema);

        } catch (Error e) {
            lSumBJCLogger.WriteErrorStack("Error in ", e);
        }
    }

    public void getTabBySchema(String sFrmSchema, String sToSchema) {
        String sTabName = "";
        String sSchemaName = "";
        lSumBJCLogger.setSYSTEM_LOG_OUT(true);

        try {
            // Get the table list in a schema
            objSelrs = objSrcloc._conn
                    .getMetaData()
                    .getTables(null, sFrmSchema, null, new String[]{"TABLE"});

            objDBts.objFrmSchema = new ischema();
            objDBts.objFrmSchema.setName(sFrmSchema);
            objDBts.objFrmSchema.setDbType(dbtype.db.valueOf(lPropertyReader.getProperty("SRC.DB.TYPE") ) );
                        
            objDBts.objToSchema = new ischema();
            objDBts.objToSchema.setName(sToSchema);
            objDBts.objToSchema.setDbType(dbtype.db.valueOf(lPropertyReader.getProperty("DES.DB.TYPE") ) );
            
            
            objDBts.conn1 = objSrcloc._conn;

            objSrcloc.iprintDbinfo("Source");

            objSrcloc.iprintDbinfo("Destination");

            objDBts.bprintFK = true;

            while (objSelrs.next()) {
                sTabName = objSelrs.getString(3);
                sSchemaName = objSelrs.getString(2);
                objDBts.getTabDetails(sFrmSchema, sToSchema, sTabName);
                lSumBJCLogger.WriteLog("table = " + sTabName + " 2 " + sSchemaName);
                if (!objSrcloc._conn.getClass().getName().contains("com.ibm.db2")) {
                    lSumBJCLogger.WriteLog(objSrcloc._conn.getClass().getName()
                            + objSelrs.getSQLXML("TABLE_TYPE"));
                }

            }

            objDBts.prntTableswithIssues();
            String d = objDBts.strDropStatRec("");
            lSumBJCLogger.WriteLog(" DROP TABS " + d);

            lSumBJCLogger.setSYSTEM_LOG_OUT(false);

        } catch (SQLException ex) {
            lSumBJCLogger.WriteErrorStack("GetShcmea ", ex);

        } catch (Exception ex) {
            lSumBJCLogger.WriteErrorStack("GetShcmea ", ex);
        } catch (Throwable ex) {
            //Logger.getLogger(testiDrive_1.class.getName()).log(Level.SEVERE, null, ex);
            lSumBJCLogger.WriteError(ex.getStackTrace().toString());
        } finally {
            objSelrs = null;
            lSumBJCLogger.WriteLog("getTabBySchema ::Done ");
        }

    }

    @Override
    void setDbConnection() {

        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    void _MergePlacerOrder(LinkedHashMap HL7Msg) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    void sqlDbConnection() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected void _updateFillerInfo(LinkedHashMap HL7Msg) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public void tabFldTovalues(String sStartTime, int DES_DB_BATCH_SIZE) throws SQLException {
        int rowCount = 0;
        ResultSetMetaData rsMD;
        String sTime = "";
        int ibatch = 1;
		//if (lPropertyReader.getProperty("DES.DB.BATCH.SIZE") !=null)
        //    DES_DB_BATCH_SIZE = Integer.parseInt(lPropertyReader.getProperty("DES.DB.BATCH.SIZE") );

        rsMD = objSelrs.getMetaData();
        while (objSelrs.next()) {
            rowCount++;
            sTime = "Start" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());

            for (int i = 1; i <= rsMD.getColumnCount(); i++) {
                if (rsMD.getColumnType(i) == java.sql.Types.TINYINT
                        || rsMD.getColumnType(i) == java.sql.Types.BIGINT
                        || rsMD.getColumnType(i) == java.sql.Types.INTEGER
                        || rsMD.getColumnType(i) == java.sql.Types.SMALLINT) {
                    _insStatement.setInt(i, objSelrs.getInt(i));
                } else if (rsMD.getColumnType(i) == java.sql.Types.NUMERIC
                        || rsMD.getColumnType(i) == java.sql.Types.DECIMAL
                        || rsMD.getColumnType(i) == java.sql.Types.FLOAT) {
                    _insStatement.setLong(i, objSelrs.getLong(i));
                } else if (rsMD.getColumnType(i) == java.sql.Types.VARCHAR
                        || rsMD.getColumnType(i) == java.sql.Types.CHAR) {
                    if (objSelrs.getString(i) != null) {
                        _insStatement.setString(i, objSelrs.getString(i).replaceAll("\\x00", ""));
                    } else {
                        _insStatement.setString(i, objSelrs.getString(i));
                    }

                    _insStatement.setString(i, objSelrs.getString(i));
                } else if (rsMD.getColumnType(i) == java.sql.Types.DATE
                        || rsMD.getColumnType(i) == java.sql.Types.TIME
                        || rsMD.getColumnType(i) == java.sql.Types.TIMESTAMP
                        || rsMD.getColumnType(i) == java.sql.Types.TIMESTAMP_WITH_TIMEZONE
                        || rsMD.getColumnType(i) == java.sql.Types.TIME_WITH_TIMEZONE) {
                    _insStatement.setObject(i, objSelrs.getObject(i));
                } else if (rsMD.getColumnType(i) == java.sql.Types.BLOB
                        || rsMD.getColumnType(i) == java.sql.Types.LONGVARBINARY) {
                    _insStatement.setBytes(i, objSelrs.getBytes(i));
                } else if (rsMD.getColumnTypeName(i).toUpperCase().trim().endsWith("UUID")) {
                    _insStatement.setObject(i, objSelrs.getObject(i));
                } else {
                    _insStatement.setObject(i, objSelrs.getObject(i));
                }
            }
            sTime = sTime + ": adding to batch =" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());
            // add to batch
            _insStatement.addBatch();
            if (rowCount % DES_DB_BATCH_SIZE == 0) {
                _insStatement.executeBatch();
                objDesloc._conn.commit();
                _insStatement.clearBatch();
                sTime = sTime + " : batch commit =" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());

                lSumBJCLogger.WriteLog(sStartTime + "\n"
                        + String.join("", Collections.nCopies(30, "-")) + '\n' + sTime
                );
                ibatch++;

            }

        }// while
        _insStatement.executeBatch();
        objDesloc._conn.commit();
        _insStatement.clearBatch();
        objSelrs.close();
        _insStatement.close();

        sStartTime = sStartTime
                + String.join("", Collections.nCopies(30, "-"))
                + ":End" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());

        lSumBJCLogger.WriteLog("Done  " + getName() + sStartTime);

    }

	//--------------------------------------------------------------------------------------------------------
    // ltestiDrive.Write2DB("", "", "tms.mime_type_lu", "tms_tmp.mime_type_lu", "tms", "tms_tmp");String strfromTabName, String strToTabName, String strFrmSchema, String strtoSchema
    public void Write2DB(int limitSize) {
        String strsql = "";
        String strFrmSch = objDBts.objFrmSchema.getName();
        String strInsSql = "";
        String sStartTime = "";
        String WhereClause = "";

        int SRC_DB_FETCH_SIZE = 2000;
        int DES_DB_BATCH_SIZE = 2000;

        try {
            if (lPropertyReader.getProperty("SRC.DB.FETCH.SIZE") != null) {
                SRC_DB_FETCH_SIZE = Integer.parseInt(lPropertyReader.getProperty("SRC.DB.FETCH.SIZE"));
            }

            if (lPropertyReader.getProperty("DES.DB.BATCH.SIZE") != null) {
                DES_DB_BATCH_SIZE = Integer.parseInt(lPropertyReader.getProperty("DES.DB.BATCH.SIZE"));
            }

            for (itable itab : objDBts.objToSchema.gettables()) {
                // verify if  table is not created in destination
                //if (objDBts.isDbtable(objDBts.objFrmSchema.getName().toLowerCase(), itab.getName().toLowerCase())) {
                if (objDBts.isDbtable(objDBts.objFrmSchema.getName(), itab.getName())) { // db2 doens't like toLower
                    if (itab.hasBCLOB) // has clob ior large fields then
                    {
                        SRC_DB_FETCH_SIZE = Integer.parseInt(lPropertyReader.getProperty("SRC.DB.FETCH.SIZE.LARGE.OBJ")); //SRC.DB.FETCH.SIZE.LARGE.OBJ=20
                        DES_DB_BATCH_SIZE = SRC_DB_FETCH_SIZE;

                    }
                    // if it has clob/ blob then lets chnage the fetch size
                    sStartTime = "Begin" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());
                    strsql = "Select " + itab.getName() + ".* from " + strFrmSch + "." + itab.getName() + " " + itab.getName();//+ " limit 10";
                    // recursively get the whereclause

                    if (limitSize > 0) {
                        WhereClause = getRecuriveFKs(itab, objDBts.objFrmSchema.getName(), limitSize);// get this from from Schema Clause
                        //WhereClause = WhereClause + "  LIMIT " + limitSize;
                        if (objDBts.objFrmSchema.getDbType().name().equals("POSTGRES")) 
                                WhereClause = WhereClause + "  LIMIT " + limitSize;
                        else if (objDBts.objFrmSchema.getDbType().name().equals("DB2")) 
                                WhereClause = WhereClause + "  fetch first  " + limitSize + " rows only ";
                        else WhereClause = WhereClause + "  LIMIT " + limitSize;
                        
                    }

                    System.out.println("table = " + itab.getName() + "\n " + strsql + " WhereClauseaa = " + WhereClause);

                    _selStatement = objSrcloc._conn.prepareStatement(strsql + WhereClause);
                    objSelrs = _selStatement.executeQuery();
                    _selStatement.setFetchSize(SRC_DB_FETCH_SIZE);// chnage this if PK index cardinality

                    strInsSql = itab.getInsStatCommaSep(objDBts.objToSchema.getName());
                    _insStatement = objDesloc._conn.prepareStatement(strInsSql);

                    tabFldTovalues(sStartTime, DES_DB_BATCH_SIZE); // call this proc to map the values ot derstinationj tagle and run in bathes and commit 
                    _insStatement.close();// DO I NEED THIS

                }// dont process the table if the table is no created in destination
                //else itab.= false
            }// end for

        } catch (Error e) {
            lSumBJCLogger.WriteErrorStack(strsql, e);
        } catch (SQLException e) {

            Exception E = null;
            while (e.getNextException() != null) {
                E = e.getNextException();

                setErrDesc("SQL Error !! in tquery.open() Err Code" + "\n" + E.getMessage());// + lSumBJCLogger.GetErrFromStack(E);

                System.out.println("\nError:" + getiErrDesc());

                lSumBJCLogger.WriteErrorStack(strsql, E);
            }

            setErrDesc("SQL Error !! in tquery.open() Err Code" + e.getErrorCode() + "\n" + e.getMessage());
            System.out.println("\nError:" + getiErrDesc());

            e = null;

            lSumBJCLogger.WriteErrorStack(strsql, e);

        } finally {
            _selStatement = null;
            _insStatement = null;
        }

    }

    
    
// get JOIN CLuae when there is limit pull 
// filter from the destination Db and get only data for those rows 
// Select * from tab1 a left join    
public  String getRecuriveFKs(itable itab, String s2Sch, int iRowCnt )
    {
        String strJoin= "";
        String CON_TABLE = ""; // check if there is referecne to the same table of 2 columns 
        String strchldJOinCond= "";
        String sfltrCond = "";
        StringBuilder sWhrCond = new StringBuilder("");
        String inclause = "";
        
        if (itab.fktables != null)
        {
            for (fkTable fktab : itab.fktables.gettables() )
            {   System.out.println( itab.getName()  + " << Starting " +  objDBts.objToSchema.gettable(fktab.PKColumn.CON_TABLE).getName()  + "---------" ); 
                inclause = "";    
               if (fktab.hasDups)  {        
                    strJoin= strJoin + " LEFT JOIN " 
                            + objDBts.objFrmSchema.getName() + "." + 
                           // joinCon(objToSchema.gettable(fktab.PKColumn.CON_TABLE) , s2Sch)  
                            fktab.PKColumn.CON_TABLE // only the table name not its parent 
                            + "  /*Condition */  " 
                            + fktab.PKColumn.CON_TABLE  +"_" + fktab.FkColumn.field.getName() 
                            + " ON "  + fktab.FkColumn.CON_TABLE+ "."    
                            + fktab.FkColumn.field.getName()  + " =  " 
                            + fktab.PKColumn.CON_TABLE +"_"+ fktab.FkColumn.field.getName()+ "." 
                            + fktab.PKColumn.field.getName();
                    
                    inclause = getleftjoinSwhereCond(fktab.PKColumn.CON_TABLE
                                        , fktab.PKColumn.field.getName()
                                        , iRowCnt);
                    
                    if (!inclause.equalsIgnoreCase(""))
                        sWhrCond.append(" and ("   )
                            .append(
                                fktab.PKColumn.CON_TABLE +"_"+ fktab.FkColumn.field.getName()+ "." 
                                + fktab.PKColumn.field.getName()
                                )
                            .append(" in(")
                            
                            .append(
                                    getleftjoinSwhereCond(fktab.PKColumn.CON_TABLE
                                        , fktab.PKColumn.field.getName()
                                        , iRowCnt)
                                    )      
                            .append(") or ")
                            .append(fktab.FkColumn.CON_TABLE)
                            .append( "."    )
                            .append( fktab.FkColumn.field.getName())
                            .append(" is null )")
                            ;
                    else 
                        sWhrCond.append(" and ")
                        .append (fktab.FkColumn.CON_TABLE).append(".").append( fktab.FkColumn.field.getName()).append( " is null ");
               }            
                else 
               {   
                    strJoin= strJoin 
                            + " LEFT JOIN " 
                            + objDBts.objFrmSchema.getName() + "." + 
                            
                           // joinCon(objToSchema.gettable(fktab.PKColumn.CON_TABLE), s2Sch )  
                            fktab.PKColumn.CON_TABLE
                            
                            + " /*condition*/ " + fktab.PKColumn.CON_TABLE
                    + " ON "  + fktab.FkColumn.CON_TABLE+ "."    
                        + fktab.FkColumn.field.getName()  + " =  " + fktab.PKColumn.CON_TABLE + "." + fktab.PKColumn.field.getName();
                    inclause = getleftjoinSwhereCond(fktab.PKColumn.CON_TABLE
                                        , fktab.PKColumn.field.getName()
                                        , iRowCnt);
                    if (!inclause.equalsIgnoreCase(""))
                        sWhrCond.append(" and ("   )
                                .append(
                                    fktab.PKColumn.CON_TABLE + "." + fktab.PKColumn.field.getName()
                                    )
                                .append(" in(").append(inclause)      
                                .append(") or ")
                                .append(fktab.FkColumn.CON_TABLE)
                                .append( "."    )
                                .append( fktab.FkColumn.field.getName())
                                .append(" is null )")
                                ;
                    else // if the parent table is empty we need to pull were condition w/ is null
                        sWhrCond.append(" and ")
                                .append(fktab.FkColumn.CON_TABLE).append(".").append( fktab.FkColumn.field.getName()).append( " is null ");
                
               }    

// recursilvel call the function 
                System.out.println("Recurisvelu calling " +  objDBts.objToSchema.gettable(fktab.PKColumn.CON_TABLE).getName()  + "-----"  );
//           
                strJoin = strJoin + " " 
                        //+ getRecuriveFKs (   this.gettable(fktab.PKColumn.CON_TABLE) ) 
                        + "";
  
            }    // end for         
            // join the select and where clauses 
                strJoin = strJoin  + " where 1=1 "+ sWhrCond.toString();
        }
        else
        {
            //itab.sJoincondition =s2Sch + "." + itab.getName();
          //  System.out.println("Recurisvelu calling but missede *****" +  itab.getName() );
        
        }
            
            
            
        if (strJoin.trim() !="")    
            itab.sJoincondition = s2Sch + "." + itab.getName() + " "  + "  "+strJoin + "   "  + "  /* "
                    + itab.getName()+ " */";   
        else 
            itab.sJoincondition = s2Sch + "." + itab.getName() ;
    return strJoin;
    }    
    
    
        

   //get limit # of FKs from Fk table 
/*select  a.transportation_mode_type_code ,  b.type_code, *   From tms.manifest a
 left join tms.transportation_mode_type_lu  b on a.transportation_mode_type_code = b.type_code 
where (transportation_mode_type_code in( 'EXPRESS') or  a.transportation_mode_type_code is null)
        
        */        
    public String getleftjoinSwhereCond(String sFrgnTablName, String sFrgnTabCol , int iRowCnt) {
        StringBuilder sSelFrgnTab = new StringBuilder(" "); 
        String sWhrCond = "";
        List<String> FKSQL = new ArrayList<String>();
        
        ResultSet getFrgnTabColValue = null;
        PreparedStatement _selStatement = null;
        
        
        try {
            sSelFrgnTab.append("Select ").append(sFrgnTablName).append(".").append(sFrgnTabCol).append(" from ");
            sSelFrgnTab.append(objDBts.objToSchema.getName()).append(".").append(sFrgnTablName);
    // Check the Db type 
            if (dbtype.db.POSTGRES.name().equals("POSTGRES")) 
                sSelFrgnTab.append(" Limit " ).append(iRowCnt);
            else if (dbtype.db.DB2.name().equals("DB2")) 
                sSelFrgnTab.append(" fetch first  " ).append(iRowCnt).append(" rows only ");
            else sSelFrgnTab.append(" Limit " ).append(iRowCnt);
    //      
            _selStatement =  objDesloc._conn.prepareStatement(sSelFrgnTab.toString());
            
            getFrgnTabColValue = _selStatement.executeQuery();
             
            while (getFrgnTabColValue.next())
                FKSQL.add(getFrgnTabColValue.getString(1));
            
            sWhrCond = FKSQL.stream().map(Object::toString)
                            .collect(Collectors.joining("','"));
            if (! sWhrCond.equals("")){
                sWhrCond = "'" +sWhrCond + "'";
            
            }
            
                
                    
        }
        catch ( Error e){
               lSumBJCLogger.WriteErrorStack("", e);
        }
        catch (SQLException ex) {
			//Logger.getLogger(Dbtables.class.getName()).log(Level.SEVERE, null, ex);
               lSumBJCLogger.WriteErrorStack("", ex);
		} 
        finally {
            return sWhrCond;
        
        }
        
        
        
    }        
        
    
    
    public static void main(String[] args) {
        iCopy licopy = new iCopy("", "");
//        licopy.getTabBySchema("tms", "tms_tmp");
        //   licopy.getTabBySchema("qtg", "qtg_tmp");
        
        licopy.getTabBySchema("PRHQGCLM", "uni_tmp");
        //licopy.getTabBySchema("test", "test_tmp");
        // mysql 
       // licopy.getTabBySchema("test", "test_tmp"); // postgres to mysql 
//        licopy.getTabBySchema("agnt", "clindb"); // postgres to mysql 
        

//       licopy.Write2DB(25);
        licopy = null;

    }

}
