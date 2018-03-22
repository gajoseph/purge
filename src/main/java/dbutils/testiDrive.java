/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
straung postgres 
cmd C:\Program Files\PostgreSQL\9.3\bin
pg_ctl start -D c:\data2

STARTING MYSQL
cmd C:\Users\tgaj2\Downloads\mysql-5.7.18-winx64\bin
mysqld --console --init-file=c:\\sql\mysql.txt
## testing
cmd C:\Users\tgaj2\Downloads\mysql-5.7.18-winx64\bin
mysql -u root -p --port=3306
CREATE DATABASE clindb

WORKING 02/28/2018


*/
package dbutils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.postgresql.*;


/**
 *
 * @author tgaj2
 */
public class testiDrive extends dbutils.idrive {

   // @Override
    
    void sqlDbConnection() throws Exception{
    
    
    }
    void setDbConnection() throws Exception {
         try 
        {
             Class.forName("org.postgresql.Driver");
             
             String url = "jdbc:postgresql://stgd520a/geo_coredb?user=tgaj2&password=8UcREt3p&ssl=false";
              
             _connDest = java.sql.DriverManager.getConnection(url);

             
             
         //   _connDest = java.sql.DriverManager.getConnection(" jdbc:postgresql://stgd520a:5432/coredb", "tgaj2", "8UcREt3p");
          //  _insStatement = _connDest.createStatement();
        }
        catch (ClassNotFoundException e) 
        {
            System.out.println("  error." + e.getMessage());
            _iErrDesc = "  error." + e.getMessage();

        } 
        
         catch (SQLException e) 
         {
            Exception E = null;
            while (e.getNextException() != null) 
            {
                E = e.getNextException();

                _iErrDesc = _iErrDesc + "\n" + "SQL Error !! " + "\n" + E.getMessage(); 
                setErrDesc(_iErrDesc);
                //System.out.println("\nError:" + _ErrDesc);
            }
            _iErrDesc = "SQL Error !! in tquery.open() Err Code" + e.getErrorCode() + "\n" + e.getMessage();
            setErrDesc(_iErrDesc);
            E = null;
            throw new UnsupportedOperationException(_iErrDesc); //To change body of generated methods, choose Tools | Templates.
            

        } 
        
        
        
        
    }

    void PrintTabInfo(String sSchema, String sTable, DatabaseMetaData t){
        
        ResultSet  pkrs;
        try {
            pkrs = t.getPrimaryKeys(null,sSchema, sTable);
       
                    while (pkrs.next()){
                        String PK = pkrs.getString("COLUMN_NAME");
                        System.out.println("getPrimaryKeys(): columnName=" + PK);
                    //getFK(Schemas.getString("TABLE_SCHEM"), resultSet.getString("TABLE_NAME"));
                    }// while pk rs 
                    pkrs.close();
                    
                    pkrs  = t.getImportedKeys(null,sSchema, sTable);
                    while (pkrs.next()){

                        String PK = pkrs.getString("FKTABLE_NAME");
                        System.out.println("FK(): Tabname=" + PK + ": Colname " + pkrs.getString("FKCOLUMN_NAME"));

                    }// while pk rs 
        
     } catch (SQLException ex) {
            Logger.getLogger(testiDrive.class.getName()).log(Level.SEVERE, null, ex);
        }
    
    
    }
    
    
    
    public void Write2DB (String strsql, String strSRVname, String strfromTabName, String strToTabName ) throws Throwable{
/*
 --GEO--C-- 3/12 thgis will be staring dump from one db to another Db. 
 * Thinsg to do 
 *  1) ready from source and estination info from XML files. decrypt password 
 * 
 */            
        ResultSet objRS = null;
        PreparedStatement _Statement = null;
        //Statement _Statement = null;
        ResultSetMetaData rsMD;
// -- Rowdata 
        String srowData = "";
        int rowCount = 0;
        Date dtstar = new Date();
        String snewline = "\n";
        Connection conn1;
        Connection connDest;
        PreparedStatement _insStatement = null;
        String strInsSql = "";
        String sCrap = "";
        String straparams = "";
        String sStartTime = "";
        String _sDbg = "";
        String sTime = "";
        byte[] browBites;
        //-- define  2 threads 
        ithread t1;

        ExecutorService executor = null;

        executor = Executors.newFixedThreadPool(3);
        /**/
        byte[] readbytes;

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        /**/
        int ibatch = 1;

        //String url = "jdbc:postgresql://stgd542a/qtgdb?user=tgaj2&password=8UcREt3p&ssl=false&relaxAutoCommit=true";
        String url = "jdbc:postgresql://localhost/clindb?user=asd&password=asd&ssl=false&relaxAutoCommit=true&ApplicationName=copy";

        url = "jdbc:postgresql://stgd542a/qtgdb?user=tgaj2&password=8UcREt3p&ssl=false&relaxAutoCommit=true";

        String db2drvr = "com.ibm.db2.jcc.DB2Driver";
        //String db2url= "jdbc:db2://ug01.unigroupinc.com:5032/DBP"; 
        String db2url = "jdbc:db2://ug04.unigroupinc.com:5026/DBT";
        String db2user = "tgaj2";
        String db2pass = "Alex2017";

                //url = "jdbc:postgresql://stgd542a/qtgdb?user=tgaj2&password=8UcREt3p&ssl=false&relaxAutoCommit=true";
    //url = "jdbc:postgresql://localhost/coredb?user=asd&password=asd&ssl=false&relaxAutoCommit=true&ApplicationName=copy";                 
        sStartTime = "Begin" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());

	//conn = Tobjects.tconnection.getInstance();
        try {
//                    Class.forName("org.postgresql.Driver");
            Class.forName(db2drvr);
            System.out.println("Source conn1");
//                     conn1 = java.sql.DriverManager.getConnection(url);
            conn1 = java.sql.DriverManager.getConnection(db2url, db2user, db2pass);
            conn1.setAutoCommit(false);

            strsql = "Select * from " + strfromTabName;//+ " limit 100";

            strsql = "Select * from " + strfromTabName + " WITH UR ";

            _Statement = conn1.prepareStatement(strsql);
            _Statement.setFetchSize(2000);

            objRS = _Statement.executeQuery();
            /*  Destination 
                         
             */

            Class.forName("org.postgresql.Driver");
            //          url = "jdbc:postgresql://stgd542a/qtgdb?user=tgaj2&password=8UcREt3p&ssl=false&relaxAutoCommit=true?ApplicationName=Thread";

            url = "jdbc:postgresql://localhost/clindb?user=asd&password=asd&ssl=false&relaxAutoCommit=true";

            url = "jdbc:postgresql://stgd520a/geo_coredb?user=tgaj2&password=8UcREt3p&ssl=false&relaxAutoCommit=true";
                //url = "jdbc:postgresql://prdd520a/coredb?user=tgaj2&password=8UcREt3p&ssl=false&relaxAutoCommit=true";

               // url = "jdbc:postgresql://stgd542a/qtgdb?user=tgaj2&password=8UcREt3p&ssl=false&relaxAutoCommit=true";
            //url = "jdbc:postgresql://localhost/clindb?user=asd&password=asd&ssl=false&relaxAutoCommit=true&ApplicationName=copy";
            // Get metadata
            rsMD = objRS.getMetaData();

            strInsSql = "Insert into " + strToTabName + " (";
            straparams = " Values(";
            for (int i = 1; i <= rsMD.getColumnCount(); i++) {
                strInsSql = strInsSql + rsMD.getColumnName(i) + ",";
                straparams = straparams + "?,";
                System.out.println(rsMD.getColumnName(i) + "  " + rsMD.getColumnTypeName(i));
            }

            strInsSql = strInsSql.substring(0, strInsSql.length() - 1);
            strInsSql = strInsSql + ")" + straparams.substring(0, straparams.length() - 1) + ")";
            System.out.println(strInsSql);
               //System.out.println( "Row # " + rowCount + " I : " + i +" " +rsMD.getColumnName(i)+ " "+ rsMD.getColumnTypeName(i) + " " + objRS.getString(i) );

//                _insStatement = connDest.prepareStatement(strInsSql);
            t1 = new ithread(ibatch + "", strInsSql, url);// Destination is run as threads 

                //strInsSql="insert into COUNT_EVENT (PATIENT_ID,MODULE_ID, CARE_EVENT_COUNTER, EVENT_TYPE_SEQ_NUM,INCORRECT_ACTIONS, EVENT_TYPE, COUNT_NAME , INCORRECT_ACTIONS_COMMENTS, INCORRECT_NOTIFIED, INCORRECT_NOTIFIED_COMMENTS, COMMENTS, INCORRECT_XRAY_TAKEN_TF )values (?,?, ?, ?,?, ?, ?, ?, ?, ?, ?,?)";
            while (objRS.next()) {

                rowCount = rowCount + 1;
                sCrap = "";
                sTime = "Start" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());
                for (int i = 1; i <= rsMD.getColumnCount(); i++) {

                         //browBites  = objRS.getBytes(i);
                    //    sCrap= sCrap + ""+ objRS.getString(i);
                    if (rsMD.getColumnTypeName(i).toUpperCase().trim() == "UUID") {
                        System.out.println("ddddd" + rsMD.getColumnTypeName(i));
                    }

                    /* if (objRS.getBytes(i)== null ) 
                     t1._insStatement.setNull(i, rsMD.getColumnType(i));
                     else*/ if (rsMD.getColumnType(i) == java.sql.Types.TINYINT
                            || rsMD.getColumnType(i) == java.sql.Types.BIGINT
                            || rsMD.getColumnType(i) == java.sql.Types.INTEGER
                            || rsMD.getColumnType(i) == java.sql.Types.SMALLINT
                            || rsMD.getColumnType(i) == java.sql.Types.INTEGER) {
                        t1._insStatement.setInt(i, objRS.getInt(i));
                    } else if (rsMD.getColumnType(i) == java.sql.Types.NUMERIC
                            || rsMD.getColumnType(i) == java.sql.Types.DECIMAL
                            || rsMD.getColumnType(i) == java.sql.Types.FLOAT) {
                        t1._insStatement.setLong(i, objRS.getLong(i));
                    } else if (rsMD.getColumnType(i) == java.sql.Types.VARCHAR
                            || rsMD.getColumnType(i) == java.sql.Types.CHAR) {
                        if (objRS.getString(i) != null) {
                            t1._insStatement.setString(i, objRS.getString(i).replaceAll("\\x00", ""));
                        } else {
                            t1._insStatement.setString(i, objRS.getString(i));
                        }
                    } else if (rsMD.getColumnType(i) == java.sql.Types.DATE
                            || rsMD.getColumnType(i) == java.sql.Types.TIME
                            || rsMD.getColumnType(i) == java.sql.Types.TIMESTAMP
                            || rsMD.getColumnType(i) == java.sql.Types.TIMESTAMP_WITH_TIMEZONE
                            || rsMD.getColumnType(i) == java.sql.Types.TIME_WITH_TIMEZONE) {
                        t1._insStatement.setObject(i, objRS.getObject(i));
                    } else if (rsMD.getColumnType(i) == java.sql.Types.BLOB) {
                        t1._insStatement.setBytes(i, objRS.getBytes(i));
                    } else if (rsMD.getColumnTypeName(i).toUpperCase().trim().endsWith("UUID")) {
                        t1._insStatement.setObject(i, objRS.getObject(i));
                    } else {
                        _insStatement.setObject(i, objRS.getObject(i));
                    }
//                            outputStream.write(browBites);

//                            if (i < rsMD.getColumnCount())                             outputStream.write("|".getBytes());
                    _sDbg = _sDbg + '\n' + "Row # " + rowCount + " I : " + i + " " + rsMD.getColumnName(i) + " " + rsMD.getColumnTypeName(i) + " " + objRS.getString(i);

                }
//                         outputStream.write("\n".getBytes());
                sTime = sTime + ": end =" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());
                        // System.out.println(String.join("", Collections.nCopies(30, "-"))+'\n'+ _sDbg
                // + String.join("", Collections.nCopies(30, "-")));

                _sDbg = "";
                sTime = sTime + ": adding to batch =" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());
                t1._insStatement.addBatch();

                        // System.out.println(String.join("", Collections.nCopies(30, "-"))+'\n'+ sTime
                // + String.join("", Collections.nCopies(30, "-")));
                // Check if # rows is 1000 then write . 
                if ((int) rowCount % 2000 == 0) {
                    readbytes = null;
                    browBites = null;
                    readbytes = outputStream.toByteArray();
                    t1.c = readbytes;

                    outputStream = null;

                    outputStream = new ByteArrayOutputStream();
                    sTime = "batch# " + ibatch + " :StartTime ="
                            + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date())
                            + " rowCount= " + ibatch * 2000;

                    //   t1.start();
                    executor.execute(t1);

//                             _insStatement.clearBatch();
//                              lSumBJCLogger.WriteInfo("Committed rows " + rowCount 
                    //                                     + " ["+ new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(new Date()) + "]\t" +  " ["+ new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(dtstar) + "]\t");
                    //_output.write(srowData);
                    sTime = sTime + " : batch commit =" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());
                    System.out.println(
                            sStartTime + "\n"
                            + String.join("", Collections.nCopies(30, "-")) + '\n' + sTime
                            + " Threads["
                            + Thread.activeCount() + " ] "
                            + Thread.currentThread().getClass().getName()
                    );
                    ibatch++;

// START NEW THREAD                             
                    t1 = new ithread(ibatch + "", strInsSql, url + ibatch);
                    t1.Rtype = t1.Rtype.JDBCBATCH;
                }

            }
                     // for 

                 //   readbytes= outputStream.toByteArray( );
            executor.execute(t1);

			   //   if (_output != null)  _output.close();
            objRS.close();
            _Statement.close();
            //         conn1.setAutoCommit(true);
            System.out.println(" Closing ::\t " + conn1.getMetaData().getURL());
            conn1.close();
            t1.finalize();

					//Statement _Statement = null;
            //      if executor.
            sStartTime = sStartTime
                    + String.join("", Collections.nCopies(30, "-"))
                    + ":End" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());

            System.out.println("Done in " + sStartTime);
        } catch (SQLException e) {
				    //e = new  SQLException();

            Exception E = null;
            while (e.getNextException() != null) {
                E = e.getNextException();

                setErrDesc("SQL SQLException !! callinmg getNextException" + "\n" + E.getMessage());// + lSumBJCLogger.GetErrFromStack(E);

                System.out.println("\nError:" + getiErrDesc() + E.getMessage());
                E.printStackTrace();
            }

            setErrDesc("SQL SQLException !! " + e.getErrorCode() + "\n" + e.getMessage());
            System.out.println("\nError:" + getiErrDesc());

            e = null;

        } // end Catch
        catch (Error e) {
            _iErrDesc = "Error !! in tquery.open()  Err Code" + e.getCause() + "\n" + e.getMessage();
            System.out.println("\nError:" + _iErrDesc);
            e.printStackTrace();

				    //lSumBJCLogger.WriteErrorStack("getReportDetails " ,e ) ;
            //lSumBJCLogger.WriteLog("");
            e = null;

        } // end Catch 
        catch (Exception e) {
            _iErrDesc = "Exception !! in tquery.open() Err Code " + e.getCause() + "\n" + e.getMessage();
            System.out.println("\nError:" + _iErrDesc);
//				    lSumBJCLogger.WriteErrorStack("getReportDetails " ,e ) ;
            e = null;

        } finally {

            objRS = null;
            _Statement = null;
            //    _output = null;
            conn1 = null;

            executor.shutdown();
            try {
                executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                _iErrDesc = "executor.awaitTermination!!!!!!!! :: " + e.getCause() + "\n" + e.getMessage();
                System.out.println("\n:" + _iErrDesc);
                e = null;
            }

        }

    }

    
      public static void main(String[] args) {
 //       muhl7ParseRad lmuhl7ParseRad =  new muhl7ParseRad("C:\\Users\\gaj3236\\Stuff\\downloads\\IR\\NR\\mercury.na55asyng_out422.hl7", "na55asyng_out");
        testiDrive ltestiDrive =  new testiDrive();//("C:\\Users\\gaj3236\\Stuff\\downloads\\IR\\NR\\mercury.na51syngo_in422.hl7", "na55asyng_in");
       
        System.out.println(ltestiDrive._iErrDesc);
        try {
         
        
        //ltestiDrive.Write2DB("", "", "qtg.esopportunity_TT", "appdba.esopportunity");
//        ltestiDrive.Write2DB("", "", "tms.LTAB", "TMS.LTAB_NEW");
         ltestiDrive.Write2DB("", "", "UNITEST.locateagent", "appdba.locateagent");
         ltestiDrive.Write2DB("", "", "UNITEST.masteragent", "appdba.masteragent");
          
          //mime_type_lu
//          ltestiDrive.Write2DB("", "", "tms.mime_type_lu", "tms_tmp.mime_type_lu");
          
        //appdba.TUCT2ORDER 
        
//        ltestiDrive.Write2DB("", "", "UNIPROD.locateagent", "appdba.locateagent");
      }
        
        
        

        
        catch (Exception e) 
         {
            System.out.println(e.fillInStackTrace().toString());

        } catch (Throwable ex) {
            Logger.getLogger(testiDrive.class.getName()).log(Level.SEVERE, null, ex);
        } 
        
        
        ltestiDrive =  null;
    }//end of main

    @Override
    void _MergePlacerOrder(LinkedHashMap HL7Msg) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected void _updateFillerInfo(LinkedHashMap HL7Msg) throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
      
      
      
     
    
    
} // testIDrive

/*

 DatabaseMetaData t ; 
          ltestiDrive.setDbConnection();
          t = ltestiDrive._connDest.getMetaData();
         
          ResultSet Schemas = t.getSchemas();
          
          
        while (Schemas.next())  {
                System.out.println("Printing schema \"TABLE\" " + Schemas.getString("TABLE_SCHEM"));


                String SS = String.join("", Collections.nCopies(30, "-"));
                ResultSet resultSet = t.getTables(null, Schemas.getString("TABLE_SCHEM"), null, new String[]{"TABLE"});
                System.out.println("Printing TABLE_TYPE \"TABLE\" ");
                System.out.println(SS);

                while(resultSet.next())
                {
                    //Print
                    System.out.println(resultSet.getString("TABLE_NAME"));
                    ltestiDrive.PrintTabInfo(Schemas.getString("TABLE_SCHEM"),resultSet.getString("TABLE_NAME"), t  );

                }
                   // get PK 

                System.out.println(SS +  Schemas.getString("TABLE_SCHEM").toString() + SS);
        } 
        


*/
