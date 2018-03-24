/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbutils;

import java.sql.Connection;
import java.sql.SQLException;

import static dbutils.idrive.lSumBJCLogger;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author TGAJ2
 */
public class iDataloc extends tfield {
    public Connection _conn;
    public String surl = "";// JDBC connection URL 

    
    
    
    
    public iDataloc(String url, String class4Name, String db2user, String db2pass) {
        try {
            Class.forName(class4Name);
            
            _conn =java.sql.DriverManager.getConnection(url, db2user, db2pass);
            _conn.setAutoCommit(false);
            lSumBJCLogger.WriteLog(" Created connection  " + url);

        } catch (SQLException e) {
            Exception E = null;
            while (e.getNextException() != null) {
                E = e.getNextException();
                lSumBJCLogger.WriteErrorStack("Error ", E);
            }

            lSumBJCLogger.WriteErrorStack("Error ", e);

        } 
        
        catch (ClassNotFoundException E) {
             lSumBJCLogger.WriteErrorStack(url, E);


        } 
        
        finally 
        {

        }

    }
    
    public void iprintDbtypes(){
        
        ResultSet rsColumns = null;
        String sPrintHeader = "";
        int sPrintHeaderLen= 0;
        
        
        sPrintHeader = sPrintHeader+ String.format("\n|%50s | %10s |"
                        , "Type Name", "Data_type");
        sPrintHeaderLen = sPrintHeader.length();
        sPrintHeader = lSumBJCLogger.sTabPrint(sPrintHeader, sPrintHeaderLen, "-", "|");
        
        try {
            rsColumns = _conn.getMetaData()
                    .getTypeInfo();
            lSumBJCLogger.setSYSTEM_LOG_OUT(true);
            
            while (rsColumns.next()) {
                sPrintHeader= sPrintHeader 
                        + String.format("\n|%50s | %10s |"
                        ,rsColumns.getString("TYPE_NAME")                 
                        ,rsColumns.getString("DATA_TYPE")  );
                
            }
            
        } catch (SQLException ex) {
            Logger.getLogger(iDataloc.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally{
            sPrintHeader = sPrintHeader + lSumBJCLogger.printLine( sPrintHeaderLen, "-", "|");
            lSumBJCLogger.WriteLog(sPrintHeader);
            lSumBJCLogger.setSYSTEM_LOG_OUT(false);
    
    }
         
    }
    
    
    public void iprintDbinfo(String strHeaderInfo) throws SQLException{
        lSumBJCLogger.setSYSTEM_LOG_OUT(true);
        lSumBJCLogger.WriteLog("/*============================================================================");  
        lSumBJCLogger.WriteLog("-- " + strHeaderInfo+ " Db:");
        lSumBJCLogger.WriteLog("============================================================================*/");

        
        lSumBJCLogger.WriteLog("DatabaseProductName: " + _conn.getMetaData().getDatabaseProductName() );  
        lSumBJCLogger.WriteLog("DatabaseProductVersion: " + _conn.getMetaData().getDatabaseProductVersion() );  
        lSumBJCLogger.WriteLog("DatabaseMajorVersion: " + _conn.getMetaData().getDatabaseMajorVersion() );  
        lSumBJCLogger.WriteLog("DatabaseMinorVersion: " +_conn.getMetaData().getDatabaseMinorVersion() );  
         lSumBJCLogger.WriteLog("=====  Driver info =====");  
         lSumBJCLogger.WriteLog("DriverName: " + _conn.getMetaData().getDriverName() );  
         lSumBJCLogger.WriteLog("DriverVersion: " + _conn.getMetaData().getDriverVersion() );  
         lSumBJCLogger.WriteLog("DriverMajorVersion: " + _conn.getMetaData().getDriverMajorVersion() );  
         lSumBJCLogger.WriteLog("DriverMinorVersion: " + _conn.getMetaData().getDriverMinorVersion() );  
         lSumBJCLogger.setSYSTEM_LOG_OUT(false);
    
    }
            
            
            
    

    protected void finalize() throws Throwable {
        tfield l;
        String indx1;
        try {
            if (!(_conn == null)) 
            {
                if (!_conn.isClosed()) {
                    _conn.close();
                }
            }
        } catch (Exception e) {
            lSumBJCLogger.WriteErrorStack("Finalize Exception", e);

        } catch (Error e) {
            lSumBJCLogger.WriteErrorStack("Finalize Error ", e);

        } finally {
            _conn = null;
            super.finalize();
        }
    }

}
