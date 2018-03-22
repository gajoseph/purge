/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbutils;



/**
 *
 * @author tgaj2
 */


import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;


import java.util.LinkedHashMap;
import java.util.Properties;
import bj.BJCLogger;
//import static utilities.HL7Parser.lPropertyReader;
import  bj.PropertyReader;
//import utilities.*;
//import Tobjects.*;
abstract class idrive extends  tfield{
    String      _iErrDesc = "Initiated"       ;
    private     LinkedHashMap _ErrList;
    private     int _ErrlistCnt=0;
    private     tfield _ichild;
    private     int _ichildlvl=0; 
    ResultSet   _selResultSet = null;

    PreparedStatement _insStatement  = null;
        
    public Connection   _connDest;
    public Connection   _connSrc;
    

    public String getiErrDesc() {
        return _iErrDesc;
    }

    public LinkedHashMap getErrList() {
        return _ErrList;
    }

    public int getErrlistCnt() {
        return _ErrlistCnt;
    }

    public tfield getIchild() {
        return _ichild;
    }

    public int getIchildlvl() {
        return _ichildlvl;
    }

    public ResultSet getSelResultSet() {
        return _selResultSet;
    }

    public PreparedStatement getInsStatement() {
        return _insStatement;
    }

    public Connection getConnDest() {
        return _connDest;
    }

    public void setiErrDesc(String _iErrDesc) {
        this._iErrDesc = _iErrDesc;
    }

    public void setErrList(LinkedHashMap _ErrList) {
        this._ErrList = _ErrList;
    }

    public void setErrlistCnt(int _ErrlistCnt) {
        this._ErrlistCnt = _ErrlistCnt;
    }

    public void setIchild(tfield _ichild) {
        this._ichild = _ichild;
    }

    public void setIchildlvl(int _ichildlvl) {
        this._ichildlvl = _ichildlvl;
    }

    public void setSelResultSet(ResultSet _selResultSet) {
        this._selResultSet = _selResultSet;
    }

    public void setInsStatement(PreparedStatement _insStatement) {
        this._insStatement = _insStatement;
    }

    public void setConnDest(Connection _connDest) {
        this._connDest = _connDest;
    }
    
/*
    generateing error list 
    */    
    public void setErrDesc(String _pErrDesc) {
        this._iErrDesc = _pErrDesc;
        _ErrList.put(_ErrlistCnt+"", _pErrDesc);
        _ErrlistCnt++;
    }

    
    public static bj.BJCLogger lSumBJCLogger ;
    public static Properties lPropertyReader;

    
    
    public  idrive(){
        try {
            
        System.out.println(this.getClass().getName() + "  here");    
        _ErrList = new LinkedHashMap();
        
        _ichild = new tfield();//-- creating a default child 
        String Flname = this.getClass().getName() + "_Log_" ;
        
        this.lPropertyReader = PropertyReader.getPropInstance();
        
        //this.lPropertyReader.setProperty("LOG.PATH", "c:/testing/");
        
        //lSumBJCLogger = new BJCLogger(lPropertyReader.getProperty("LOG.PATH") + Flname);
        
        lSumBJCLogger = new BJCLogger(lPropertyReader.getProperty("LOG.PATH") + Flname
                                         , lPropertyReader.getProperty("SQL.DDL.PATH") + this.getClass().getName()  
                                        );
        
        
        //lSumBJCLogger.set_fileFormat(Flname);
        lSumBJCLogger.set_fileType("html");
        lSumBJCLogger.set_SYSTEM_OUT("TRUE");
        lSumBJCLogger.setSYSTEM_LOG_OUT(false);
       
        }
        catch (FileNotFoundException e) 
        {
            _iErrDesc = "FileNotFoundException !! in HL7Parser Err Code" + "\n" + e.getMessage();
            System.out.println("\nError:" + _iErrDesc);
        } 
        catch (IOException e) 
        {
            _iErrDesc = "FileNotFoundException !! in HL7Parser Err Code" + "\n" + e.getMessage();
            System.out.println("\nError:" + _iErrDesc);
        }

    }
    abstract void setDbConnection() throws  Exception;
    
    //2)
    abstract void _MergePlacerOrder(LinkedHashMap HL7Msg);
    
    
    abstract void sqlDbConnection() throws  Exception;
    
    
    
    
    protected abstract void _updateFillerInfo(LinkedHashMap HL7Msg)throws Exception;
    
    
    
    
    
    
     protected synchronized void finalize() throws java.lang.Throwable {
        try {
            
            lSumBJCLogger = null;
            
            
            _ichild=null;
            if (!(_insStatement== null)) 
                   // if (!(_insStatement.isClosed()))
                         _insStatement.close();
            
            if (!(_connDest== null)) 
                if (!_connDest.isClosed())
                _connDest.close();
            
            
            if (_ErrList != null)
                _ErrList.clear();
            
            if (_selResultSet !=null)
                _selResultSet.close();
             
        }
        catch (Exception e){
//        	////System.out.println("Exception " + e.getMessage() + " :" + e.getCause() );
        	throw new RuntimeException("unexpected invocation exception: " +e.getMessage());
        	}
        catch (Error e){
//        	////System.out.println("Exception " + e.getMessage() + " :" + e.getCause() );
        	throw new Error("unexpected Error occured : " +e.getMessage());
        	}
        finally {
            _insStatement = null;
            _connDest = null;
            _connDest = null;
            _ErrList = null;
            _selResultSet = null;
            super.finalize(); 
        }
   
    }
    
    
    
    



 public static void main(String[] args) {
 //       muhl7ParseRad lmuhl7ParseRad =  new muhl7ParseRad("C:\\Users\\gaj3236\\Stuff\\downloads\\IR\\NR\\mercury.na55asyng_out422.hl7", "na55asyng_out");
        //idrive lidrive =  new idrive();//("C:\\Users\\gaj3236\\Stuff\\downloads\\IR\\NR\\mercury.na51syngo_in422.hl7", "na55asyng_in");
      
       

    }//end of main

}