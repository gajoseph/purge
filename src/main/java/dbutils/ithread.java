/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbutils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

/**
 *
 * @author TGAJ2
 */
public class ithread extends Thread {
//    utilities.UnGZIP T = null; 

    private Thread t;
    public Connection _connDest;
    public  enum runtype {
    COPY, JDBCBATCH}
    public String _iErrDesc;
    String iName, sStartTs, sEndTs;
    public PreparedStatement _insStatement = null;
    private LinkedHashMap _ErrList = new LinkedHashMap();
    public byte c[];
   CopyManager cpManager;
   public runtype Rtype ;
    
    public ithread(String strName, String pstrSql, String pstrURL) {

    //public ithread(){
        //super(T);
        super(strName);
        this.Rtype=runtype.JDBCBATCH;
        System.out.println(strName + " starting! ");
        iName = strName;
        sStartTs = new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());
        //this.start();
        try {
            if (activeCount() >= 6)
                    this.wait();
          // this.notify(); 
            Class.forName("org.postgresql.Driver");

            String url = pstrURL;
            _connDest = java.sql.DriverManager.getConnection(url);
            _connDest.setAutoCommit(false);
            _insStatement = _connDest.prepareStatement(pstrSql);
            //UseServerSidePrepare
           cpManager=  ( (PGConnection)_connDest).getCopyAPI();
         
         
        } catch (ClassNotFoundException e) {
            System.out.println("  error." + e.getMessage());

        } catch (SQLException e) {
            Exception E = null;
            while (e.getNextException() != null) {
                E = e.getNextException();

                _iErrDesc = _iErrDesc + "\n" + "SQL Error !! " + "\n" + E.getMessage();

                //System.out.println("\nError:" + _ErrDesc);
            }
            _iErrDesc = "SQL Error !! in tquery.open() Err Code" + e.getErrorCode() + "\n" + e.getMessage();

            E = null;
            throw new UnsupportedOperationException(_iErrDesc); //To change body of generated methods, choose Tools | Templates.

        } catch (InterruptedException ex) {
            Logger.getLogger(ithread.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

  //  private ithread(String thread1, String ____sssss) {
    //      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    //  }
    /*
     public void  start(){
     try{this.start();}
     catch (Throwable T) {
     System.out.println(" error caued in ithread"  );
        
     }
        
     }   
     */
    
    private void copywhat(){
    ByteArrayInputStream input = new ByteArrayInputStream(c);
    
        try {
            cpManager.copyIn("COPY TMS.LTAB_NEW FROM STDIN  WITH DELIMITER '|'", input);
            _connDest.commit();
                        

            
        } catch (SQLException ex) {
            System.out.println("Copywhat " + ex.getMessage());
        } catch (IOException ex) {
            System.out.println("Copywhat " + ex.getMessage());
        }
        
    
    }
    
    private void runJDBC() {
        int successCount,notAavailable,failCount;
        successCount=notAavailable=failCount=0;
        try {
            _insStatement.executeBatch();
            _connDest.commit();
            _insStatement.clearBatch();
            //this.join(); this.notifyAll();
           
        } catch (BatchUpdateException buex) {
            buex.printStackTrace();
            //System.out.println(buex);
            int[] updateCounts = buex.getUpdateCounts();
            for (int i = 0; i < updateCounts.length; i++) {
                if (updateCounts[i] >= 0) {
                    successCount++;

                } else if (updateCounts[i] == _insStatement.SUCCESS_NO_INFO) {
                    notAavailable++;

                } else if (updateCounts[i] == _insStatement.EXECUTE_FAILED) {
                    failCount++;

                }
            }
        }
        catch (SQLException e ){
            //e = new  SQLException();

            Exception E = null;
            while(e.getNextException() != null) {
                E= e.getNextException();


                System.out.println("\nError:" + E.getMessage() +  E.getMessage()   );
                E.printStackTrace();
                }
    //        setErrDesc( "SQL Error !! in tquery.open() Err Code" + e.getErrorCode() + "\n" + e.getMessage());
            System.out.println("\nError:" + E.getMessage() );

      e  = null;


        }

        finally {
            System.out.println("Number of affected rows before Batch Error :: " + successCount);
            System.out.println("Number of affected rows not available:" + notAavailable);
            System.out.println("Failed Count in Batch because of Error:" + failCount);
        }

    }

    public void run() {

        try {
            super.run();
        //T.quicksort( L,H);
            //   T.UNZIPOS_1(ZipFlname,FileSep , UnzipFilePath);

           // getWork();
            synchronized (this) {
                this.getWork();
//                this.T.iThreadpool= T.iThreadpool+1;
               // this.T.unzip(ZipFlname, UnzipFilePath);
//                runJDBC();
                switch (this.Rtype){
                    case JDBCBATCH:
                        runJDBC();
                        break;
                    case COPY:
                      //  copywhat();
                        break;
                }
                    
                //Thread.sleep((int)(Math.random() * 10000));
                this.stop();
                System.out.println("=========================" + getName() + "=========================");
                System.out.println("Thread :" + getName() + " Started : "+ sStartTs 
                        + " END "
                        + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date())
                        );

                this.finalize();

                //this.notify();
                this.RelWork();
            }

        } catch (Throwable T) {
            System.out.println("Error in thread" + this.iName + " " + T.getMessage());

        }

    }

    public synchronized boolean getWork() throws InterruptedException {
        synchronized (this) {
            if (activeCount() >= 6) {
                this.wait(100);
                this.notify();
                return false;

            } else {
                return true;
            }
        }
    }

    public synchronized void RelWork() {

        synchronized (this) {
            //  this.T.iThreadpool= T.iThreadpool-1;
            this.notify();
               // ithread.currentThread().notify();

        }
    }
            //notifyAll();

    protected synchronized void finalize() throws java.lang.Throwable {
       String a= "";
        try {
            System.out.println("Destroying " + this.iName);

            if (!(_insStatement == null)) // if (!(_insStatement.isClosed()))
            {
                a= a + "_insStatement.close()\n";
                _insStatement.close();
            }

            if (!(_connDest == null)) {
                if (!_connDest.isClosed()) {
                a= a + "_connDest.close()\n";
                    _connDest.close();
                }
            }

            if (_ErrList != null) {
                _ErrList.clear();
            }
            System.out.println("s="+a );
            System.out.println("========================= END " + getName() + "=========================");
                
        } catch (Exception e) {
//        	////System.out.println("Exception " + e.getMessage() + " :" + e.getCause() );
            throw new RuntimeException("unexpected invocation exception: " + e.getMessage());
        } catch (Error e) {
//        	////System.out.println("Exception " + e.getMessage() + " :" + e.getCause() );
            throw new Error("unexpected Error occured : " + e.getMessage());
        } finally {
            _insStatement = null;
            _connDest = null;
            _connDest = null;
            _ErrList = null;
            super.finalize();
        }

    }

    public static void main(String args[]) {
       
    }

}
