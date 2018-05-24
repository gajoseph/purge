package dbutils;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PushbackReader;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class delthread extends Thread{

    private Thread t;
    public Connection _connDest;
    public String _iErrDesc;
    String iName, sStartTs, sEndTs;
    private LinkedHashMap _ErrList = new LinkedHashMap();

    public PreparedStatement _insStatement = null;
    public byte c[];
    public ithread.runtype Rtype ;


    public delthread(String strName, String pstrSql, String pstrURL) {

        //public ithread(){
        //super(T);
        super(strName);
        //this.Rtype= ithread.runtype.JDBCBATCH;
        System.out.println("=========================" + getName() + "starting =========================");
        System.out.println( pstrSql + "DFDDFD");
        iName = strName;
        sStartTs = new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());
        //this.start();
        try {
            if (this.activeCount() >= 6)
                this.wait();
            // this.notify();
            Class.forName("org.postgresql.Driver");// this need to be passed as a parameter/ the connection object

            String url = pstrURL;
            _connDest = java.sql.DriverManager.getConnection(url);
            _connDest.setAutoCommit(false);
            _insStatement = _connDest.prepareStatement(pstrSql);
            //UseServerSidePrepare


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



    private void runJDBC() {
        int successCount,notAavailable,failCount;
        successCount=notAavailable=failCount=0;
        try {
            System.out.println("Inside runJDBC");

            _insStatement.execute();//executeBatch();
            _connDest.commit();
            //_insStatement.clearBatch();
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
            if (this.activeCount() >= 6) {
                this.wait(100);
                this.notify();
                return false;

            } else {
                return true;
            }
        }
    }

    public synchronized void RelWork() throws InterruptedException {

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
