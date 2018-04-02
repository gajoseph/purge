/*
 * BJCLogger.java
 *
 * Created on March 21, 2006, 2:36 PM
 *
 * To change this template, choose Tools | Options and locate the template under
 * the Source Creation and Management node. Right-click the template and choose
 * Open. You can then make changes to the template in the Source Editor.
 */

package bj;

/**
 *
 * @author Gajoseph
 */
import java.io.*;
import java.util.*;
import java.text.*;
import java.lang.StackTraceElement;
import java.lang.String;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BJCLogger {
    /*
        Private Variable Declaration
     */
    private String _DATE_FORMAT = "yyyy-MM-dd HH:mm:ss:SSS";
    private Boolean _bputDate ;//= true;
    private File  _objFile;
    private String _sFilename  ;
    private String _sOutputFilename;
    private File  _objOutPutFile;
    private boolean hasOutPutFile = false ;
    public String sCommentBlock = "--";
    public int sTabsize= 120;
    public String sTabLine= "-";

    public String printLine(int iTabsize,String sTabLine , String Colsep   ){

        return "\n"+ Colsep + new String(new char[iTabsize-3]).replace("\0", sTabLine)+ Colsep;
    }


    public String  sTabPrint(String sContents, int iTabsize, String sTabLine, String Colsep  )
    {
        return  printLine(iTabsize,sTabLine, Colsep )+ sContents + printLine(iTabsize,sTabLine, Colsep );

    }

    private Date fileCreateDatetime = new Date();

    public boolean isSYSTEM_LOG_OUT() {
        return _SYSTEM_LOG_OUT;
    }

    public void setSYSTEM_LOG_OUT(boolean _SYSTEM_LOG_OUT) {
        this._SYSTEM_LOG_OUT = _SYSTEM_LOG_OUT;
    }


    public void setOutputFilename(String _sOutputFilename) {
        this._sOutputFilename = _sOutputFilename;
    }


    private String _SYSTEM_OUT = "TRUE";
    private boolean _SYSTEM_LOG_OUT =   true;
    private String _fileFormat = "_yyyy_MM_dd__HH_mm_ss";
    private String _fileType = "log";

    private String _outfileType = "sql";

    public void setOutfileType(String _outfileType) {
        this._outfileType = _outfileType;
    }



    public String getFileName (){return _sFilename;}
    public Boolean getKeepdate(){return _bputDate;}
    public void setKeepdate(Boolean bKeepdate){_bputDate = bKeepdate;}


    public String getFilename(){

        return _sFilename  + "_Log_" + new java.text.SimpleDateFormat(_fileFormat).format(new java.util.Date()) + "." + _fileType;

    }




    public String getOutFilename(){

        return _sOutputFilename  + "_OutPut_" + new java.text.SimpleDateFormat(_fileFormat).format(new java.util.Date()) + "." + _outfileType;

    }




    public BJCLogger(String sFlName, String sOutputfile) throws FileNotFoundException, IOException {
        if (sFlName.equals(""))      throw new IllegalArgumentException("File Name cannotbe null.");
        _sFilename = sFlName;
        _sOutputFilename = sOutputfile;

        fileCreateDatetime = new Date();

        _objFile = new File(getFilename());
        _objOutPutFile = new File(getOutFilename());

        if (_objFile.isDirectory()) {
            _objFile = null;
            throw new IllegalArgumentException("Should not be a directory: " + sFlName);
        }

        if (_objOutPutFile.isDirectory()) {
            _objOutPutFile = null;
            throw new IllegalArgumentException("Should not be a directory: " + sFlName);
        }

        hasOutPutFile= true; /// means there is avlid output file

        System.out.println("BJCLOGGER PATH = " + _objFile.getPath());
        System.out.println("Out[putfile PATH: " + _objOutPutFile.getPath());


        Writer output = null;
        try {

            output = new BufferedWriter( new FileWriter(_objFile, true)  );

            output.write( sCommentBlock+ "------------------------------------------------------------------------------------" );
            output.write( System.getProperty("line.separator") );

        }
        finally {
            //flush and close both "output" and its underlying FileWriter
            if (output != null)  output.close();

        }

        try {

            output = new BufferedWriter( new FileWriter(_objOutPutFile, true)  );

            output.write( sCommentBlock+ "------------------------------------------------------------------------------------" );
            output.write( System.getProperty("line.separator") );

        }
        finally {
            //flush and close both "output" and its underlying FileWriter
            if (output != null)  output.close();

        }

        java.util.Properties p = System.getProperties();
        java.util.Enumeration keys = p.keys();
        while( keys.hasMoreElements() ) {
            String propName = (String)keys.nextElement();
            String propValue = (String)p.get(propName);

            //    System.out.println( propName  + "= " + propValue  );
        }


    }


    public BJCLogger(String sFlName, String sOutputfile, String spCommentBlock ) throws FileNotFoundException, IOException {
        if (sFlName.equals(""))      throw new IllegalArgumentException("File Name cannotbe null.");
        _sFilename = sFlName;
        _sOutputFilename = sOutputfile;

        fileCreateDatetime = new Date();

        _objFile = new File(getFilename());
        _objOutPutFile = new File(sOutputfile);

        if (!spCommentBlock.equalsIgnoreCase(""))// if empty then use default
            sCommentBlock = spCommentBlock;

        if (_objFile.isDirectory()) {
            _objFile = null;
            throw new IllegalArgumentException("Should not be a directory: " + sFlName);
        }

        if (_objOutPutFile.isDirectory()) {
            _objOutPutFile = null;
            throw new IllegalArgumentException("Should not be a directory: " + sFlName);
        }

        hasOutPutFile= true; /// means there is avlid output file

        System.out.println("BJCLOGGER PATH = " + _objFile.getPath());
        System.out.println("Out[putfile PATH: " + _objOutPutFile.getPath());


        Writer output = null;
        try {

            output = new BufferedWriter( new FileWriter(_objFile, true)  );

            output.write( sCommentBlock+ "------------------------------------------------------------------------------------" );
            output.write( System.getProperty("line.separator") );

        }
        finally {
            //flush and close both "output" and its underlying FileWriter
            if (output != null)  output.close();

        }

        try {

            output = new BufferedWriter( new FileWriter(_objOutPutFile, true)  );

            output.write( sCommentBlock+ "------------------------------------------------------------------------------------" );
            output.write( System.getProperty("line.separator") );

        }
        finally {
            //flush and close both "output" and its underlying FileWriter
            if (output != null)  output.close();

        }

        java.util.Properties p = System.getProperties();
        java.util.Enumeration keys = p.keys();
        while( keys.hasMoreElements() ) {
            String propName = (String)keys.nextElement();
            String propValue = (String)p.get(propName);

            //    System.out.println( propName  + "= " + propValue  );
        }


    }





    public BJCLogger(String sFlName) throws FileNotFoundException, IOException {
        if (sFlName == "")      throw new IllegalArgumentException("File Name cannotbe null.");
        _sFilename = sFlName;

        fileCreateDatetime = new Date();

        _objFile = new File(getFilename());


        if (_objFile.isDirectory()) {
            _objFile = null;
            throw new IllegalArgumentException("Should not be a directory: " + sFlName);
        }
        System.out.println("BJCLOGGER PATH = " + _objFile.getPath());
        Writer output = null;
        try {

            output = new BufferedWriter( new FileWriter(_objFile, true)  );

            output.write( sCommentBlock+ "------------------------------------------------------------------------------------" );
            output.write( System.getProperty("line.separator") );

        }
        finally {
            //flush and close both "output" and its underlying FileWriter
            if (output != null)  output.close();

        }

        java.util.Properties p = System.getProperties();
        java.util.Enumeration keys = p.keys();
        while( keys.hasMoreElements() ) {
            String propName = (String)keys.nextElement();
            String propValue = (String)p.get(propName);

            //    System.out.println( propName  + "= " + propValue  );
        }


    }

    private  void   setOutContents(String _sFilename, String aContents )
            throws FileNotFoundException, IOException {

        if (_sFilename == "")   throw new IllegalArgumentException("File Name cannotbe null.");



        //declared here only to make visible to finally clause; generic reference
        Writer output = null;

        // Switching log file logic goes here
        SwitchLogfile();

        try {

            output = new BufferedWriter( new FileWriter(_objOutPutFile , true)  );

            aContents =sCommentBlock + printDate() +_PrintLineno()
                    + System.getProperty("line.separator")
                    + aContents ;

            output.write( System.getProperty("line.separator") );

            output.write( aContents );
            /* Put a parameter and if this parameter is set only then print to the console. */

        }
        finally {
            //flush and close both "output" and its underlying FileWriter
            if (output != null)  output.close();

        }
    }



    //-------------------------------------------------------------------------------------------------
    private  void   setContents(String _sFilename, String aContents )
            throws FileNotFoundException, IOException {

        if (_sFilename == "")   throw new IllegalArgumentException("File Name cannotbe null.");



        //declared here only to make visible to finally clause; generic reference
        Writer output = null;

        // Switching log file logic goes here
        SwitchLogfile();

        try {

            output = new BufferedWriter( new FileWriter(_objFile, true)  );
            aContents = printDate() +_PrintLineno() + aContents ;



            if (_fileType.indexOf("html") >=0 )
                output.write( "<br>" + System.getProperty("line.separator"));
            else
                output.write( System.getProperty("line.separator") );

            output.write( aContents.replace("\n", "<br>") );
            /* Put a parameter and if this parameter is set only then print to the console. */

            if (_SYSTEM_OUT.equals("TRUE"))
                System.out.println(aContents );


        }
        catch (Error e  )
        {
            if (_fileType.indexOf("html") >=0 )
                output.write( "<br>" + System.getProperty("line.separator") + e.getStackTrace().toString());
            else
                output.write( System.getProperty("line.separator")  +  e.getStackTrace().toString());
        }
        finally {
            //flush and close both "output" and its underlying FileWriter
            if (output != null)  output.close();

        }
    }

    public void WriteLog_Info(String aContents) {
        String old_SYSTEM_OUT="";
        try
        {
            if  (_SYSTEM_LOG_OUT==true)
                setContents(_sFilename , "[LOG]\t"+aContents);
            else
            {
                old_SYSTEM_OUT = this._SYSTEM_OUT;
                this._SYSTEM_OUT="FALSE";// temp flase
                setContents(_sFilename , "[LOG]\t"+aContents);
                this._SYSTEM_OUT    = old_SYSTEM_OUT;



            }
        }
        catch (IOException isi){ //   throws new  IOException(isi);
        }

    }



    public void WriteOutPut(String aContents) {
        try {
            setOutContents(_sOutputFilename, aContents);
        } catch (IOException ex) {
            Logger.getLogger(BJCLogger.class.getName()).log(Level.SEVERE, null, ex);
        }

    }




    public void WriteLog( String aContents){

        WriteLog_Info( aContents);
    }

    public void WriteError(String aContents){
        try
        {
            setContents(_sFilename , "[ERROR]\t"+aContents + "\t" );
        }
        catch (IOException isi){ //   throws new  IOException(isi);
        }
    }

    public void WriteOut(String aContents)
    {
        WriteLog_Info( aContents);
        if (hasOutPutFile )
            WriteOutPut(aContents);
    }


    private String  printDate(){
        return "["+ new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(new Date()) + "]\t";
    }

    public String PrintLineno(){
        return "" +  Thread.currentThread().getStackTrace()[4].getFileName()+ ":" + Thread.currentThread().getStackTrace()[4].getLineNumber();

    }

    public String  _PrintLineno(){
        int a ;
        String a1;
        Throwable dummyException=new Throwable();
        StackTraceElement locations[]=dummyException.getStackTrace();
        // Caller will be the third element
        String cname="unknown";
        String method="unknown";
        if( locations!=null && locations.length >3 ) {
            StackTraceElement caller=locations[4];
            cname=caller.getClassName();
            method=caller.getMethodName();
            return "" + locations[4].getFileName()
                    + ":"
                    + locations[4].getMethodName() + ":"
                    +   locations[4].getLineNumber()+" " ;
             /*   a =Thread.currentThread().getStackTrace()[2].getLineNumber();

                //a.
                a1=  Integer.toString(a);
                return a1;
                */

        }
        else
//                    return "0";
            return "" +  Thread.currentThread().getStackTrace()[4].getFileName()+ ":" + Thread.currentThread().getStackTrace()[4].getLineNumber();




    }

    public String GetErrFromStack (Throwable e  ){

        final Writer result = new StringWriter();
        final PrintWriter printWriter = new PrintWriter( result );
        e.printStackTrace( printWriter );
        String ss = result.toString() ;

        return e.getMessage() + ss;
    }
//--------------------------------------------------------------------------------------------------------

    public String GetErrFromStack (Exception e  ){

        final Writer result = new StringWriter();
        final PrintWriter printWriter = new PrintWriter( result );
        e.printStackTrace( printWriter );
        String ss = result.toString() ;

        return e.getMessage() + ss;
    }
//-------------------------------------------------------------------------------


    public String GetErrFromStack (Error e  ){

        final Writer result = new StringWriter();
        final PrintWriter printWriter = new PrintWriter( result );
        e.printStackTrace( printWriter );
        String ss = result.toString() ;

        return e.getMessage() + ss;
    }
    public void WriteErrorStack(String aContents , Error e){

        try {
            setContents(_sFilename , "[ERROR]\t"+aContents + "\t" +
                    GetErrFromStack(e)


                    + "\n");
            //setContents(_sFilename , "[ERROR]\t"+aContents + "\t" + e.printStackTrace( new PrintWriter( new StringWriter() ) )     );
        }
        catch (IOException isi){ //   throws new  IOException(isi);

            try {        setContents(_sFilename ,GetErrFromStack(isi) );           }
            catch (FileNotFoundException eFnotFE){}
            catch (IOException eFnotFE) {}

        }
    }


    public void WriteErrorStack(String aContents , Throwable e){

        try {
            setContents(_sFilename , "[Throwable]\t"+aContents + "\t" +
                    GetErrFromStack(e)


                    + "\n");
            //setContents(_sFilename , "[ERROR]\t"+aContents + "\t" + e.printStackTrace( new PrintWriter( new StringWriter() ) )     );
        }
        catch (IOException isi){ //   throws new  IOException(isi);

            try {        setContents(_sFilename ,GetErrFromStack(isi) );           }
            catch (FileNotFoundException eFnotFE){}
            catch (IOException eFnotFE) {}

        }
    }




    public void WriteErrorStack(String aContents , Exception e){

        try {
            setContents(_sFilename , "[ERROR]\t"+aContents + "\t" + GetErrFromStack(e) + "\n");
            //setContents(_sFilename , "[ERROR]\t"+aContents + "\t" + e.printStackTrace( new PrintWriter( new StringWriter() ) )     );
        }
        catch (IOException isi){ //   throws new  IOException(isi);

            try {        setContents(_sFilename ,GetErrFromStack(isi) );           }
            catch (FileNotFoundException eFnotFE){}
            catch (IOException eFnotFE) {}

        }
    }



    // NMEED TO TYERST
    public void WriteErrorStack(String aContents , IOException e){

        try {
            setContents(_sFilename , "[ERROR]\t"+aContents + "\t" + GetErrFromStack(e));
            //setContents(_sFilename , "[ERROR]\t"+aContents + "\t" + e.printStackTrace( new PrintWriter( new StringWriter() ) )     );
        }
        catch (IOException isi){ //   throws new  IOException(isi);

            try {        setContents(_sFilename ,GetErrFromStack(isi) );           }
            catch (FileNotFoundException eFnotFE){}
            catch (IOException eFnotFE) {}

        }
    }



    public void set_SYSTEM_OUT(String value){
        _SYSTEM_OUT = value;

    }

    public void set_fileFormat(String value){
        _fileFormat = value;

    }
    public void set_fileType(String value){
        _fileType = value;

    }

    public void SwitchLogfile(){

        int _intNewdate = Integer.parseInt(new java.text.SimpleDateFormat("yyyyMMdd").format(new java.util.Date()));
        int _intCurdate = Integer.parseInt(new java.text.SimpleDateFormat("yyyyMMdd").format(fileCreateDatetime));

        if (_intNewdate > _intCurdate)// switch the log filer
        {
            _objFile = new File(getFilename());
            fileCreateDatetime = new Date();
        }
    }




//-=------------------------------------------------------------

    public synchronized void finalize() throws java.lang.Throwable {

        try {
            Writer output = null;
            try {

                output = new BufferedWriter( new FileWriter(_objFile, true)  );

                output.write( "------------------------------------------------------------------------------------" );
                output.write( System.getProperty("line.separator") );


            }
            finally {
                //flush and close both "output" and its underlying FileWriter
                if (output != null)  output.close();

            }
            output= null;
            _objFile = null;

            setOutContents(_sOutputFilename,   System.getProperty("line.separator")
            );




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
//        	////System.out.println (" Finally clause of Tquery;");
            //_handle.close();
            super.finalize();
        }

    }

    public void WriteError(String error_, Error e) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }





}
