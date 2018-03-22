/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbutils;

import bj.BJCLogger;
import static dbutils.idrive.lPropertyReader;
import static dbutils.idrive.lSumBJCLogger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import dbutils.Dbtables;
import static dbutils.idrive.lPropertyReader;
import static dbutils.idrive.lSumBJCLogger;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
import java.util.stream.Collectors;
import java.util.List;
import org.eclipse.jgit.api.Git;

import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;




import bj.fileutils;
import static bj.fileutils.bkupFile;
import static bj.fileutils.delallFiles;
import java.util.Collection;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.LsRemoteCommand;

import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.RefUpdate;


/**
 *
 * @author TGAJ2
 */
public class ipurge extends idrive  {
    public String SRC_DB_URL;
    public String SRC_DB_USER;
    public String SRC_DB_PASSWORD;
    public String SRCdbDriver;
    public Dbtables objDBts;
    protected iDataloc objSrcloc;
    ResultSet objSelrs = null;
    PreparedStatement _selStatement = null;
    
    public String qlikProj= "";
    public String qlikLoadScriptName= "";
    public String qlikTableName4qvs = "";
    public String qlikTableSchemaName4qvs = "";
    
    
    
    
    
    public  String sFilespearatot; // = System.getProperty("file.separator");
    public  String sFullOutFilename;// = this.qlikProj+ sFilespearatot + this.qlikLoadScriptName;
    public  String sFullOutPath;// = this.qlikProj+ sFilespearatot + this.qlikLoadScriptName;

    public void setsFilespearatot() {
        this.sFilespearatot = System.getProperty("file.separator");
    }

    public void setsFullOutFilename() {
        this.sFullOutFilename = this.qlikProj+ sFilespearatot + this.qlikLoadScriptName;
        this.sFullOutPath = this.qlikProj+ sFilespearatot;
    }

    public ipurge(String SRC_DB_URL) {
          // Get the URL from Propert file
        SRC_DB_URL = lPropertyReader.getProperty("SRC.DB.URL");
        SRC_DB_USER = lPropertyReader.getProperty("SRC.DB.USER");
        SRC_DB_PASSWORD = lPropertyReader.getProperty("SRC.DB.PASSWORD");
        // Get Db driver info property file
        SRCdbDriver = lPropertyReader.getProperty("SRC.dbDriver");
        setsFilespearatot();
        try {
            //Initaite Src Db location object
            Class.forName(SRCdbDriver);
        } 
        catch (ClassNotFoundException ex) 
        {
            Logger.getLogger(iCopy.class.getName()).log(Level.SEVERE, null, ex);
        }
        objSrcloc = new iDataloc(SRC_DB_URL, SRCdbDriver, SRC_DB_USER, SRC_DB_PASSWORD);
        _connSrc = objSrcloc._conn;

        // initiate the dbtables object
        objDBts = new Dbtables();
        
    }
    
    
    public void getTabBySchema(String sFrmSchema, String sToSchema, String sTablename) {
        String sTabName = "";
        String sSchemaName = "";
        lSumBJCLogger.setSYSTEM_LOG_OUT(true);

        try {
            // Get the table list in a schema
            objSelrs = objSrcloc._conn
                    .getMetaData()
                        .getTables(null, sFrmSchema, sTablename, new String[]{"TABLE", "ALIAS"});

            objDBts.objFrmSchema = new ischema();
            objDBts.objFrmSchema.setName(sFrmSchema);
            objDBts.objFrmSchema.setDbType(dbtype.db.valueOf(lPropertyReader.getProperty("SRC.DB.TYPE") ) );
            
            objDBts.objToSchema = new ischema();
            objDBts.objToSchema.setName(sToSchema);
            objDBts.objToSchema.setDbType(dbtype.db.valueOf(lPropertyReader.getProperty("SRC.DB.TYPE") ) );
            
            objDBts.conn1 = objSrcloc._conn;

            objSrcloc.iprintDbinfo("Source");


            //objDBts.bprintFK = true;
            
            //lSumBJCLogger.setSYSTEM_LOG_OUT(true);

            while (objSelrs.next()) {
                sTabName = objSelrs.getString(3);
                sSchemaName = objSelrs.getString(2);
                objDBts.getTabDetails(sFrmSchema, sToSchema, sTabName);
                //lSumBJCLogger.setSYSTEM_LOG_OUT(true);
                lSumBJCLogger.WriteLog("table = " + sTabName + " 2 " + sSchemaName + ";TABLE_TYPE: " + objSelrs.getString("TABLE_TYPE")
                                    );
                lSumBJCLogger.setSYSTEM_LOG_OUT(false);
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
    
   
  public String rep(String s ){
      String sfldname = s;
     if (sfldname.contains("_FK"))
         sfldname = sfldname+ " as  %" + sfldname.replace("_FK", "_ID");
     else if (sfldname.contains("_PK")) 
         sfldname = sfldname+ " as  %" + sfldname.replace("_PK", "_ID") ;       
     else if (sfldname.contains("_IND")) 
          sfldname = sfldname+ " as  %" + sfldname.replace("_IND", "_IND_") ;     
     else if (sfldname.equalsIgnoreCase("Source Record Number"))         
         sfldname = "//" + sfldname.replace(" ", "") ;
    return sfldname ;
  }    
  
  
  public String repPhase3(String s ){
      String sfldname = s;
     if (sfldname.contains("_FK"))
         sfldname =  "%" + sfldname.replace("_FK", "_ID");
     else if (sfldname.contains("_PK")) 
         sfldname = "%" + sfldname.replace("_PK", "_ID") ;       
     else if (sfldname.contains("_IND")) 
          sfldname = "%" + sfldname.replace("_IND", "_IND_") ;     
     else if (sfldname.equalsIgnoreCase("Source Record Number"))
         sfldname = "//" + sfldname.replace(" ", "") ;     

    return sfldname ;
  }    
  
  public void genqvsp1( String sphase){
   String strsql = "";
   String strFrmSch = objDBts.objFrmSchema.getName();
   String strInsSql = "";
   String sStartTime = "";
   String WhereClause = "";
   String sContent= "";
   
        String strFKSQL = "";
         tfield lfield = new tfield();
        lfield.setName("RECNo () as SourceRecordNumber");
   for (itable itab : objDBts.objToSchema.gettables()) 
       
       {    
           itab.getfields().add(1,lfield);
        List<String>FKSQL   = itab.getfields().stream()
                                .map(e ->  ((tfield) e).getName() +  "//" + ((tfield) e).getType()  )
                                .collect(Collectors.toList());
        if (FKSQL != null) {
            strFKSQL = FKSQL.stream().map(Object::toString)
                    .collect(Collectors.joining("\" \n\t\",", "\t\",", "\";"));
        }
         strFKSQL = strFKSQL.replace("\"RECNO () AS SOURCERECORDNUMBER\"","RECNo () as SourceRecordNumber" );
            
         sContent =   "\n" 
            //       + String.format("///$tab %s \n",   itab.getName().replaceAll("_", " ")          )
                  // + String.format("SET vTableName = '%s';\n", itab.getName()+sphase       )
                  //  + "$(vTableName):\n"
                   + "LOAD "+ strFKSQL
                   + String.format("\nSQL SELECT * \n FROM %s.\"%s\" fetch first 10 rows only ;\n CALL DIM_AUDIT_SAVE;\n" , itab.OwnerName,  itab.getName()  )
                   + "\n"   ;    

         write2File(itab.getName() , sContent, sphase )  ;
        //else itab.= false
    }// end for
  }
  
    public void genqvsPhase2(String sphase) throws IOException, Throwable {
        String strFrmSch = objDBts.objFrmSchema.getName();
        String sContent = "";
        String strFKSQL = "";
        tfield lfield = new tfield();
        lfield.setName("Source Record Number");

        for (itable itab : objDBts.objToSchema.gettables()) {
            itab.getfields().add(1, lfield);

            List<String> FKSQL = itab.getfields().stream()
                    .filter(e -> !((tfield) e).getName().equalsIgnoreCase("RECNo () as SourceRecordNumber"))
                    .map(e -> rep(((tfield) e).getName()))
                    .collect(Collectors.toList());

            if (FKSQL != null) {
                strFKSQL = FKSQL.stream().map(Object::toString)
                        .collect(Collectors.joining(", \n\t", "\t", ""));
            }

            sContent = "\n"
//                    + String.format("//$tab %s \n", itab.getName().replaceAll("_", " "))
                   // + String.format("SET vTableName = '%s';\n", itab.getName() + sphase)
                  //  + "$(vTableName):\n"
                    + "LOAD " + strFKSQL
                    + String.format("\n FROM   [$(vG.SourcePHS2)\\%sPHASE1.qvd]\n (qvd);\n  CALL StoreAndDrop_PHS2;\n", itab.getName())
                    + "\n";

            write2File(itab.getName(), sContent, sphase);

            //else itab.= false
        }// end for

    }
    
    
    public void genqvsPhase3(String sphase) throws IOException, Throwable {
        String strFrmSch = objDBts.objFrmSchema.getName();
        String sContent = "";
        String strFKSQL = "";
        tfield lfield = new tfield();
        lfield.setName("SourceRecordNumber");

        for (itable itab : objDBts.objToSchema.gettables()) {
            itab.getfields().add(1, lfield);

            List<String> FKSQL = itab.getfields().stream()
                    .filter(e -> !((tfield) e).getName().equalsIgnoreCase("RECNo () as SourceRecordNumber"))
                    .map(e -> repPhase3(((tfield) e).getName())  )
                    .collect(Collectors.toList());

            if (FKSQL != null) {
                strFKSQL = FKSQL.stream().map(Object::toString)
                        .collect(Collectors.joining(", \n\t", "\t", ""));
            }

            sContent = "\n"
                   // + String.format("//$tab %s \n", itab.getName().replaceAll("_", " "))
                   //+ String.format("SET vTableName = '%s';\n", itab.getName() + sphase)
                   // + "$(vTableName):\n"
                    + "LOAD " + strFKSQL
                    + String.format("\n FROM   [$(vG.SourcePHS2)\\%sPHASE2.qvd]\n (qvd);\n  CALL MAPPINGCONFDIM_SUB;\n", itab.getName())
                    + "\n";

            write2File(itab.getName(), sContent, sphase);

            //else itab.= false
        }// end for

    }
          
  public void  write2File(String sFilename, String sContents, String sphase)    {
    String updCont = sContents;
    String sFullInFilename = lPropertyReader.getProperty("SQL.DDL.PATH") + sFilename + sphase+".qvs";
    try {
          bj.BJCLogger lSumBJCLogger;
          
          
          lSumBJCLogger = new BJCLogger(lPropertyReader.getProperty("LOG.PATH") +sFilename,
                  sFullInFilename , "//"          );
          
          lSumBJCLogger.set_fileType("html");
          lSumBJCLogger.set_SYSTEM_OUT("TRUE");
          lSumBJCLogger.setSYSTEM_LOG_OUT(false);
          updCont = 
                "//--------------------------------------------------------------------------------\n"
                + String.format("///$tab %s \n",   sFilename.replaceAll("_", " ") ) // filename has the tablename
                + "LET vStartTime = Now(); \n"
                + String.format("SET vTableName = '%s';\n", sFilename + sphase)
                + "Trace Started : $(vTableName) at \t starttime: $(vStartTime); \n" 

                + "$(vTableName):\n"
                + updCont + "\n"
                + "LET vEndTime = Now();\n"
                + "Trace Time to load: $(vTableName) is $(vDuration). \t starttime: $(vStartTime) \t endtime: $(vEndTime);\n" 
                + "//--------------------------------------------------------------------------------\n"
            ;
          lSumBJCLogger.WriteOut(updCont);
          
          lSumBJCLogger = null;
          
          // Append H:\asd\qwe-prj
          
          
          String filename= this.qlikProj+ sFilespearatot + this.qlikLoadScriptName;
          
          
          fileutils.appendFile(sFullInFilename,sFullOutFilename,sphase );
          
          
          
          

      } catch (FileNotFoundException e) {
          _iErrDesc = "FileNotFoundException !! in HL7Parser Err Code" + "\n" + e.getMessage();
          System.out.println("\nError:" + _iErrDesc);
      } catch (IOException e) {
          _iErrDesc = "FileNotFoundException !! in HL7Parser Err Code" + "\n" + e.getMessage();
          System.out.println("\nError:" + _iErrDesc);
      }
  
  
  
  }
  
  public void AppendaFile(String sSrcFileName, String sDestFilename)
  {
/* this will appned a file w/ another file 
      First bake a backup of source 
      
      */      
  
  
  
  }
    
  public void Delerows()  {
   String strsql = "";
   String strFrmSch = objDBts.objFrmSchema.getName();
   String strInsSql = "";
   String sStartTime = "";
   String WhereClause = "";
     for (itable itab : objDBts.objToSchema.gettables()) {
                // verify if  table is not created in destination
                //if (objDBts.isDbtable(objDBts.objFrmSchema.getName().toLowerCase(), itab.getName().toLowerCase())) {
                if (objDBts.isDbtable(objDBts.objFrmSchema.getName(), itab.getName())) { // db2 doens't like toLower
                   
                    // if it has clob/ blob then lets chnage the fetch size
                    sStartTime = "Begin" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());
                    strsql = "Select " + itab.getName() + ".* from " + strFrmSch + "." + itab.getName() + " " + itab.getName();//+ " limit 10";
                    // recursively get the whereclause

                    
                        WhereClause = objDBts.getRecuriveFKs1(itab, objDBts.objFrmSchema.getName(), 10);// get this from from Schema Clause
                        //WhereClause = WhereClause + "  LIMIT " + limitSize;
                        
                    
                    System.out.println("--------------------------------------------------------------------------------"    )
                    ;System.out.println("table = " + itab.getName() + "\n " + strsql + " WhereClauseaa = " + WhereClause);
                    System.out.println("--------------------------------------------------------------------------------"    )
                  ;

                }// dont process the table if the table is no created in destination
                //else itab.= false
            }// end for
  
  }
  
  
  public void gitStuff(){
      
//      FileRepositoryBuilder repositoryBuilder = new FileRepositoryBuilder();
  //    repositoryBuilder.setMustExist( true );

    //    repositoryBuilder.setGitDir("/path/to/repo");

//    Repository repository = repositoryBuilder.build();

      
      //zFYC8dVDrVUWG13-STSE -- git eee 
      
      String REMOTE_URL = "https://gitee.unigroupinc.com/BI/GIT_SCRIPTS.git";
     LsRemoteCommand lsRemote = Git.lsRemoteRepository().setRemote(REMOTE_URL)
                        .setCredentialsProvider(null);
  
  
  }
    

    @Override
    void setDbConnection() throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    void _MergePlacerOrder(LinkedHashMap HL7Msg) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    void sqlDbConnection() throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected void _updateFillerInfo(LinkedHashMap HL7Msg) throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    
    public void genQvs(){
    try {
        getTabBySchema(qlikTableSchemaName4qvs, qlikTableSchemaName4qvs, qlikTableName4qvs);
        //genqvsp1("PHASE1");
       // genqvsPhase2("PHASE2");
       // genqvsPhase3("PHASE3");
        
        } catch (Throwable ex) {
            Logger.getLogger(ipurge.class.getName()).log(Level.SEVERE, null, ex);
        }
    
    }
    
    public void delQvs(){
        delallFiles(lPropertyReader.getProperty("SQL.DDL.PATH"));
    }
    
    public static void main(String[] args) {
        ipurge ilpurge = new ipurge("");
//        licopy.getTabBySchema("tms", "tms_tmp");
        //   licopy.getTabBySchema("qtg", "qtg_tmp");  
        
        //ilpurge.getTabBySchema("UNIPROD", "UNIPROD", "%OPERATOR_DIM%");
        ilpurge.qlikProj = "H:\\asd\\qwe-prj";
        ilpurge.qlikLoadScriptName = "LoadScript.txt";
        ilpurge.sFilespearatot = System.getProperty("file.separator");
        ilpurge.setsFullOutFilename();
        
        // Wipe out all junk 
        bkupFile(ilpurge.sFullOutPath+"LoadScript_template.txt" 
                    ,ilpurge.sFullOutPath+"LoadScript.txt" );
        
        // remove files 
        delallFiles(lPropertyReader.getProperty("SQL.DDL.PATH"));
        
        
        bkupFile(ilpurge.sFullOutFilename,ilpurge.sFullOutFilename +"_" +"Bkup" );

        
        ilpurge.qlikTableSchemaName4qvs = "UNIPROD";
//        ilpurge.qlikTableName4qvs = "%OPERATOR_DIM%";
//        ilpurge.genQvs();
        ilpurge.qlikTableName4qvs = "%FINANCIAL%";
        ilpurge.genQvs();
        ilpurge = null;
       
       

    }

    
    


}



