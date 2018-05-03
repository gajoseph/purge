/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbutils;

import bj.BJCLogger;
import bj.fileutils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.LsRemoteCommand;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static bj.fileutils.bkupFile;
import static bj.fileutils.delallFiles;
import static dbutils.fkid.*;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;


/**
 *
 * @author  gfdf TGAJ2 testing merge
 */
public class ipurge extends idrive  {
    public String SRC_DB_URL;
    public String SRC_DB_USER;
    public String SRC_DB_PASSWORD;
    public String SRCdbDriver;
    public static Dbtables objDBts;
    protected iDataloc objSrcloc;
    ResultSet objSelrs = null;
    PreparedStatement _selStatement = null;
    
    public String qlikProj= "";
    public String qlikLoadScriptName= "";
    public String qlikTableName4qvs = "";
    public String qlikTableSchemaName4qvs = "";
    idTabs lidTabs ;

    
    
    
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
        lidTabs = new idTabs();
        setsFilespearatot();
        try {
            Class.forName(SRCdbDriver);
        } 
        catch (ClassNotFoundException ex) 
        {
            lSumBJCLogger.WriteErrorStack("GetShcmea ", ex);
        }
        objSrcloc = new iDataloc(SRC_DB_URL, SRCdbDriver, SRC_DB_USER, SRC_DB_PASSWORD);
        _connSrc = objSrcloc._conn;

        /* initiate the dbtables object
        * */
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
                        .getTables(null, sFrmSchema, sTablename, new String[]{"TABLE", "AL_IAS"});

            objDBts.objFrmSchema = new ischema(sFrmSchema);
            objDBts.objFrmSchema.setName(sFrmSchema);// remove this line
            objDBts.objFrmSchema.setDbType(dbtype.db.valueOf(lPropertyReader.getProperty("SRC.DB.TYPE") ) );
            
            objDBts.objToSchema = new ischema(sToSchema);
            objDBts.objToSchema.setName(sToSchema);// remove this line after testing
            objDBts.objToSchema.setDbType(dbtype.db.valueOf(lPropertyReader.getProperty("SRC.DB.TYPE") ) );
            
            objDBts.conn1 = objSrcloc._conn;

            objSrcloc.iprintDbinfo("Source");
            objSrcloc.iprintDbtypes();
            


            //objDBts.bprintFK = true;
            
            //lSumBJCLogger.setSYSTEM_LOG_OUT(true);

            while (objSelrs.next()) {
                sTabName = objSelrs.getString(3);
                sSchemaName = objSelrs.getString(2);
                lSumBJCLogger.setSYSTEM_LOG_OUT(true);
                lSumBJCLogger.WriteLog("Starting table = " + sTabName + " ; " + sSchemaName + ";TABLE_TYPE: " + objSelrs.getString("TABLE_TYPE"));
                lSumBJCLogger.setSYSTEM_LOG_OUT(false);
                objDBts.getTabDetails(sFrmSchema, sToSchema, sTabName);
                
                
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
    
  
 public String repPahse1(tfield tfld ){
      String sfldname = "";
      if (tfld.getType().equalsIgnoreCase("DATE")){
          sfldname = //"//" + tfld.getName() + 
                  String.format("Floor(date(\"%s\"))as \"" + tfld.getName().replace("_FK", "") +"\"", tfld.getName(), tfld.getName()) ;
          tfld.setName(tfld.getName().replace("_FK", ""));
          
      }
      else 
          sfldname = tfld.getName();
    return sfldname ;
  }    
     
    
  public String repPahse2(String s ){
      String sfldname = s;


      if (sfldname.contains("_DATE_FK"))
          sfldname = sfldname.replace("_FK", "");//+ " as  %" + sfldname.replace("_FK", "_ID");
      else if (sfldname.contains("_FK"))
         sfldname = sfldname+ " as  %" + sfldname.replace("_FK", "_ID");
     else if (sfldname.contains("_PK")) 
         sfldname = sfldname+ " as  %" + sfldname.replace("_PK", "_ID") ;       
     else if (sfldname.contains("_IND")) 
          sfldname = sfldname+ " as  %" + sfldname.replace("_IND", "_IND_") ;     
     else if (sfldname.equalsIgnoreCase("Source Record Number"))         
         sfldname = "//" + sfldname.replace(" ", "") ;
     else if (sfldname.contains("_CNT"))
          sfldname = sfldname  + ",\n\tIF (" + sfldname + "= 0, Dual('NO',0), Dual('YES',1)) as " + sfldname.replace("_CNT", "_IND");

      return sfldname ;
  }    
  
  
  public String repPhase3(String s ){
      String sfldname = s;
     if (sfldname.contains("_FK"))
         sfldname =  "%" + sfldname.replace("_FK", "_ID");
     else if (sfldname.contains("_PK")) 
         sfldname = "%" + sfldname.replace("_PK", "_ID") ;       
     else if (sfldname.contains("_IND")) 
          sfldname = "%" + sfldname.replace("_IND", "_IND") ;
     else if (sfldname.equalsIgnoreCase("Source Record Number"))
         sfldname = "//" + sfldname.replace(" ", "") ;
     else if (sfldname.contains("_CNT"))
         sfldname = sfldname  + ",\n " + sfldname.replace("_CNT", "_IND");

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
        genqvsp11(itab,"PHS1");
    }
  }
  
  public String memTab(itable itab, boolean  brcnumber )
  {
    String strFKSQL = "";
    String slimiting = "";
       List<String>FKSQL   = itab.getfields().stream()
                       .map(e ->     
                                   //+ "\t\t//" + ((tfield) e).getType()
                                     String.format("%-150s  %-50s",repPahse1(((tfield) e)), "//"+((tfield) e).getType() )
                           )
                       .collect(Collectors.toList());
        if (FKSQL != null) 
        {
            strFKSQL = FKSQL.stream().map(Object::toString)
                    .collect(Collectors.joining("\n\t,", "\t", "\n;")); // .collect(Collectors.joining("\n\t,\"", "\t\"", "\n;"));
        }
        if (brcnumber== true )
         strFKSQL = strFKSQL.replace("RECNO () AS SOURCERECORDNUMBER","RECNo () as SourceRecordNumber" );

            
        String  sContent =   "\n" 
                   + "LOAD "+ strFKSQL
                   + String.format("\nSQL SELECT * \n FROM %s.\"%s\" %s ;\n CALL DIM_AUDIT_SAVE;\n" , "UNIDWHS",  itab.getName() , dbtype.limit_fetchrows()   )
                   + "\n"   ;   
        return sContent;
  }
  
   public void genqvsp11( itable itab, String sphase){
   String strsql = "";
   String strFrmSch = objDBts.objFrmSchema.getName();
   String strInsSql = "";
   String sStartTime = "";
   String WhereClause = "";
   String sContent= "";
   String strFKSQL = "";
   tfield lfield = new tfield();
   lfield.setName("RECNo () as SourceRecordNumber");
   if  (itab.getName().equalsIgnoreCase(qlikTableName4qvs.replace("%", ""))) 
    {
        itab.getfields().add(1,lfield);
         
         for ( iindex idx : itab.Indexes.getindexes())
         {// check if the cardinality is very low 
             System.out.println(idx.CARDINALITY + " " +  idx.getName());
         
         }    
         
         for ( fkTable ift : itab.fktables.gettables())
         {// check if the cardinality is very low 
             System.out.println("TAb is " + itab.getName()+";"+ift.FkColumn.field.getName()  + ";  " +  ift.PKColumn.field.getName() + ";"+
             ift.PKColumn.CON_TABLE
             
             );
             for ( iindex id : objDBts.objToSchema.gettable(ift.PKColumn.CON_TABLE).Indexes.getindexes())        
             {
               if (id.getIndexField(ift.PKColumn.field.getName()).indxField.getName()== ift.PKColumn.field.getName())
               {    
                    System.out.println( String.format(" idx= %s; cardin=%s", id.getName(), id.CARDINALITY));
                    if (Integer.parseInt(id.CARDINALITY) <=1000)
                        write2File(itab.getName() , (memTab( objDBts.objToSchema.gettable(ift.PKColumn.CON_TABLE), false)), sphase,  ift.PKColumn.CON_TABLE )  ;
               } 
             }
         
            }
         write2File(itab.getName() , (memTab( itab, true)), sphase,  itab.getName())  ;
       
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
                    .map(e -> repPahse2(((tfield) e).getName()))
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
                    + String.format("\n FROM   [$(vG.SourcePHS2)\\%sPHS1.qvd]\n (qvd);\n  CALL StoreAndDrop_PHS2;\n", itab.getName())
                    + "\n";

            write2File(itab.getName(), sContent, sphase, itab.getName());

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
                    .map(e -> repPhase3(((tfield) e).getName()))
                    .collect(Collectors.toList());

            if (FKSQL != null) {
                strFKSQL = FKSQL.stream().map(Object::toString)
                        .collect(Collectors.joining(", \n\t", "\t", ""));
            }

            sContent = "\n"
                    + "LOAD " + strFKSQL
                    + String.format("\n FROM   [$(vG.SourcePHS3)\\%sPHS2.qvd]\n (qvd);\n  CALL MAPPINGCONFDIM_SUB;\n", itab.getName())
                    + "\n";

            write2File(itab.getName(), sContent, sphase, itab.getName());

            //else itab.= false
        }// end for

    }
          
  public void  write2File(String sFilename, String sContents, String sphase, String sTablename)    {
    String updCont = sContents;
    String sFullInFilename = lPropertyReader.getProperty("SQL.DDL.PATH") + sFilename + sphase+".qvs";
    try {
          bj.BJCLogger lSumBJCLogger;
          
          
          lSumBJCLogger = new BJCLogger(lPropertyReader.getProperty("LOG.PATH") +sFilename,
                  sFullInFilename , "//"          );
          
          lSumBJCLogger.set_fileType("html");
          lSumBJCLogger.set_SYSTEM_OUT("TRUE");
          lSumBJCLogger.setSYSTEM_LOG_OUT(false);
          String sIncludes = "";

          if (sphase.equalsIgnoreCase("PHS1"))
            sIncludes = "\n" + "$(Include=d:/qpub_qvd/global/connectionstring/qvconnstrng.qvs);\n"
            + "$(Include=d:/qpub_qvd/global/global.qvs);\n"
            + "$(Include=d:/qpub_qvd/global/subphs1.qvs);\n"
            + "// $(Include=H:/QPUB_QVD/GLOBAL/Global.qvs);\n"
            + "// $(Include=H:/QPUB_QVD/GLOBAL/SubPHS1.qvs);\n"
            + "// $(MUST_Include=H:/Qlik Development/QDEV/GLOBAL/QVConnStrng.qvs);\n";
          else if (sphase.equalsIgnoreCase("PHS2"))
              sIncludes = "\n"
                      + "$(Include=d:/qpub_qvd/global/global.qvs);\n"
                + "$(Include=d:/qpub_qvd/global/subphs2.qvs);\n"
                + "// $(Include=H:/QPUB_QVD/GLOBAL/Global.qvs);\n"
                + "// $(Include=H:/QPUB_QVD/GLOBAL/SubPHS2.qvs);\n"
        ;

          else if (sphase.equalsIgnoreCase("PHS3"))
              sIncludes = "\n"
                      + "$(Include=d:/qpub_qvd/global/global.qvs);\n"
                      + "$(Include=d:/qpub_qvd/global/subphs3.qvs);\n"
                      + "// $(Include=H:/QPUB_QVD/GLOBAL/Global.qvs);\n"
                      + "// $(Include=H:/QPUB_QVD/GLOBAL/SubPHS3.qvs);\n"
                      ;

        updCont =
                "//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n"
                + String.format("// Name: %s%s.qvw\n",sFilename,sphase)
                        + String.format("// Author: George Joseph %s\n", System.getProperty("user.name"))
                        + String.format("// Created: %s \n", new java.text.SimpleDateFormat("mm/dd/yyyy").format(new Date()))
                        + String.format("// Purpose: \t Created %s for %s \n", sphase ,sTablename )
                        + "//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n\n"
                + String.format("///$tab %s \n",   sTablename.replaceAll("_", " ") ) // filename has the tablename
                + sIncludes + "\n"
                + "LET vStartTime = Now(); \n"
                + String.format("SET v%s = '%s';\n","TableName", sTablename +  ((sphase=="PHS3")? "":  sphase ) )
                        + String.format("Trace Started : $(v%s) at \t starttime: $(vStartTime); \n","TableName" )

                + String.format("$(v%s):\n", "TableName" )
                + updCont + "\n"
                + "LET vEndTime = Now();\n"
                + String.format("Trace Time to load: $(v%s)  \t starttime: $(vStartTime) \t endtime: $(vEndTime);\n" , "TableName")
                + "//--------------------------------------------------------------------------------\n"            ;

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

  public boolean isTabCandiadte(itable itab){
      String ALL_TAB_EXCLUDE_ENDS_WITH= lPropertyReader.getProperty("ALL.TAB.EXCLUDE.ENDS.WITH");
      String ALL_TAB_EXCLUDE_CONTAINS= lPropertyReader.getProperty("ALL.TAB.EXCLUDE.CONTAINS");
      String ALL_TAB_EXCLUDE_BEGIN_WITH=lPropertyReader.getProperty("ALL.TAB.EXCLUDE.BEGIN.WITH");
      String TAB_EXCLUDE_LIST[] = lPropertyReader.getProperty("TAB.EXCLUDE.LIST").split(",");// this has to be split and

      boolean returnVal= true;
      if (!ALL_TAB_EXCLUDE_ENDS_WITH.equalsIgnoreCase(""))
          if (itab.getName().endsWith(ALL_TAB_EXCLUDE_ENDS_WITH))
              returnVal=false;

      if (!ALL_TAB_EXCLUDE_CONTAINS.equalsIgnoreCase(""))
          if (itab.getName().contains(ALL_TAB_EXCLUDE_CONTAINS))
              returnVal= false;

      if (!ALL_TAB_EXCLUDE_BEGIN_WITH.equalsIgnoreCase(""))
          if (itab.getName().startsWith(ALL_TAB_EXCLUDE_BEGIN_WITH))
              returnVal=false;

       returnVal = !Arrays.asList( TAB_EXCLUDE_LIST ).contains( itab.getName() );

      return returnVal;

  }
  /*
  --GEO--C--    Delete rows based on a criteria  add a paramter
  
  */  
  public void Delerows()  {
   String strsql = "";
   String strFrmSch = "";
   String strInsSql = "";
   String sStartTime = "";
   String WhereClause = "";
    if (objDBts.objFrmSchema==null)
      getTabBySchema(qlikTableSchemaName4qvs, qlikTableSchemaName4qvs, qlikTableName4qvs);

      strFrmSch = objDBts.objFrmSchema.getName();
     for (itable itab : objDBts.objToSchema.gettables()) {
        if (isTabCandiadte(itab))// need to convert in2 a list
        if (objDBts.isDbtable(objDBts.objFrmSchema.getName(), itab.getName()))
        //if (this.lidTabs.getidTabByTabName(itab.getName())==null)
        { /* db2 doens't like toLower */
            sStartTime = "Begin" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());
//          strsql = "Select " + itab.getName() + ".* from " + strFrmSch + "." + itab.getName() + " " + itab.getName();//+ " limit 10";

                /*
                for (fkTable fk: itab.fktables.gettables()) {
                    WhereClause = getRecuriveFKs1(
                            objDBts.objToSchema.gettable(fk.PKColumn.CON_TABLE)

                            , objDBts.objFrmSchema.getName(), 1,
                            lidTabs.getidTabbyTabName_pkName(fk.PKColumn.CON_TABLE, fk.PKColumn.field.getName())
                            );// get this from from Schema Clause
                }*/
            lidTabs.SqlpreparedStat = "";
            if ( itab.fktables.gettables().isEmpty())
            //for (fkTable fk: itab.dptables.gettables())
            {

                WhereClause = getRecuriveFKs1(
                        //objDBts.objToSchema.gettable(fk.PKColumn.CON_TABLE)
                        itab
                        , objDBts.objFrmSchema.getName(), 1,
                        //lidTabs.getidTabbyTabName_pkName(fk.PKColumn.CON_TABLE, fk.PKColumn.field.getName())
                        null
                );// get this from from Schema Clause



            }

                //WhereClause = WhereClause + "  LIMIT " + limitSize;


            System.out.println( "--------------------------------------------------------------------------------"    );
            System.out.println("table = " + itab.getName() + "\n " + strsql + " WhereClauseaa = " + WhereClause);
            System.out.println("--------------------------------------------------------------------------------"    )
          ;

        }// dont process the table if the table is no created in destination
        //else itab.= false
     }// end for
      /*
        new lop thru the itabs and print the delete statement
        get the last child; recurisevely thur all the way to the top level parents and see if that id can be deleted
        reason being if another child table row of the same parent may not qualify the parent's deletable status is set to false

       */

      for(int j = lidTabs.getidtabs().size() - 1; j >= 0; j--)
      { idTab it = lidTabs.getidtabs().get(j);
          System.out.println(it.getName() + " Generating Delete statement ");
          if (it.hasDelStatGenByAnotherParent==false) {
              lidTabs.CanDeletableStatus_new(it);
              DeleteRows(it, objDBts.objToSchema.gettable(it.getName()).getPKField().getName());
          }
      }
      System.out.println(sStartTime + "\t " +  "End" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date()));





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
        qlikTableSchemaName4qvs = "UNIPROD";
        qlikTableName4qvs = "%SHPM_CLAIM_02_DIM%";

        //qlikTableSchemaName4qvs = "test";
        //qlikTableName4qvs = "%tab1%";


        getTabBySchema(qlikTableSchemaName4qvs, qlikTableSchemaName4qvs, qlikTableName4qvs);
        genqvsp1("PHS1");
        genqvsPhase2("PHS2");
        genqvsPhase3("PHS3");
        
        } catch (Throwable ex) {
            Logger.getLogger(ipurge.class.getName()).log(Level.SEVERE, null, ex);
        }
    
    }
    
    public void delQvs(){
        delallFiles(lPropertyReader.getProperty("SQL.DDL.PATH"));
    }




    /****************************************************************************************************************/


    public void DeleteFromtopLevelParentRows(idTab tab, String sPKColname ){
        String Sdeleet = "";
        String NOdeleet = "";
        for (fkid fkid : tab.parentId_pkids.fkids())
            for (ids pids : fkid.Pks)
                if (pids.deleteable)
                    Sdeleet = Sdeleet + "'" + pids.FkID+"',";
                else
                    NOdeleet = NOdeleet + "'" + pids.FkID+"',";
        // createa  prepared statement; exec deletes by batches of ~2000 --batch size; threads even though at db is will be queued; it will be kind of parallel -- ready for next batch
        if (!Sdeleet.equalsIgnoreCase(""))
        {
            Sdeleet = Sdeleet.substring(0, Sdeleet.length() - 1) ;
            lSumBJCLogger.WriteOut(
                    String.format("\nDelete FROM %s.%s  where %s in (%s); \n /* %s*/", objDBts.objToSchema.getName(), tab.getName(), sPKColname
                            , Sdeleet, NOdeleet));


        }
    }



    public void DeleteRows(idTab tab, String sFKTableColname ){
        String Sdeleet = "";
        String NOdeleet = "";
        String sids2del = "";
        for (fkid fkid : tab.parentId_pkids.fkids()){
            for (ids pids : fkid.Pks) {

                if (pids.deleteable)
                    Sdeleet = Sdeleet+ pids.Pkids.stream().distinct().collect(Collectors.joining("','", "'", "'"));
                else
                    NOdeleet = NOdeleet + pids.Pkids.stream().distinct().collect(Collectors.joining("','", "'", "'"));
                System.out.println(pids.Pkids.stream().distinct().collect(Collectors.joining("','", "'", "'")) + "Sdeleet =  " + Sdeleet);
        }

        }
        sids2del = sids2del +  tab.parentId_pkids.fkids().stream().map( fkid -> fkid.Pks.stream()
                .filter(ids -> ids.deleteable==true )
                .map(ids -> ids.Pkids.stream()
                .map(Object::toString)
                .collect(Collectors.joining("','")))
                .collect(Collectors.joining("','")))
                .distinct()
                .collect(Collectors.joining("','", "('", "')"))
        ;
        tab.hasDelStatGenByAnotherParent = true;
        List<Map<Boolean, Map<List<String>, Long>>> dummy =  tab.parentId_pkids.fkids().stream().map((fkid fkid) -> fkid.Pks.stream().collect(groupingBy( (ids ids) -> ids.deleteable
                , groupingBy( (ids ids)-> ids.Pkids, Collectors.counting())) ))
                    .collect(Collectors.toList());

        Map<String, Long> dummy1= tab.parentId_pkids.fkids().stream().map(fkid->fkid.PKColumn.CON_TABLE)
                    .collect(groupingBy((String s)->s,   Collectors.counting()));



        Map<String, Map<String, Long>> dummy2;
        Map<String, Map<String, Long>> map = new HashMap<>();
       /* for (fkid s : tab.parentId_pkids.fkids()) {
            map.computeIfAbsent(s.getFkTabname(), key -> new HashMap<>()).computeIfAbsent(s.Pks.stream().collect(groupingBy((ids ids) -> ids.FkID, Collectors.counting())));
        }
*/        dummy2 = map;


        System.out.println("Sids " + sids2del + "dummy = " + dummy + " dummy 1= " + dummy1 + " dummy2 = " + dummy2);
        /*
        createa  prepared statement; exec deletes by batches of ~2000 --batch size; threads even though at db is will be queued; it will be kind of parallel -- ready for next batch
        createa  prepared statement; exec deletes by batches of ~2000 --batch size; threads even though at db is will be queued; it will be kind of parallel -- ready for next batch
        */
        if (!sids2del.equalsIgnoreCase(""))
        {
            //Sdeleet = Sdeleet.substring(0, Sdeleet.length() - 1) ;
            lSumBJCLogger.setSYSTEM_LOG_OUT(true);
            lSumBJCLogger.WriteOut(
                    String.format("\nDelete FROM %s.%s  where %s in %s; \n /* %s*/", objDBts.objToSchema.getName(), tab.getName(), sFKTableColname
                            , sids2del, NOdeleet));


        }
    }
// get JOIN CLuae when there is limit pull
// filter from the destination Db and get only data for those rows 
// Select * from tab1 a left join    
    public String getRecuriveFKs1(itable itab// Table name(child/1st starting table)
            , String s2Sch // to schema name 
            , int iRowCnt // level 1st sbiling 1; child's child then 2 
            , idTab pidTab // parent table'sPK info  {tab1, {pk1,2,3..n)}
    ) 
    {
        String FKTableName = "";// is the table table which refrences the primary/Unique  key on another table
        String PKTableName = ""; // is the table name which is refrenced by a FK table name 
        String FKTableColname = "";
        String PKTableColname = "";
        String DeleteSql= "";
        idTab lidTab= null;
        
        int i = 0; 
        if (pidTab == null) {
            /*
            * if pidTab is null add initialiize and add
            */
            pidTab = lidTabs.addTabs(itab.getPKField().getName(), objDBts.objToSchema.getName(), itab.getName(), pidTab, itab.getPKField().getName(), iRowCnt, null, null, null);

            getRecuriveFKs1(itab, objDBts.objToSchema.getName(), iRowCnt+2,pidTab);
            System.out.println(" PID is nyull " + itab.getPKField().getName() );
            //DeleteRows(pidTab, itab.getPKField().getName());
            return "";
        }

        // return if parent deons't
        if (pidTab.parentId_pkids.fkids().isEmpty())
            return "";
        if (itab.dptables != null) 
        {
            /*
            * if table has dp tables add them
            */
            for (fkTable fktab : itab.dptables.gettables())
            {
                i++;
                FKTableName = fktab.FkColumn.CON_TABLE;
                PKTableName = fktab.PKColumn.CON_TABLE;
                FKTableColname =  fktab.FkColumn.field.getName();
                PKTableColname =  fktab.PKColumn.field.getName();
                //inclause = "";
                lSumBJCLogger.setSYSTEM_LOG_OUT(true);
                lSumBJCLogger.WriteLog(
                        String.format("\n %sTab = %s\t; Parent =%s; PKcol= %s;  Child =%s FKCol= %s"
                                , (new String(new char[iRowCnt]).replace("\0", "\t"))
                                , itab.getName(),PKTableName,PKTableColname,   FKTableName, FKTableColname )
                );

                System.out.println(
                                String.format("%s%10d) Select %s From %s.%s where %s in ",  (new String(new char[iRowCnt]).replace("\0", "\t")),i
                                              , FKTableColname
                                              , objDBts.objToSchema.getName(),FKTableName
                                              , FKTableColname)

                                            );
                // build the delete statements

                // need to check if there are more than 1 PKS ie col1, col2 are pks
                // NEED TO CHECK IF PARENT HAS A QUALIFIED ROWS TO BE DELETD IF NOT WE NEED TO FLAG THE CHILDREN OUT THAT THEY CAN'T BE DELETED

                lidTab = lidTabs.addTabs(
                        objDBts.objToSchema.gettable(FKTableName).getPKField().getName()
                        , objDBts.objToSchema.getName()
                        , FKTableName
                        , pidTab
                        , FKTableColname
                        , iRowCnt
                        , fktab.FkColumn
                        , fktab.PKColumn
                        , fktab
                );

                getRecuriveFKs1(objDBts.objToSchema.gettable(FKTableName)
                                , objDBts.objToSchema.getName(), iRowCnt+2
                                ,lidTab
                                );
                System.out.println(" Inside 4 loop " + FKTableName);
               // DeleteRows(lidTab, fktab.FkColumn.field.getName());

            }    // end for
            System.out.println(
                    String.format("\n%s Select %s From %s.%s", (new String(new char[iRowCnt]).replace("\0", "\t")),PKTableColname , objDBts.objToSchema.getName(),PKTableName )
                );
        }
        else 
        {
            DeleteSql = String.format("Select %s from %s.%s ", itab.getPKField().getName(), objDBts.objToSchema.getName() ,  itab.getName()) ;
//            DeleteRows(pidTab, PKTableColname);// delete statement fro the top level
        }

        return DeleteSql;
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
        ilpurge.qlikTableName4qvs = "%CLM_ANLYS_ROLE_01_DIM%";
//        ilpurge.genQvs();
        //ilpurge.qlikTableName4qvs = "%AGNT_ACCT_03_DIM%";
        // TETSING W/ postgres 
        ilpurge.qlikTableSchemaName4qvs = "tms";
        ilpurge.qlikTableName4qvs = "%house_bill%";

        ilpurge.qlikTableSchemaName4qvs = "test";
        ilpurge.qlikTableName4qvs = "%tab%";



       // ilpurge.genQvs();
        ilpurge.Delerows();


        ilpurge = null;

    }

    
    


}



