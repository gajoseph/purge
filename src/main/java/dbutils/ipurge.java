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
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static bj.fileutils.bkupFile;
import static bj.fileutils.delallFiles;


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
    List<idTab> idtabs;
    public String scurrParent= "";
    public String SqlpreparedStat = "";
    
    
    
    
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
        idtabs = new ArrayList<idTab>();
        setsFilespearatot();
        try {
            //Initaite Src Db location object
            Class.forName(SRCdbDriver);
        } 
        catch (ClassNotFoundException ex) 
        {
            lSumBJCLogger.WriteErrorStack("GetShcmea ", ex);
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
                        .getTables(null, sFrmSchema, sTablename, new String[]{"TABLE", "AL_IAS"});

            objDBts.objFrmSchema = new ischema();
            objDBts.objFrmSchema.setName(sFrmSchema);
            objDBts.objFrmSchema.setDbType(dbtype.db.valueOf(lPropertyReader.getProperty("SRC.DB.TYPE") ) );
            
            objDBts.objToSchema = new ischema();
            objDBts.objToSchema.setName(sToSchema);
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
        genqvsp11(itab,"PHASE1");
    }
  }
  
  public String memTab(itable itab, boolean  brcnumber )
  {
    String strFKSQL = "";  
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
         strFKSQL = strFKSQL.replace("\"RECNO () AS SOURCERECORDNUMBER\"","RECNo () as SourceRecordNumber" );
            
        String  sContent =   "\n" 
            //       + String.format("///$tab %s \n",   itab.getName().replaceAll("_", " ")          )
                  // + String.format("SET vTableName = '%s';\n", itab.getName()+sphase       )
                  //  + "$(vTableName):\n"
                   + "LOAD "+ strFKSQL
                   + String.format("\nSQL SELECT * \n FROM %s.\"%s\" fetch first 10 rows only ;\n CALL DIM_AUDIT_SAVE;\n" , itab.OwnerName,  itab.getName()  )
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
                    + String.format("\n FROM   [$(vG.SourcePHS2)\\%sPHASE1.qvd]\n (qvd);\n  CALL StoreAndDrop_PHS2;\n", itab.getName())
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
                   // + String.format("//$tab %s \n", itab.getName().replaceAll("_", " "))
                   //+ String.format("SET vTableName = '%s';\n", itab.getName() + sphase)
                   // + "$(vTableName):\n"
                    + "LOAD " + strFKSQL
                    + String.format("\n FROM   [$(vG.SourcePHS3)\\%sPHASE2.qvd]\n (qvd);\n  CALL MAPPINGCONFDIM_SUB;\n", itab.getName())
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
          updCont = 
                "//--------------------------------------------------------------------------------\n"
                + String.format("///$tab %s \n",   sTablename.replaceAll("_", " ") ) // filename has the tablename
                + "LET vStartTime = Now(); \n"
                + String.format("SET v%s = '%s';\n",sTablename, sTablename + sphase)
                + String.format("Trace Started : $(v%s) at \t starttime: $(vStartTime); \n",sFilename ) 

                + String.format("$(v%s):\n", sTablename )
                + updCont + "\n"
                + "LET vEndTime = Now();\n"
                + String.format("Trace Time to load: $(v%s) is $(vDuration). \t starttime: $(vStartTime) \t endtime: $(vEndTime);\n" , sTablename) 
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
  /*
  --GEO--C--    Delete rows based on a criteria  add a paramter
  
  */  
  public void Delerows()  {
   String strsql = "";
   String strFrmSch = objDBts.objFrmSchema.getName();
   String strInsSql = "";
   String sStartTime = "";
   String WhereClause = "";
     for (itable itab : objDBts.objToSchema.gettables()) {
        if (itab.getName().equalsIgnoreCase("HOUSE_BILL"))    
        if (objDBts.isDbtable(objDBts.objFrmSchema.getName(), itab.getName())) { // db2 doens't like toLower

            // if it has clob/ blob then lets chnage the fetch size
            sStartTime = "Begin" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());
//            strsql = "Select " + itab.getName() + ".* from " + strFrmSch + "." + itab.getName() + " " + itab.getName();//+ " limit 10";
            // recursively get the whereclause


                WhereClause = getRecuriveFKs1(itab, objDBts.objFrmSchema.getName(), 1, null);// get this from from Schema Clause
                //WhereClause = WhereClause + "  LIMIT " + limitSize;


            System.out.println( "--------------------------------------------------------------------------------"    );
            System.out.println("table = " + itab.getName() + "\n " + strsql + " WhereClauseaa = " + WhereClause);
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
        //genqvsPhase3("PHASE3");
        
        } catch (Throwable ex) {
            Logger.getLogger(ipurge.class.getName()).log(Level.SEVERE, null, ex);
        }
    
    }
    
    public void delQvs(){
        delallFiles(lPropertyReader.getProperty("SQL.DDL.PATH"));
    }
    
    
    
    public idTab addTabs( String sPkTabColName // Pk of the table(child/topmost paremt)
            , String sSchemaName
            , String sTabName // Tab name
            , idTab pidTab // parent table id to which child tables PKs are added into
            , String sFKTableColname // Child tables Fk to Parent tables PK
            , int iCurrentDepth )
    {
        ResultSet   objrsrecQry; 
        PreparedStatement _selStatement;
        
        String[] Values = new String[2] ;
        String ssql ;
        boolean bhookedFk2pk= false;
        String sPrintHeader= "";
        int sPrintHeaderLen= 0;
        
        getPrepStatSQL(pidTab);// of the format (?,?,?,?):1,2,4,5,  not need to generate if the parent is same 
        
        if (SqlpreparedStat.contains(":")) { 
            Values = SqlpreparedStat.split(":");
            ssql =  String.format("Select %s as PK, %s as FK From %s.%s WHERE %s in (%s) "
                    , sPkTabColName , sFKTableColname
                    , objDBts.objToSchema.getName(),sTabName, sFKTableColname, Values[1]);
        }
        else 
            ssql =  String.format("Select %s as PK, %s as FK From %s.%s  where updt_timestmp > '2018-03-21'  "
                    , sPkTabColName, sPkTabColName // here we are pulling PK as 2 columns 
                    , objDBts.objToSchema.getName(),sTabName);
        
        
        // ids of a table has tabname {pk1,true,{childids},pk2,pk3}
        idTab lidTab = null;
        ids lids   ;
        boolean alreadyaddedTab = false;
        try {
            _selStatement = objDBts.conn1.prepareStatement(ssql);
       
            objrsrecQry = _selStatement.executeQuery(); // get the data from child table 
       
            if (objrsrecQry !=null){
                // check if the tables is already added why though b/c if a table is already added we need to add only if the already insctae is a 
                // highte depth level
     //           for ( idTab tab: idtabs){
     //               if (tab.Name.equalsIgnoreCase(sTabName)){
     //                   lidTab = tab;
     //                   alreadyaddedTab= true;
     //                   pidTab = lidTab;
     //                   break;
     //
     //               }
     //           }

                sPrintHeader = sPrintHeader +
                                String.format("\n|%40s | %40s|", sPkTabColName, sFKTableColname);
                sPrintHeaderLen = sPrintHeader.length();
                sPrintHeader = lSumBJCLogger.sTabPrint(sPrintHeader, sPrintHeaderLen, "-", "|");
                System.out.print(sPrintHeader);
            }
       while (objrsrecQry.next())
       {    // Set the header to print
           sPrintHeader =  String.format("\n|%40s | %40s|"
                            , objrsrecQry.getString("PK")
                            , objrsrecQry.getString("FK")
                            );
           System.out.print(sPrintHeader);
           
           lids = new ids();// create a local ids to 
           lids.ID = objrsrecQry.getString("PK");
           lids.deleteable = true;
           bhookedFk2pk=false;

           if (pidTab != null)// wee need to asscoiate the child rows 
               for(ids parentlids  : pidTab.Pks)
               {
                   if (parentlids.ID.equalsIgnoreCase(objrsrecQry.getString("FK")))
                   {//parentlids.Fks.add(lids);
                        for(idTab chld  :parentlids.Fks)  { 
                            if (chld.Name.equalsIgnoreCase(sTabName))
                                bhookedFk2pk= true;
                                chld.Pks.add(lids);    
                        }    
                        //if (parentlids.Fks.isEmpty()) // add the child table name 
                        if (bhookedFk2pk==false )
                        {   // 1st time we are adding a table name; P1,{Pk1{child1(THIS IS ADDED NOW){pk1,pk2:child2{pk1}}}, Pk2}
                            lidTab = new idTab();
                            lidTab.Name = sTabName;
                            lidTab.Pks.add(lids);
                            parentlids.Fks.add(lidTab);
                        }
                   }    

               }
           else {
               //if (alreadyaddedTab==false)
               if (lidTab==null)
                    lidTab = new idTab();
               lidTab.Name = sTabName;
               lidTab.Pks.add(lids);
           }
       } ;// wend 
       
    }

    catch (SQLException ex) {       lSumBJCLogger.WriteErrorStack("GetShcmea ", ex);           } 
    finally {
        objrsrecQry = null;

        _selStatement = null;
        if ((pidTab == null)) // 
        {
            if (!(lidTab == null)) {               pidTab = lidTab ;            
                idtabs.add(lidTab);
            }
            //pidTab  = lidTab ;
        } 
        //else lidTab = pidTab;
         sPrintHeader = sPrintHeader + lSumBJCLogger.printLine( sPrintHeaderLen, "-", "|");
        
        
        return pidTab;

        }    
        
    }

public String getPrepStatSQL(idTab pidTab){
    String sids = "";
    if (pidTab!= null){
        // check parent is same as previuos childs parent 
        if (!(pidTab.Name.equalsIgnoreCase(scurrParent))) {
            scurrParent =pidTab.Name;
            for (ids pk: pidTab.Pks) {
                SqlpreparedStat = SqlpreparedStat + "?,";
                sids = sids + "'" + pk.ID + "',";
            }
            if (!SqlpreparedStat.equalsIgnoreCase(""))
                SqlpreparedStat= "(" + SqlpreparedStat.substring(0, SqlpreparedStat.length()-1) + ")";
            if (!sids.equalsIgnoreCase("")){
                sids= sids.substring(0, sids.length()-1);
                SqlpreparedStat = SqlpreparedStat + ":" + sids;}
        }
    }        
    return SqlpreparedStat;
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
        StringBuilder sWhrCond = new StringBuilder("");
        String inclause = "";
        String FKTableName = "";// is the table table which refrences the primary/Unique  key on another table 
        String PKTableName = ""; // is the table name which is refrenced by a FK table name 
        String FKTableColname = "";
        String PKTableColname = "";
        String DeleteSql= "";
        
        int i = 0; 
        if (pidTab == null)
            pidTab  = addTabs( itab.getPKField().getName(),  objDBts.objToSchema.getName(),  itab.getName() , pidTab ,itab.getPKField().getName(), iRowCnt  );

        if (itab.dptables != null) 
        {
            System.out.println(
                String.format("%sTab = %s\t; PKTableName=%s; PKcol= %s;  FKTableName=%s FKCol= %s"
                        , (new String(new char[iRowCnt]).replace("\0", "\t"))
                        , itab.getName(),PKTableName,PKTableColname,   FKTableName, FKTableColname )
                );
            for (fkTable fktab : itab.dptables.gettables()) 
            {
                i++;
                FKTableName = fktab.FkColumn.CON_TABLE;
                PKTableName = fktab.PKColumn.CON_TABLE;
                FKTableColname =  fktab.FkColumn.field.getName();
                PKTableColname =  fktab.PKColumn.field.getName();
                inclause = "";
                System.out.println(
                //String.format("Tab = %s\t; PKTableName=%s; PKcol= %s;  FKTableName=%s FKCol= %s",itab.getName(),PKTableName,PKTableColname,   FKTableName, FKTableColname )
                String.format("%s%10d) Select %s From %s.%s where %s in ",  (new String(new char[iRowCnt]).replace("\0", "\t")),i
                 ,FKTableColname 
                        , objDBts.objToSchema.getName(),FKTableName 
                , FKTableColname)
                
                );
                // need to check if there are more than 1 PKS ie col1, col2 are pks
                getRecuriveFKs1(objDBts.objToSchema.gettable(FKTableName)
                                ,objDBts.objToSchema.getName(), iRowCnt+2 ,
                                    addTabs( objDBts.objToSchema.gettable(FKTableName).getPKField().getName()
                                    ,  objDBts.objToSchema.getName(),  FKTableName , pidTab, FKTableColname
                                    , iRowCnt)
                                );
              
                
                
            }    // end for  
            System.out.println(
                
                String.format("\n%s Select %s From %s.%s", (new String(new char[iRowCnt]).replace("\0", "\t"))
                 ,PKTableColname , objDBts.objToSchema.getName(),PKTableName )
                
                );
            // join the select and where clauses 
           // strJoin = strJoin + " " + sWhrCond.toString();
        }
        else 
        {
            DeleteSql = String.format("Select %s from %s.%s ", itab.getPKField().getName(), objDBts.objToSchema.getName() ,  itab.getName()) ;

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
        
        ilpurge.genQvs();
        ilpurge.Delerows();
                
        ilpurge = null;
       
       

    }

    
    


}



