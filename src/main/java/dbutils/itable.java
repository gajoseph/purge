/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbutils;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import static dbutils.idrive.lSumBJCLogger;
import java.util.Arrays;

/**
 *
 * @author TGAJ2
 */
public class itable extends tfield implements NewInterface {

    protected List<tfield> tabFields;
     //    private List<tfield> idxfields;

    // --Commented out by Inspection (5/24/2018 9:33 AM):public int ID;

    public int getFieldcount() {
        return fieldcount;
    }

// --Commented out by Inspection START (5/24/2018 9:33 AM):
//    public void setFieldcount(int fieldcount) {
//        this.fieldcount = fieldcount;
//    }
// --Commented out by Inspection STOP (5/24/2018 9:33 AM)
    protected int fieldcount;

    private dbtype.db _dbType;

    public String TableDDL;

    public boolean isvalid = true;
    public String Grants = "";
    public String PKDDL = "";
    public String PK_NAME = "";
    public String OwnerName = "";
    public String TableCommDDL = "";
    public boolean hascreatedDropDDL = false;
    // Storing the join condition 
    public String sJoincondition = "";
    public boolean  hasBCLOB= false; // this is to determine the fetch size ;

 //  HashMap<String,iindex> indexes=new HashMap<String,iindex>(); 
    protected indexes Indexes;
    protected fkTables fktables;
    protected fkTables dptables;// dependeat / child tables 

    public tfield getPKField() {

        Optional<tfield> t
                = tabFields.stream().filter(u -> u.isPrimary()).findFirst();

        if (t.isPresent()) {
            return t.get();
        } else {
            tfield t1 = new tfield();
            t1.setName("UNKNOW");
            return t1;
        }

    }
    
    public List<tfield> getPKFields() 
    {

        List<tfield> t
                = tabFields.stream().filter(u -> u.isPrimary())
                .collect(Collectors.toList());

        if (t.isEmpty()) {
            return   Arrays.asList(new tfield());
        } else {
            return t;
        }

    }
    
    
    

    public String getTableDDL() {
        return TableDDL;
    }

    @Override
    public void setTableDDL(String TableDDL) {
        this.TableDDL = TableDDL;
    }

    public dbtype.db getDbType() {
        return _dbType;
    }

    public void setDbType(dbtype.db _dbType) {
        this._dbType = _dbType;
    }

    public itable() {
        this.fieldcount = 0;
        //_Fields = new Hashtable();
        tabFields = new ArrayList<tfield>();

        //        Indexes    = new indexes();
        TableDDL = "";
        this.setComment("");
        Grants = "";
        this.setName("");
        hascreatedDropDDL= false;
        hasBCLOB = false;

    }

    public String getGrants() {
        return Grants;
    }

    public void setGrants(String Grants) {
        this.Grants = Grants;
    }

    protected tfield DelField(String fldName) {
        Optional<tfield> t
                = tabFields.stream().filter(u -> u.getName().equalsIgnoreCase(fldName)).findFirst();

        if (t.isPresent()) {
            boolean remove = tabFields.remove(t);
            return t.get();
        } else {
            return new tfield();
        }
    }


public void setIfhasLargData( String Type)  
{
    int itype = Integer.parseInt(Type);
    if (!hasBCLOB) 
        if ( (JDBCType.BINARY.ordinal()==itype)           ||  (JDBCType.BLOB.ordinal()==itype)  ||  (JDBCType.CLOB.ordinal()==itype) 
            ||  (JDBCType.LONGVARBINARY.ordinal()==itype) ||  (JDBCType.NCLOB.ordinal()==itype) ||  (JDBCType.VARBINARY.ordinal()==itype) 

           )
        {
            hasBCLOB= true;
        }


    
}    
    
    
    protected tfield AddField(String name, String type, String value, int indx, int Nullable, int Length,
            String Comm, int piDecimal_digits
            ) {
        tfield lfield = new tfield();
        String soutstring= "";

        try {
            String indx1 = "" + indx;
       //System.out.println("Created tfield obj");
            //lfield.Length   = value.length();
            if (value != null) {
                lfield.setValue(value);
            }
        //System.out.println("Set the value ");

            lfield.setName(name.toUpperCase());
            
            if (isInt(type))
                lfield.setType(getDestDbType(Integer.parseInt(type)));// map the types to dest type
             else   
                lfield.setType(getDestDbType(type.toUpperCase()));// map the types to dest type
            
            
            if (this._dbType == dbtype.db.POSTGRES) 
            {
                if ((Length > 100000) && lfield.getType().equalsIgnoreCase("VARCHAR")) {
                    lfield.Length = 0;
                    lfield.setType(dbtype.PosJDBC.TEXT.toString());
                } else {
                    lfield.Length = Length;
                }
//--2017-06-10 mapping boolena to boolan and not to BIT 
                if ((Length ==1 ) && lfield.getType().equalsIgnoreCase("BIT")) 
                {   lfield.Length = 0;
                    lfield.setType(dbtype.PosJDBC.BOOLEAN.toString());
                    
                }
                   
            }
            else if  (this._dbType == dbtype.db.MYSQL) 
            {
                if ((Length >  65535) &&  
                        (JDBCType.valueOf(Integer.parseInt(type)) ==JDBCType.LONGNVARCHAR  
                        ||JDBCType.valueOf(Integer.parseInt(type)) ==JDBCType.CLOB
                        ||JDBCType.valueOf(Integer.parseInt(type)) ==JDBCType.NCHAR
                        ||JDBCType.valueOf(Integer.parseInt(type)) ==JDBCType.NCLOB
                        ||JDBCType.valueOf(Integer.parseInt(type)) ==JDBCType.NVARCHAR
                        ||JDBCType.valueOf(Integer.parseInt(type)) ==JDBCType.LONGVARCHAR
                        ||JDBCType.valueOf(Integer.parseInt(type)) ==JDBCType.VARCHAR
                        ) 
                        
                        ) {
                    lfield.Length = Length; // mysql needs length for clob
                    lfield.setType(dbtype.mySqlJDBC.LONGTEXT.toString());
                } 
                else 
                    lfield.Length = Length;
                
                if ((Length ==1 ) && lfield.getType().equalsIgnoreCase("BIT")) 
                {   lfield.Length = 0;
                    lfield.setType(dbtype.mySqlJDBC.BOOLEAN.toString());
                    
                }
               
            
            }   
            else  if (this._dbType == dbtype.db.DB2) 
            {
                if ((Length > 100000) && 
                        (JDBCType.valueOf(Integer.parseInt(type)) ==JDBCType.LONGVARCHAR
                        ||JDBCType.valueOf(Integer.parseInt(type)) ==JDBCType.VARCHAR 
                        ||JDBCType.valueOf(Integer.parseInt(type)) ==JDBCType.CHAR 
                        )
                    )    
                {
                    lfield.Length = 0;
                    lfield.setType(dbtype.PosJDBC.TEXT.toString());
                } else {
                    lfield.Length = Length;
                }
//--2017-06-10 mapping boolena to boolan and not to BIT 
                if ((Length ==1 ) && lfield.getType().equalsIgnoreCase("BIT")) 
                {   lfield.Length = 0;
                    lfield.setType(dbtype.PosJDBC.BOOLEAN.toString());
                    
                }
                   
            }
            
            if (JDBCType.valueOf(Integer.parseInt(type)) == JDBCType.NUMERIC)
            {
                lfield.Length = Length;
                lfield.iDecimal_digits = piDecimal_digits;
                
            }
            
            
            
            lfield.setNullable(Nullable);
            if (Comm != null)
                lfield.setComment(Comm);
            //System.out.println("set the name ");
            lfield.ID = this.fieldcount;
            tabFields.add(lfield);

            //_Fields.put(indx1,lfield);
            fieldcount++;
            this.TableDDL = this.TableDDL + lfield.GetDDL();
            soutstring = soutstring + String.format("\n|(%s) %20s | %10s | %20s | %20s | %100s|"
                    , indx, name, type, value, lfield.getType() , lfield.GetDDL() );
            
            
            this.TableCommDDL = this.TableCommDDL
                    + "\n" + " ";
            if (   lfield.getType().equalsIgnoreCase("OTHER")                  ) 
            {
                this.isvalid = false;
            }
        } catch (Exception e) {
            System.out.println("  ADD Fields error " + e.getMessage());
        } finally {
//            lSumBJCLogger.WriteLog(soutstring);
            return lfield;
        }
    }

    protected void AddPKField(tfield fld) {
        tabFields.add(fld);
        fieldcount++;
    }

    
    
static boolean isInt(String s)
{
 try
  { int i = Integer.parseInt(s); return true; }

 catch(NumberFormatException er)
  { return false; }
}
    
    
// here setting the correct Db types using the 
    public String getDestDbType(String Type) {
        String getDestDbType = "NOFOUND";
        try 
        {
            if (_dbType == dbtype.db.POSTGRES) {
                //System.out.println(JDBCType.valueOf(Integer.parseInt(Type)) + "");
                
                
                System.out.println(JDBCType.valueOf(Type) + "");
                if (JDBCType.valueOf(Type).getName() != "OTHER") {
                    getDestDbType = //dbtype.PosJDBC.get(Type).toString().replace("_", " "); //old 
                            dbtype.PosJDBC.valueOf(Type).name().replace("_", " ");
                }
                else 
                    getDestDbType = dbtype.PosJDBC.get(Type).name().replace("_", " ");
            } 
            else if (_dbType == dbtype.db.MYSQL) 
            {
                //System.out.println(JDBCType.valueOf(Integer.parseInt(Type)) + "");
                //if (JDBCType.valueOf(Integer.parseInt(Type)).getName() != "OTHER") {
                System.out.println(JDBCType.valueOf(Type) + "");
                if (JDBCType.valueOf(Type).getName() != "OTHER") {
                
                    getDestDbType = //dbtype.mySqlJDBC.get(Type).toString().replace("_", " ");
                            dbtype.PosJDBC.get(Type).name().replace("_", " ");
                }
                else 
                    getDestDbType = dbtype.PosJDBC.get(Type).name().replace("_", " ");
            }
           
            else 
            {
                getDestDbType = Type;
                
            }
        } catch (Exception ex) 
        {
            lSumBJCLogger.WriteErrorStack("getDest", ex);
        }
        return getDestDbType;
    }
//-------------------------------------------------------------------------------------------------------------------------------------    
    // heart of the mapping get the JDBC type using getShort 
    // getShort("DATA_TYPE") and using the destion db type constant find the corresponding type; some time 
     public String getDestDbType(int Type) {
        String getDestDbType = "NOFOUND";
        try 
        {
            if (_dbType == dbtype.db.POSTGRES) {
                //System.out.println(JDBCType.valueOf(Integer.parseInt(Type)) + "");
                
                
//                System.out.println(JDBCType.valueOf(Type) + "");
                if (JDBCType.valueOf(Type).getName() != "OTHER") {
                    
                    getDestDbType = //dbtype.PosJDBC.get(Type).toString().replace("_", " "); //old 
                            dbtype.PosJDBC.get(""+Type).name().toString().replace("_", " ");
                }
                else 
                    dbtype.PosJDBC.get(""+Type).name().toString().replace("_", " ");
            } 
            else if (_dbType == dbtype.db.MYSQL) 
            {
                //System.out.println(JDBCType.valueOf(Integer.parseInt(Type)) + "");
                //if (JDBCType.valueOf(Integer.parseInt(Type)).getName() != "OTHER") {
//                System.out.println(JDBCType.valueOf(Type) + "");
                if (JDBCType.valueOf(Type).getName() != "OTHER") {
                
                    getDestDbType = //dbtype.mySqlJDBC.get(Type).toString().replace("_", " ");
                            dbtype.mySqlJDBC.get(""+Type).name().toString().replace("_", " ");
                }
                else 
                    getDestDbType = dbtype.mySqlJDBC.get(""+Type).name().toString().replace("_", " ");
            }
            else if (_dbType == dbtype.db.DB2) 
            {
//                System.out.println(JDBCType.valueOf(Type) + "");
                if (JDBCType.valueOf(Type).getName() != "OTHER") {
                    
                    getDestDbType = //dbtype.mySqlJDBC.get(Type).toString().replace("_", " ");
                    dbtype.db2JDBC.get(""+Type).name().toString().replace("_", " ");
                }
                else 
                    getDestDbType = dbtype.db2JDBC.get(""+Type).name().toString().replace("_", " ");
            }
            else 
            {
                getDestDbType = "UNKNN";
            }
        } catch (Exception ex) 
        {
            lSumBJCLogger.WriteErrorStack("getDest", ex);
        }
        return getDestDbType;
    }
    
    
    
    protected tfield FieldByName(String FldName) {
        /* return the field by name 
         */
        try {
            Optional<tfield> t
                    = tabFields.stream().filter(u -> u.getName().equalsIgnoreCase(FldName)).findFirst();
            if (t.isPresent()) {
                return t.get();
            } else {
                return new tfield();
            }

        } catch (Exception e) {

            throw new RuntimeException("unexpected invocation exception: " + e.getMessage());

        } catch (Error e) {
            throw new Error("unexpected Error occured : " + e.getMessage());
        } finally {////System.out.println("End of Funciton FieldBYname ");
        }
    } 

    protected List<tfield> getfields() {
        return this.tabFields;
    }
    
    
    public String getInsStatCommaSep( String sSchemaName)
    {
        String strInsSql = "";
        String straparams= "";
        
        String newName ="";
       straparams = " Values(";
        for (tfield tfld : getfields())
        {
            
            newName= (hassqlkeywords(tfld.getName()))? ('"' +   tfld.getName() + '"') :  tfld.getName() ;
            //strInsSql = strInsSql + tfld.getName() + ",";
            strInsSql = strInsSql + newName + ",";
            straparams = straparams + "?,";
        
        }
        strInsSql = "Insert into " + sSchemaName  + "."+ getName() + " (" + strInsSql.substring(0, strInsSql.length() - 1);        
        strInsSql = strInsSql + ")" + straparams.substring(0, straparams.length() - 1) + ")"; 
       
        lSumBJCLogger.WriteLog(strInsSql);
        return strInsSql;
        
        
    }
    
    // GetCyher merge Cmd 
    public String getMergeCypr( String sSchemaName)
    {
        String strMergeCypr = "";
        String straparams= "";
        
        strMergeCypr  = "Merge (p:" + this.getName() + " {";
        for (tfield tfld : getfields())
        {
            strMergeCypr = strMergeCypr + tfld.getName().trim() + ": {" + tfld.getName().trim() + "},";
            
        }
      
        strMergeCypr =   strMergeCypr.substring(0, strMergeCypr.length() - 1) + "})"; 
       
        lSumBJCLogger.WriteLog(strMergeCypr);
        return strMergeCypr;
        
        
    }
    
    
    
    public String getWhrClause4limit( String sSchemaName)
    {
    String strWhereClause = "";
    itable itab = this;// get the cuurent table 
    
    if (itab.fktables != null )// if there are FKS loop thru them recursivly 
    {
        // call a function 
        
    }
    
   return strWhereClause;
        
        
        
    }
            
    //   get the field collection of PKS 
    public List<String> getStrPkNameList()
    {
     List<String> FKSQL=
    
        tabFields.stream()
                .filter(e -> e.isPrimary())
                .map(e ->  e.getName()  )
                .collect(  Collectors.toList()   );
     
     return FKSQL;
    }
    
    public boolean isprrimaryKey( String sfldname)
    {
     List<String> t=
    
        tabFields.stream()
                .filter(e ->  e.getName().equalsIgnoreCase(sfldname) && e.isPrimary())
                .map(e ->  e.getName()  )
                .collect(  Collectors.toList()   );
        return (!t.isEmpty());
     
    }


    
    

    protected void finalize() throws Throwable {
        tfield l;
        String indx1;
        try 
        {
            comfun.rmFldFromArrylist(tabFields);
            
            if (Indexes != null) 
            {
                Indexes.finalize();//
            }
            if (fktables != null) 
            {
                fktables.finalize();
            }
            if (dptables != null) 
            {
                dptables.finalize();
            }
            
        } 
        catch (Exception e) 
        {
            lSumBJCLogger.WriteErrorStack("Finalize Exception", e);

        } catch (Error e) 
        {
            lSumBJCLogger.WriteErrorStack("Finalize Error ", e);

        } finally 
        {
            Indexes = null;
            fktables = null;
            dptables= null;
            super.finalize();
        }

    }

}
