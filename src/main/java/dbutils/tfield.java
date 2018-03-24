/*
 * tfield.java
 *
 * Created on July 19, 2005, 11:21 PM
 */

package dbutils;
import java.lang.*;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.util.*;
import dbutils.dbtype;

/**
 *
 * @author George
 */
public class tfield {
    public  int ID ;
    private String Name;
    private String Value;
    private String Type;
    public  int Length;
    public  int iDecimal_digits = 0; 
    public  dbtype.isnull Nullable;
    private boolean bPrimary= false ; 
    private String Comment="";
   
    
    
    // GEO--C-- DATABASE RESERVED Words
    public static final String[] REV_WORDS = new String[] {
        "DESC","SQL","DISTINCT","JOIN", "ORDER", "GROUP", "FROM", "SELECT", "WHERE"
            , "BY", "ASC", "DESC", "ALTER", "DROP", "OWNER", "DELETE", "UPDATE"};
    
    public static final String[] LRG_DATA_TYPES = new String[] {"BLOB","CLOB"};
    
    public String getComment() {
        return Comment;
    }

    public void setComment(String Comment) {
        this.Comment = Comment;
    }

    
    //-----------------------------------------------------------------
    public dbtype.isnull getNullable() {
        return Nullable;
    }


    public boolean isPrimary() {
        return bPrimary;
    }

    public void setPrimary(boolean bprimary) {
         this.bPrimary =  bprimary;
    }
    
    
    
    
   //------------------------------------------------------------------------- 
    public int getLength() {
        return Length;
    }

    public void setLength(int Length) {
        this.Length = Length;
    }

// this is custome 
    public void setNullable(int nullable) {
        
        if (nullable == DatabaseMetaData.columnNullable) {
                    this.Nullable = dbtype.isnull.NULL;
                } else {
                
                    this.Nullable = dbtype.isnull.NOT_NULL;
                }
    }
    
    
    /** Creates a new instance of tfield */
    public tfield() {
        this.ID     = 0;
        this.Name   = "";
        this.Type   = "";
        this.Value  = "";
        this.Length = 0;
        this.Nullable   = dbtype.isnull.NULL;
        this.bPrimary   = false;
        this.Comment    = "";
    }
    /**  get the name
     *  Added on 08/25/2005 
     * **/
    public String getType () {
      return this.Type;
    }
    /**  set the name **/
    public void setType (String nm) {
      if (nm!= null)
        this.Type = nm;
   }
    /**  get the name **/
  public String getName () {
    return this.Name;
  }
  /**  set the name **/
  public void setName (String nm) {
    this.Name = nm.toUpperCase();
 }
  
  /**  get the value **/
  public String getValue () {
    return this.Value;
  }
  /**  set the name **/
  public void setValue (String nm) {
    this.Value = nm;
    this.Length = nm.length();
  }
  public int getID () {
    return this.ID;
  }
  
  
  protected void finalize() throws java.lang.Throwable {
    try {
       //System.out.println("Excuitng the finalized method of the Tfield ");
     }
    finally {
        super.finalize(); 
    }
        
    }

public boolean hassqlkeywords(String Name ){

     Optional<String>  t= Arrays.asList(REV_WORDS).stream()
            .filter(a -> a.toString().equalsIgnoreCase(Name))
            .findFirst();
      if (t.isPresent())    
            return true;
        else return                   
             false;

}
  
  
public String GetDDL()
    {
        
     String a =(Value=="")? "":  "/*\t DEFAULT " +Value  + " */"  ; // setting the default 
     
     String comment =(Value=="")? "":  "/*\t" +  this.Comment  + "*/"  ;
     
// replace Sql Keywords
     String newName =(hassqlkeywords(Name)==true)? ('"' +   Name + '"') :  Name  ;
     
      if (this.Type=="CHARACTER" 
             || this.Type=="CHAR" 
             || this.Type=="VARCHAR"  
             || this.Type=="CLOB"  
         )
                return newName
                        + "\t" + Type 
                        + "("  + Length 
                        + ")" 
                        + "\t" +Nullable.toString().replace("_", " ") 
                        + a
                        + comment
                        + ",\n";
      else if (iDecimal_digits >0 )
                 return newName
                        + "\t" + Type 
                        + "("  + Length 
                        + "," 
                        + iDecimal_digits 
                        + ")" 
                        + "\t" +Nullable.toString().replace("_", " ") 
                        + a
                        + comment
                        + ",\n"; 
      else  
                return newName 
                        + "\t" + Type  
                        + "\t" + Nullable.toString().replace("_", " ") 
                        + a
                        + comment
                        + ",\n";
    }

public static void main(String[] args) {
  tfield t = new tfield();
}

}

  
  

