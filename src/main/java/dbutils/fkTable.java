/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbutils;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author TGAJ2
 */
public class fkTable extends tfield {
    public contraintcolumn FkColumn;
    public contraintcolumn PKColumn;
    public boolean hasIssues ;
    public String  shasIssues ;
    public boolean hasDups ;
    public dbtype.reference_option Update_rule;
    public dbtype.reference_option Delete_rule;
    public String deferrability ;
    
public fkTable()
{
    super(); 
}      

protected  void AddFKField(tfield tt, String tabName, String  sSchema, String FK_NAME)   {
   FkColumn = new contraintcolumn(tt);
   FkColumn.CON_TYPE="FK";
   FkColumn.CON_SCHEMA = sSchema.toUpperCase();
   FkColumn.CON_TABLE = tabName.toUpperCase();
   FkColumn.CON_NAME = FK_NAME.toUpperCase();
   this.setName(tabName.toUpperCase());
   
    FkColumn.field = tt;   
    
    hasIssues= false;
    hasDups=false ;
    
}

protected  void AddPKField(tfield tt, String tabName, String  sSchema, String PK_NAME)   {
   PKColumn = new contraintcolumn(tt);
   PKColumn.CON_TYPE="PK";
   PKColumn.CON_SCHEMA = sSchema.toUpperCase();
   PKColumn.CON_TABLE = tabName.toUpperCase();
   PKColumn.field = tt;        
   PKColumn.CON_NAME = PK_NAME.toUpperCase();
    
}

// this generated the index ddl 

public String getReference_optionString(dbtype.reference_option Delete_rule, String Action )
{
    String crap = " ";
    if (!Delete_rule.equals(""))
        if (Delete_rule==dbtype.reference_option.importedKeyCascade)
            crap = " ON " + Action + " CASCADE ";
        else if (Delete_rule==dbtype.reference_option.importedKeyNoAction)             
            crap = " ON " + Action +" NO ACTION  ";
        else if (Delete_rule==dbtype.reference_option.importedKeyRestrict)             
            crap = " ON " + Action + " RESTRICT ";
        else if (Delete_rule==dbtype.reference_option.importedKeySetDefault)             
            crap = " ON " + Action + " SET DEFAULT ";        
        else if (Delete_rule==dbtype.reference_option.importedKeySetNull)             
            crap = " ON " + Action + " SET NULL";  
        
    return  crap ;

}

public String GetDDL(String s2Schema){
    //String sidxCrte= "Alter table ";
    StringBuilder sidxCrte = new StringBuilder();
    String scols= "";

    sidxCrte.append("Alter table ")
            .append( s2Schema).append(".")
            .append( FkColumn.CON_TABLE).append("\t ")
            .append( "ADD CONSTRAINT ").append( FkColumn.CON_NAME)
            .append(  "\t" )
            .append(  FkColumn.CON_TYPE.replace("FK", "FOREIGN KEY("))
            .append(  " " ).append(  FkColumn.field.getName())
            .append( ") ")
            .append(  " References ")
            //''+ PKColumn.CON_SCHEMA + "."
// we are creating the table also onto the same schema             
            .append(  s2Schema).append(  ".")
            .append(  PKColumn.CON_TABLE )
            .append( "(" )
            .append(  PKColumn.field.getName())
            .append( ") " )
            .append(getReference_optionString(Delete_rule, " DELETE "))
            .append("\t")
            .append(getReference_optionString(Delete_rule, " UPDATE"))
            .append(  " ;");
//System.out.println(FkColumn.toString() + "  skdkdk");
    
    if (hasIssues)
      sidxCrte.insert(0, " ----" + shasIssues 
              + "\n"  
              + "\n  --- " ) ; // this will comment ou the mismacthed FK PK 
    
    return sidxCrte.toString();
    }


public String GetConstraint_name()
{
    String Con_name = "";
    if (FkColumn.CON_NAME =="")
        Con_name = FkColumn.CON_TABLE + "_ref_" + PKColumn.CON_TABLE ;
    else 
        Con_name = FkColumn.CON_NAME;    

    return Con_name;
}

protected void finalize() throws Throwable {
    
    try {
       
       this.FkColumn= null;
       this.PKColumn= null;
       super.finalize(); 
       
    }
       catch (Exception e ){
       	}
       catch (Error e ){
       	}

    finally { 
    	super.finalize();     }
    }


}
