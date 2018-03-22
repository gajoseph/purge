/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbutils;



/**
 *
 * @author TGAJ2
 */
public class idxFld {
public String ORDINAL_POSITION;
public String ASC_OR_DESC = "" ;
public tfield  indxField;
public boolean hasIssues ;
public String  shasIssues ;


    public String getASC_OR_DESC() {
        return ASC_OR_DESC;
    }

    public void setASC_OR_DESC(String ASC_OR_DESC) {
        if (ASC_OR_DESC!=null)
        {   if (ASC_OR_DESC.equalsIgnoreCase("A"))
                this.ASC_OR_DESC = "ASC";
            else if (ASC_OR_DESC.equalsIgnoreCase("D"))
                    this.ASC_OR_DESC = "DESC";
        }
    }
    

    public idxFld(tfield indxField) {
        this.indxField = indxField;
        
    }
    
    
    
    protected void finalize() throws java.lang.Throwable {
    try {
       //System.out.println("Excuitng the finalized method of the Tfield ");
        
        indxField = null;
     }
    finally {
        super.finalize(); 
        
    }
        
    }
}
