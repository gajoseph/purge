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
public class contraintcolumn {
    
    public String CON_SCHEMA;
    public String CON_TABLE;
    public tfield field;
    public String CON_TYPE;
    public String CON_NAME;

    public contraintcolumn( tfield tt) {
        field = new tfield();
        field = tt;
        
    }
    
  protected void finalize() throws java.lang.Throwable {
    try {
       //System.out.println("Excuitng the finalized method of the Tfield ");
        
        field = null;
     }
    finally {
        super.finalize(); 
        
    }
        
    }
    
}
