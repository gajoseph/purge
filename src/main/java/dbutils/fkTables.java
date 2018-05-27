/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbutils;

import java.util.*;

/**
 *
 * @author TGAJ2
 */
public class fkTables {
    
    private int fieldcount=0;
    private List<fkTable> fkTabList;
    
    public fkTables() {
        super();
        fieldcount=0;
        fkTabList         = new ArrayList<fkTable>();
    }

    
    
    public void AddFkTable(fkTable fktab )
    { 	
        fkTable duup;
    try {
    //    this._FieldByName.put(index.getName(),index);
        //check if another fields is refrencing to the same FK table 
        duup = FndMutipleKStoSamePKs(fktab );
        if (duup.getName()!= ""){
            duup.hasDups= true;
            fktab.hasDups = true;
        }    
        
            
                
        
        
        fieldcount++;
        fkTabList.add(fktab);
        }
        catch (Exception e){
            System.out.println("  ADD Fields error " + e.getMessage());
        }
        finally {
            
        }
    }
    
    
    /*
    public fkTable fkTableByName(String IdxName ) throws  Throwable    {
    //-- return the field by name      
       	try {
            return (fkTable) _FieldByName.get(IdxName.toUpperCase());
    	}
    	catch (Exception e ){
    		 throw new RuntimeException("unexpected invocation exception: " +e.getMessage());
    		}
    	catch (Error e ){
   		 throw new Error("unexpected Error occured : " +e.getMessage());
   		}
    	finally {////System.out.println("End of Funciton FieldBYname ");
    	}
    } 
   
    */
    
    private  Optional<fkTable>  gettable( String tabName , String schemaName ){

        Optional<fkTable>  t= fkTabList.stream()
                .filter(u -> u.PKColumn.CON_TABLE.equalsIgnoreCase(tabName)
                        && u.PKColumn.CON_SCHEMA.equalsIgnoreCase(schemaName)
                )
                .findFirst();
        return t;

    }

    public  Map<fkTable, Boolean>  getFktable( String tabName , String schemaName ){

        Optional<fkTable> t = gettable(tabName, schemaName);
        Map<fkTable, Boolean> t1= new HashMap<>();

        if (t.isPresent())
            t1.put(t.get(), true );
        else
            t1.put(new fkTable(), false );


        return t1;

    }




    public fkTable FndMutipleKStoSamePKs( fkTable fktab ){
    
    /* Optional<fkTable>  t= fkTabList.stream()
                        .filter(u -> u.PKColumn.CON_TABLE.equalsIgnoreCase(fktab.PKColumn.CON_TABLE)  
                                    && u.PKColumn.CON_SCHEMA.equalsIgnoreCase(fktab.PKColumn.CON_SCHEMA)
                                )
                            .findFirst();
    */
        Optional<fkTable> t = gettable(fktab.PKColumn.CON_TABLE, fktab.PKColumn.CON_SCHEMA);
        return t.orElseGet(() -> new fkTable(fktab));
        /*
        * if (t.isPresent()) return t.get();
        else return new fkTable(fktab);*/

    }    
    
    
    public List<fkTable> gettables(){
    
        return this.fkTabList;

    }

     protected void finalize() throws Throwable {
    
    try {
       
       comfun.hasFkRemovelst(this.fkTabList);
       this.fkTabList.clear();
       this.fkTabList = null;
//       super.finalize();
       
    }
       catch (Exception e ){
           
       	}
       catch (Error e ){
       	}

    finally { 
    	super.finalize();     }
    }
}
