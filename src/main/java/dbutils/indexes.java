/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbutils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 *
 * @author TGAJ2
 */
public class indexes {
    private int fieldcount=0;
    private List<iindex> idxlist;
    
    
    public indexes() {
        super();
        fieldcount= 0;
        idxlist         = new ArrayList<iindex>();
        
    }
    
 /*   public void AddIndex(iindex index )
    { 	
    try {
        this._FieldByName.put(index.getName(),index);
        fieldcount++;
        }
        catch (Exception e){
            System.out.println("  ADD Fields error " + e.getMessage());
        }
        finally {
            
        }
    }
    
    public iindex IndexByName(String IdxName ) throws  Throwable    {
    // return the field by name 
     
       	try {
            return (iindex) _FieldByName.get(IdxName.toUpperCase());
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
    
    public boolean IndexExists(String IdxName){
        boolean IndexExists = false ;
        try {
            IndexExists= _FieldByName.containsKey(IdxName);
        }
        catch (Exception e ){
                System.out.println("IndexEcists " + e.getMessage());
    		}
    	catch (Error e ){
                System.out.println("IndexEcists " + e.getMessage());
   		}
    	finally {////System.out.println("End of Funciton FieldBYname ");
          return IndexExists;
    	}
        
    }
    
    */
    
    
    public void AddIndex(iindex index )
    { 	
    try {
        this.idxlist.add(index);
        fieldcount++;
        }
        catch (Exception e){
            System.out.println("  ADD Fields error " + e.getMessage());
        }
        finally {
            
        }
    }
    
      public iindex getIndex( String idxName ){
    
        Optional<iindex>  t = idxlist.stream()
                                .filter(u -> u.getName().equalsIgnoreCase(idxName)
                                        )
                                        .findFirst();
          return t.orElseGet(iindex::new);
    }

     
      
    public boolean IndexExists(String idxName){
    
        Optional<iindex>  t = idxlist.stream()
                                .filter(u -> u.getName().equalsIgnoreCase(idxName)
                                        )
                                        .findFirst();
        return t.isPresent();
    
    
    }
      
      
    
    public List<iindex> getindexes(){
    
        return this.idxlist;

    }

    
    
    
    
    
    
    
    
    
    protected void finalize() throws Throwable {
    
    try {
       
       for (int i=0; i<this.getClass().getFields().length-1; i++ )
        System.out.println(this.getClass().getFields()[i].getType().getSimpleName());
       
       
       comfun.hasIdxRemovelst(this.idxlist);
       this.idxlist.clear();
       this.idxlist = null;
    //   super.finalize();
       
       
    }
       catch (Exception e ){
       	}
       catch (Error e ){
       	}

    finally { 
    	super.finalize();     }
    }
    
   
    
}
