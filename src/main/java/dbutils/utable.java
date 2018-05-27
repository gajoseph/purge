/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbutils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import static dbutils.idrive.lSumBJCLogger;

/**
 *
 * @author tgaj2
 * to deploy the reselase in stage refreshed the stage database with production data; appied the db shcnages and ran the script to load the agent into stage; 
 * sent not to craig; today once craig give a thumps up will run the script today sometime today/ tommorrow before the rerlease 
 */
public class utable{
    
     public List<fkTable> fkTables;// = new ArrayList<String>();
     private int fieldcount= 0;
     
   
    public fkTable  Add(fkTable itab) {
        fkTables.add(itab);
        fieldcount++;
        return itab;
  }
    
     public fkTable  get( String name){
     
      Optional<fkTable>  t= fkTables.stream()
                        .filter(u -> u.getName().equalsIgnoreCase(name))
                            .findFirst();
         return t.orElseGet(fkTable::new);
    
     }
    
    public  void utable(){
                fkTables = new ArrayList<fkTable>();

    
    }
    
     protected synchronized void finalize() throws java.lang.Throwable {
        try{
              Iterator itr = fkTables.iterator();
        while (itr.hasNext())
        {
            //itable  t =(itable)itr.next();
            lSumBJCLogger.WriteLog( " removing list " 
                    + " Count " +fkTables.size()
                    );
            //t = null;
            itr.remove();
        }
 
        
        
        } 
         
               catch (Exception e){
//        	////System.out.println("Exception " + e.getMessage() + " :" + e.getCause() );
        	throw new RuntimeException("unexpected invocation exception: " +e.getMessage());
        	}
        catch (Error e){
//        	////System.out.println("Exception " + e.getMessage() + " :" + e.getCause() );
        	throw new Error("unexpected Error occured : " +e.getMessage());
        	}
        finally {
            
            fkTables.clear();
            fkTables= null;
            super.finalize(); 
        }
   
         
     }
    
}
