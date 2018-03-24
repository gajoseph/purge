/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbutils;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author tgaj2
 */
public class idTab {
    public  String Name;// table name 
    public  List<ids> Pks;

    
    
public idTab() {
        super();
        Name="";
        
        Pks = new ArrayList<ids>();
    }

    
 protected void finalize() throws Throwable {
    try {
       comfun.hasIdsRemovelst(this.Pks);
       this.Pks.clear();
       this.Pks = null;
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
