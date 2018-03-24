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
public class ids {
    public  String ID;
    public boolean deleteable; 
    public  List<idTab> Fks;

    public ids() {
        super();
        ID="";
        deleteable= false;
        Fks = new ArrayList<idTab>();
    }

    
 protected void finalize() throws Throwable {
    try {
       comfun.hasTabIdsRemovelst((this.Fks));
       this.Fks.clear();
       this.Fks = null;
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
