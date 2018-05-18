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
public class ids extends tfield  implements Cloneable  {
    public String FkID= "";// needs to be list to accombimideate more than i parent key ???
    public ids  parIds;// holds
    public    boolean isDeleteable() {
        return deleteable;
    }

    public boolean deleteable;

    public  List<String> getPkids() {
        return Pkids;
    }
    @Override
    public ids clone() throws CloneNotSupportedException {
        return (ids) super.clone();
    }

    //public  List<idTab> Fks;// not being used
    public List<String> Pkids; // stores the PKS
    //public ids  parentPKid; // stores the parentID Object so that if other childresn can't be deletd this we can a upt dae info
    /*
    Storing data like
        Parent HB table
            idTab : Name: HB;  { {fks:1; {1}}, {fks:2; {2}}; in this case here is no FK for HB as it is the top level
           Child tablee HBS has Fk to HB
            idTab : Name: HBs;  { {fks:1; PKS;{HBS11, HBS12}}, {fks:2; PKS:{HBS21, HBS22}};

           Child tablee HBSC has Fk to HBS
            idTab : Name: HBs;  { {fks:HBS11; PKS:{HBSC111, HBSC112}}, {fks:HBS12; PKS:{HBSC121}};
            each table will be store as a separate object


    */

    public ids( ) {
        super();
        deleteable= true;

        //Fks = new ArrayList<idTab>();
        Pkids = new ArrayList<String>();
        //parIds  = new ids();
    }

    public ids( tfield tt) {
        super();
        this.setName(tt.getName());
        this.setType(tt.getType());
        this.setValue(tt.getValue());

        deleteable= false;
        Pkids = new ArrayList<String>();
        //parIds  = new ids();
    }




    public boolean  addChildKeyforaParentFkid(  String ChldPkValue ){
            if (Pkids.contains(ChldPkValue))
                return true  ;
            else  {Pkids.add(ChldPkValue);
                    return false;
                    }
    }

    public boolean  checkPkvalueExits(String ChldPkValue){
        return Pkids.contains(ChldPkValue);

    }

    public boolean  canChdKeyforaParentFkDeleteAble(  String ChldPkValue ){
        if (checkPkvalueExits(ChldPkValue))
            return this.deleteable==true;
        else return true;


    }



    
 protected void finalize() throws Throwable {
    try {
       //comfun.hasTabIdsRemovelst((this.Fks));
        this.Pkids.clear();
        this.Pkids = null;
        super.finalize();
        parIds.finalize();
       
    }
       catch (Exception e ){
           
       	}
       catch (Error e ){
       	}

    finally { 
    	super.finalize();     }
    }    
    
    
    
}
