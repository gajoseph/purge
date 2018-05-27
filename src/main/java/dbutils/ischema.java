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
public class ischema  extends tfield{
    
// Storing in lust b/c hash table won't store in the order it was added .
    private List<itable> tabList;// = new ArrayList<String>();
             
    private int tabcount=1;
    // Store the CReate statement DDL
    private String _CreateTabStatement= "";

    
    private dbtype.db _dbType;
    
    public void setDbType(dbtype.db _dbType) {
        this._dbType = _dbType;
    }    public dbtype.db getDbType() {
        return _dbType;
    }
    

//    HashMap<String,iindex> indexes=new HashMap<String,iindex>(); 

    //----------------------------------------------------------------------------
    public ischema() {
        this.tabcount   = 0;
        tabList         = new ArrayList<itable>();
    }

    public ischema(String sSchemaName)
    {
        super();
        this.setName(sSchemaName);
        this.tabcount   = 0;
        tabList         = new ArrayList<itable>();

    }

    public String getCreateTabStatement() {
        return _CreateTabStatement;
    }

    
    public void setCreateTabStatement(String _CreateTabStatement) {
        this._CreateTabStatement = _CreateTabStatement;
    }
    
    public boolean isTableAdded(String TabName){
    return    false; 
    }
public itable TableByName( String TabName ){
  Optional<itable>  t = tabList.stream()
                          .filter(u -> u.getName().equalsIgnoreCase(TabName)
                                  )
                                  .findFirst();
    return t.orElseGet(itable::new);
}
    
    public itable AddTable(String TabName, String TabType, itable itab) {
        //tabList.add( itab);
        return AddTable(TabName, TabType ,itab , 1 );
//        this.AddField(TabName , TabType ,String value , int indx, int Nullable, int Length )
  }
     
  //----------------------------------------------------------------------------  
    
   private itable AddTable(String Tabname , String Tabtype ,itable itab , int indx )
    { 	
    	String indx1    = ""+indx;
        itable  ltable  =itab;
//Code starts 
        ltable.setName(Tabname.toUpperCase());
        ltable.setType(Tabtype);
        
        ltable.ID       = this.tabcount;
//        _tabByName.put(Tabname.toUpperCase(),ltable);
        tabList.add(itab);
        tabcount++;
        return ltable;
    }

    public List<itable> gettables(){
        return this.tabList;
    }
     
    
    public itable gettable( String tabName ){
    
       Optional<itable>  t= tabList.stream()
                        .filter(u -> u.getName().equalsIgnoreCase(tabName))
                            .findFirst();
        return t.orElseGet(itable::new);
       
        

    }
    
    
    
    
            
    
    protected void finalize() throws Throwable {
    	//lets list
//        comfun.listTabDetails(this.tabList);
        
       comfun.hasTabRemovelst(this.tabList);
       this.tabList.clear();
       this.tabList = null;
        super.finalize();     
    }
     protected void print() {
    	//comfun.print(this._tabByName, tabcount);
        
    }
    
}
