/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbutils;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import static dbutils.idrive.lSumBJCLogger;
/**
 *
 * @author TGAJ2
 */
public class comfun {

  public static  class FooException extends Exception {
  public FooException() { super(); }
  public FooException(String message) { super(message); }
  public FooException(String message, Throwable cause) { super(message, cause); }
  public FooException(Throwable cause) { super(cause); }
}

    public static boolean haskey(String searchName, Hashtable _tabByName){
      return _tabByName.containsKey(searchName.toUpperCase());

    }

    public static Object returnFromHashTab(String searchName, Hashtable _tabByName ) throws  Throwable    {
       Object obj=null;

        try {
                if (_tabByName.containsKey(searchName.toUpperCase()))
                    obj=_tabByName.get(searchName.toUpperCase());

    	}
    	catch (Exception e ){
    		 throw new RuntimeException("unexpected invocation exception: " +e.getMessage());
    		}
    	catch (Error e ){
   		 throw new Error("unexpected Error occured : " +e.getMessage());
   		}
    	finally {////System.out.println("End of Funciton FieldBYname ");
            return obj;

    	}
    }

    protected static void rmFldFromArrylist(List<tfield> fields ){
    Iterator itr = fields.iterator();
        while (itr.hasNext())
        {
            tfield  t =(tfield)itr.next();
            lSumBJCLogger.WriteLog( " removing list " + t.getName()
                    + " Count " +fields.size()
                    );
            itr.remove();
        }
        fields.clear();
        fields = null;  

    
    }
    
    
    protected static void rmidxFldFromArrylist(List<idxFld> fields ){
    Iterator itr = fields.iterator();
        while (itr.hasNext())
        {
            idxFld  t =(idxFld)itr.next();
            lSumBJCLogger.WriteLog( " removing list " + t.indxField.getName()
                    + " Count " +fields.size()
                    );
            itr.remove();
        }
        fields.clear();
        fields = null;  

    
    }
    
    protected static void listTabDetails(List<itable> tabList){
        //Iterator iterator = tabList.iterator();
        for(itable next  : tabList){
            //itable next = (itable)iterator.next();
            lSumBJCLogger.WriteLog( " list table : " + next.getName()
                    + " Count " +tabList.size() + " fld:" + next.getFieldcount()
                    );
            next.tabFields.forEach((key)->{
                lSumBJCLogger.WriteLog(" "+ ((tfield)key).getName() + " " );
           
            }
            );
        }
    }
    
    protected static  void hasTabRemovelst( List<itable> tabList)
    {
      Iterator itr = tabList.iterator();
        while (itr.hasNext())
        {
            itable  t =(itable)itr.next();
            lSumBJCLogger.WriteLog( " removing list " + t.getName() 
                    + " Count " +tabList.size()
                    );
          try {
              t.finalize();
          } catch (Throwable ex) {
                  lSumBJCLogger.WriteLog( " removing list " + t.getName() 
                    + " Count " +tabList.size()
                    );
          
          
          
          }
            t = null;
            itr.remove();
        }
    }     
    
     protected static  void hasFkRemovelst( List<fkTable> tabList)
    {
      Iterator itr = tabList.iterator();
        while (itr.hasNext())
        {
            fkTable  t =(fkTable)itr.next();
            lSumBJCLogger.WriteLog( " removing list " + t.getName()
                    + " Count " +tabList.size()
                    );
          try {
              t.finalize();
          } catch (Throwable ex) {
              Logger.getLogger(comfun.class.getName()).log(Level.SEVERE, null, ex);
          }
            t = null;
            itr.remove();
        }
    }     
    
    protected static  void hasIdxRemovelst( List<iindex> idxlist)
    {
      Iterator itr = idxlist.iterator();
        while (itr.hasNext())
        {
            iindex  t =(iindex)itr.next();
            lSumBJCLogger.WriteLog( " removing list " + t.getName()
                    + " Count " +idxlist.size()
                    );
          try {
              t.finalize();
          } catch (Throwable ex) {
              lSumBJCLogger.WriteErrorStack("errror ",(Exception)ex);
              
          }
            t = null;
            itr.remove();
        }
    }     
    
    
    
    protected static  void hasIdsRemovelst( List<ids> Fks)
    {
      Iterator itr = Fks.iterator();
        while (itr.hasNext())
        {
            ids  t =(ids)itr.next();
            lSumBJCLogger.WriteLog( " removing list " + t.ID
                    + " Count " +Fks.size()
                    );
          try {
              t.finalize();
          } catch (Throwable ex) {
              lSumBJCLogger.WriteErrorStack("errror ",(Exception)ex);
              
          }
            t = null;
            itr.remove();
        }
    }     
    
    protected static  void hasTabIdsRemovelst( List<idTab> Fks)
    {
      Iterator itr = Fks.iterator();
        while (itr.hasNext())
        {
            idTab  t =(idTab)itr.next();
            lSumBJCLogger.WriteLog( " removing list " + t.Name
                    + " Count " +Fks.size()
                    );
          try {
              t.finalize();
          } catch (Throwable ex) {
              lSumBJCLogger.WriteErrorStack("errror ",(Exception)ex);
              
          }
            t = null;
            itr.remove();
        }
    }     
     
    
    
     
    protected static void hasTabRemove(Hashtable _Hash, int hashcount) throws Throwable {
        tfield l ;
        String s = "";
        

       try {
    		for (Enumeration e = _Hash.keys(); e.hasMoreElements();){
                    s= e.nextElement().toString();
                    lSumBJCLogger.WriteLog( " removing " + s
                    + " Count " +_Hash.size()
                    );
                    _Hash.remove(s );

    		}
    		//tl.finalize();
            _Hash.clear();
            _Hash = null;
       }
    	catch (Exception e ){
    		//System.out.println("Exception occured on then  RecordSet Field[] method : " + e.getMessage());
    		 throw new RuntimeException("unexpected invocation exception: " +e.getMessage());
    		}
    	catch (Error e ){
    		//System.out.println("Error occured on then RecordSet Field[] method : " + e.getMessage());
   		 throw new Error("unexpected Error occured : " +e.getMessage());
   		}

       //_Fields.clear();
    finally {
    	System.out.println("Removed Hash tables items ");

        }

    }




    protected static void print(Hashtable _Hash, int hashcount) throws Throwable {
    String s = "";
    int i = 0;
       try {
    		for (Enumeration e = _Hash.keys(); e.hasMoreElements();){
                    s= e.nextElement().toString();
                    lSumBJCLogger.WriteLog( " In HAsh table by  index " + i +": key="+ s
                    + " Count " +_Hash.size()
                    );
    		i++;
                }
       }
    	catch (Exception e ){
    		//System.out.println("Exception occured on then  RecordSet Field[] method : " + e.getMessage());
    		 throw new RuntimeException("unexpected invocation exception: " +e.getMessage());
    		}
    	catch (Error e ){
    		//System.out.println("Error occured on then RecordSet Field[] method : " + e.getMessage());
   		 throw new Error("unexpected Error occured : " +e.getMessage());
   		}
    finally {
    	System.out.println("Removed Hash tables items ");
        }

    }
    
    protected static boolean isTiny_Big_Small_Int(int iColType)
    {
        return (iColType == java.sql.Types.TINYINT
                 || iColType == java.sql.Types.BIGINT
                 || iColType == java.sql.Types.INTEGER
                 || iColType == java.sql.Types.SMALLINT);
    
    }   
    
    protected static boolean isNumeric_Decimal_Float(int iColType)
    {
        return (iColType == java.sql.Types.NUMERIC
                 || iColType == java.sql.Types.DECIMAL
                 || iColType == java.sql.Types.FLOAT
                 );
    
    }        
     
    protected static boolean isAnyDateType(int iColType)
    {
    
            return (iColType == java.sql.Types.DATE
                        || iColType == java.sql.Types.TIME
                        || iColType == java.sql.Types.TIMESTAMP
                        || iColType == java.sql.Types.TIMESTAMP_WITH_TIMEZONE
                        || iColType == java.sql.Types.TIME_WITH_TIMEZONE
                    );
    
    }
    
    
    

}
