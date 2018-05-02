package dbutils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static dbutils.idrive.lSumBJCLogger;



public class fkids {
    private int fieldcount=0;
    private List<fkid> fkidList;

    public fkids() {
        super();
        fieldcount=0;
        fkidList         = new ArrayList<fkid>();
    }


    public boolean  addfkid(String fkParentValue , String pkChildValue, fkTable fk , boolean  isDeletAble, ids pids )
    { boolean alreadypresent =false ;
        fkid lfkid = hasFkidAlreadyAdded(fk );// checking if the PKColumn object already exists; return that; otherwise create a new one and return
        //--Geo--C--01/07/2018  Now add the FK(parent's PK/unique key ) and asscoiate the PK of the table
        alreadypresent  = lfkid.AddParentKeyWithChldKeys(fkParentValue, pkChildValue, isDeletAble, pids );
        return alreadypresent;
    }




    public boolean findPkCol(String PkColName ){
        Optional<fkid> t = fkidList.stream()
                .filter(u -> u.PKColumn.field.getName().equalsIgnoreCase(PkColName)
                            || u.PKColumn.CON_TABLE  =="null"
                )
                .findFirst();

        if (t.isPresent())
            return true;
        else
            return false ;
    }


    public boolean zAddFkId(fkid pfkid )
    {
        fkid duup;
        boolean val = false;
        try {
            //    this._FieldByName.put(index.getName(),index);
            //check if another fields is refrencing to the same FK table
            duup = hasFkidAlreadyAdded(pfkid );
            if (duup.getName()!= ""){
                val = true;
            }

        }
        catch (Exception e){
            System.out.println("  ADD Fields error " + e.getMessage());
        }
        finally {
                return val;
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

    public fkid getfkid( String fkvalue ){
    //
        return null;
    }



    public fkTable initfkTable(fkTable fk)
    {
        if (fk !=null)
            return fk;
        fk = new fkTable();
        fk.PKColumn = new contraintcolumn();
        fk.FkColumn = new contraintcolumn();
        fk.PKColumn.CON_TABLE = "null";
        fk.PKColumn.CON_SCHEMA = "null";// new to get this from caling function
        return fk;


    }

    public Optional<fkid>  checkFKAlreadyAdded(fkTable fk){

        Optional<fkid> t = fkidList.stream()
                .filter(u -> u.PKColumn.CON_TABLE.equalsIgnoreCase(fk.PKColumn.CON_TABLE)
                        && u.PKColumn.CON_SCHEMA.equalsIgnoreCase(fk.PKColumn.CON_SCHEMA)
                )
                .findFirst();

        return t;
    }
    // Based on the


    public fkid hasFkidAlreadyAdded( fkTable fk){
        fkid lfkid;
        fk = initfkTable(fk);

        Optional<fkid> t = checkFKAlreadyAdded(fk);
        if (t.isPresent())
            return t.get();
        else {
            lfkid = new fkid(fk);
            fkidList.add(lfkid);// addind to list of FKS objects; a table could have mutiple FKS from diff tables
            return lfkid;
        }

    }

    public boolean setLoadedFlagForFKCol( fkTable fk){
        // --Geo--C--04/08/2018 set the flag sloaded in fkid to true; this is to avoid going in a loop
        if (fk ==null) return false;
        Optional<fkid> t = fkidList.stream()
                .filter(u -> u.PKColumn.CON_TABLE.equalsIgnoreCase(fk.PKColumn.CON_TABLE)
                        && u.PKColumn.CON_SCHEMA.equalsIgnoreCase(fk.PKColumn.CON_SCHEMA)
                        && u.PKColumn.field.getName().equalsIgnoreCase(fk.PKColumn.field.getName())
                ).findFirst();

        if (t.isPresent()) {
            t.get().sloaded=true;
            return true;
        }
        else
            return false ;

    }




    public boolean findPkcol( fkTable fk){
        // --Geo--C--04/08/2018 set the flag sloaded in fkid to true; this is to avoid going in a loop

        Optional<fkid> t = fkidList.stream()
                .filter(u -> u.PKColumn.CON_TABLE.equalsIgnoreCase(fk.PKColumn.CON_TABLE)
                        && u.PKColumn.CON_SCHEMA.equalsIgnoreCase(fk.PKColumn.CON_SCHEMA)
                        && u.PKColumn.field.getName().equalsIgnoreCase(fk.PKColumn.field.getName())
                ).findFirst();

        if (t.isPresent()) {
            t.get().sloaded=true;
            return true;
        }
        else
            return false ;

    }



    public boolean isParentPksAlreadyAddedtoItab( fkTable fk){

        Optional<fkid> t = fkidList.stream()
                .filter(u -> u.PKColumn.CON_TABLE.equalsIgnoreCase(fk.PKColumn.CON_TABLE)
                        && u.PKColumn.CON_SCHEMA.equalsIgnoreCase(fk.PKColumn.CON_SCHEMA)
                        ).findFirst();

        if (t.isPresent())
            return true;
        else
            return false ;

    }

    public boolean isParent(String sParentTabName, String sParentSchemaName){

        Optional<fkid> t = fkidList.stream()
                .filter(u -> u.PKColumn.CON_TABLE.equalsIgnoreCase(sParentTabName)
                        && u.PKColumn.CON_SCHEMA.equalsIgnoreCase(sParentSchemaName)
                ).findFirst();

        if (t.isPresent())
            return true;
        else
            return false ;

    }

    public List<fkid> fkids(){
        return fkidList;
    }


    public fkid canPkDeletedinFk(String pkKeyid) {
        ids idfnd = null;
        fkid fkFnd = null;
        fkid freturn= null;
        try {
            for (fkid f : fkids()) {
                idfnd = f.doesPkExists(pkKeyid);
                if (idfnd != null) {
                    fkFnd = f;
                    break;
                }
            }

            freturn = fkFnd.clone();
            freturn.Pks = new ArrayList<ids>();
            freturn.Pks.add(idfnd.clone());
        } catch (Exception e) {
            lSumBJCLogger.WriteErrorStack("Throwable From FieldByName", e);
        } finally {
            return freturn;
        }

    }

    public ids getFkObjforaPK(String pkKeyid )
    {
        ids idfnd = null;
        fkid fkFnd = null;
        fkid freturn= null;
        try {
            for (fkid f : fkids()) {
                idfnd = f.doesPkExists(pkKeyid);
                if (idfnd != null) {
                    break;
                }
            }

        } catch (Exception e) {
            lSumBJCLogger.WriteErrorStack("Throwable From FieldByName", e);
        } finally {
            return idfnd;
        }

    }

    protected void finalize() throws Throwable {

        try {
            for (fkid t : fkidList)
                t.finalize();

            this.fkidList.clear();
            this.fkidList = null;

        }
        catch (Exception e ){

        }
        catch (Error e ){
        }

        finally {
            super.finalize();     }
    }
}

