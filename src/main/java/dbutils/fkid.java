package dbutils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class fkid extends fkTable implements Cloneable {

    public  List<ids> Pks;

    public String getFkTabname() {
        return fkTabname;
    }

    public String fkTabname= "";// assuming there will be only one Fk on a table from parent key HB--HBID(pk)-->HBStable
    public boolean sloaded = false ; //will be called from idTab objct to set this; we are inheriting PKColumna nd Fk column and loading them

    @Override
    public fkid clone() throws CloneNotSupportedException {
        return (fkid) super.clone();
    }

    public fkid( fkTable fktab) {
       super(fktab);
            this.fkTabname = fktab.FkColumn.CON_TABLE;

        Pks = new ArrayList<ids>();
    }


    public fkid() {
        super();
        Pks = new ArrayList<ids>();
    }


    private ids Fk_pks(String fkvalue, ids pids ) {

        try {

            Optional<ids> t =   Pks.stream().filter(e -> e.FkID.equalsIgnoreCase(fkvalue)).findFirst();

            if (t.isPresent()) {
                return t.get();
            } else {
                    ids lids = new ids();

                    lids.FkID = fkvalue;
                    lids.parIds  = new ids();
                    lids.parIds = pids ;
                    Pks.add(lids);// add to the list
                   return lids;
            }

        } catch (Exception e) {

            throw new RuntimeException("unexpected invocation exception: " + e.getMessage());

        } catch (Error e) {
            throw new Error("unexpected Error occured : " + e.getMessage());
        } finally {////System.out.println("End of Funciton FieldBYname ");
        }


    }


    public  ids  doesPkExists(String fkvalue)   {
        ids   idsreturn = null;
        try {
            //Pks.stream().filter(ids -> ids.checkPkvalueExits(fkvalue)).collect(Collectors.toList());
            for (ids lids : this.Pks)
                if (lids.checkPkvalueExits(fkvalue)) {
                    idsreturn = lids;
                    break;
                }

        } catch (Exception e) {


        } catch (Error e) {
            //throw new Error("unexpected Error occured : " + e.getMessage());
        } finally {////System.out.println("End of Funciton FieldBYname ");
        }
        return idsreturn;

    }



    public boolean AddParentKeyWithChldKeys(String fkParentValue , String pkChildValue, boolean isDeleteAble , ids pids  )
    {   //--GEO--C--04/07/2018  Check if we have already added FK-- which is parent's PK
        // Fk_pks will retunr an oject of IDS
        boolean alreadyPresent= false;
        try {
            fkParentValue =(fkParentValue==null)? "null":  fkParentValue ;
            pkChildValue =(pkChildValue==null)? "null":  pkChildValue ;

            ids objparfk_pkids =  Fk_pks( fkParentValue, pids );//
            objparfk_pkids.deleteable = isDeleteAble;
            alreadyPresent = objparfk_pkids.addChildKeyforaParentFkid(pkChildValue);// check if it is already added // like going ina look

        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        return alreadyPresent;

    }


    protected void finalize() throws Throwable {
        try {
            comfun.hasIdsRemovelst(this.Pks);
            super.finalize();
        }
        catch (Exception e ){
        }
        catch (Error e ){
        }
        finally {
            super.finalize();     }
    }

}

