/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbutils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author TGAJ2
 */
public class dbtype {



public static List<jdbcType> db2types = loadDb2TYpes();
    

public enum isnull{                
NULL(1),
NOT_NULL(2);

 private final int isnullCode;

    isnull(int levelCode) {
        this.isnullCode = levelCode;


        }
private static final Map<String, isnull> lookup = new HashMap<String, isnull>();    

static {
        for (isnull d : isnull.values()) {
            lookup.put(Integer.toString(d.isnullCode), d);
        }
    }

    
}
    
    
    
public enum db{                
POSTGRES(2003),
DB2(-5),
MYSQL(-100)
;

 private final int dbCode;

    db(int levelCode) {
        this.dbCode = levelCode;


        }
private static final Map<String, db> lookup = new HashMap<String, db>();    

static {
        for (db d : db.values()) {
            lookup.put(Integer.toString(d.dbCode), d);
        }
    }

    

 public static db get(String abbreviation) {
        return lookup.get(abbreviation);
    }
} 

public enum reference_option {                
importedKeyNoAction(3), //- do not allow delete of primary key if it has been imported
importedKeyCascade(0),// - delete rows that import a deleted key
importedKeySetNull(2),// - change imported key to NULL if its primary key has been deleted
importedKeyRestrict(1),// - same as importedKeyNoAction (for ODBC 2.x compatibility)
importedKeySetDefault(4);// - change imported key to default if its primary key has been deleted
 
    
    
 private final int reference_option_code;

     reference_option(int levelCode) {
        this.reference_option_code = levelCode;


        }
private static final Map<String, reference_option> lookup = new HashMap<String, reference_option>();    

static {
        for (reference_option d : reference_option.values()) {
            lookup.put(Integer.toString(d.reference_option_code), d);
        }
    }

    

 public static reference_option get(String abbreviation) {
        return lookup.get(abbreviation);
    }
 
  
} 


    
    
    
interface JDBC {
 
}    
    
    
    
    
public enum lJDBC implements JDBC{                
    ARRAY(2003),
    BIGINT(-5),
    BINARY(-2),
    BIT(-7),
    BLOB(2004),
    BOOLEAN(16),
    CHAR(1),
    CLOB(2005),
    DATALINK(70),
    DATE(91),
    DECIMAL(3),
    DISTINCT(2001),
    DOUBLE(8),
    FLOAT(6),
    INTEGER(4),
    JAVA_OBJECT(2000),
    LONGNVARCHAR(-16),
    LONGVARBINARY(-4),
    LONGVARCHAR(-1),
    NCHAR(-15),
    NCLOB(2011),
    NULL(0),
    NUMERIC(2),
    NVARCHAR(-9),
    OTHER(1111),
    REAL(7),
    REF(2006),
    ROWID(-8),
    SMALLINT(5),
    SQLXML(2009),
    STRUCT(2002),
    TIME(92),
    TIMESTAMP(93),
    TINYINT(-6),
    VARBINARY(-3),
    VARCHAR(12)
;

 private final int lJDBCCode;

    lJDBC(int levelCode) {
        this.lJDBCCode = levelCode;


        }
private static final Map<String, lJDBC> lookup = new HashMap<String, lJDBC>();    

static {
        for (lJDBC d : lJDBC.values()) {
            lookup.put(Integer.toString(d.lJDBCCode), d);
        }
    }

    

 public static lJDBC get(String abbreviation) {
        return lookup.get(abbreviation);
    }
} 

/*
postgres JDBC mapping 
*/


public enum PosJDBC implements JDBC{                
ARRAY(2003),
BIGINT(-5),
BYTEA(-2),
BIT(-7),
BLOB(2004),
BOOLEAN(16),
CHARACTER(1),
CLOB(2005),
DATALINK(70),
DATE(91),
DECIMAL(3),
DISTINCT(2001),
DOUBLE_PRECISION(8),
FLOAT(6),
INTEGER(4),
JAVA_OBJECT(2000),
TEXT(-16) ,//LONGNVARCHAR(-16),
BYTE_A(-4), // --LONGVARBINARY(-4),
LONGVARCHAR(-1),
NCHAR(-15),
NCLOB(2011),
NULL(0),
NUMERIC(2),
NVARCHAR(-9),
OTHER(1111),
REAL(7),
REF(2006),
ROWID(-8),
SMALLINT(5),
XML(2009),
STRUCT(2002),
TIME(92),
TIMESTAMP(93),
TINYINT(-6),
VARBINARY(-3),
VARCHAR(12)
;

 private final int PosJDBCCode;

    PosJDBC(int levelCode) {
        this.PosJDBCCode = levelCode;


}
private static final Map<String, PosJDBC> lookup = new HashMap<String, PosJDBC>();    

static 
    {
        for (PosJDBC d : PosJDBC.values()) 
        {
            lookup.put(Integer.toString(d.PosJDBCCode), d);
        }
    }

    

 public static PosJDBC get(String abbreviation) {
        return lookup.get(abbreviation);
    }
} 

/// DB2 Mapping 



// MYSQL Mapping

    public enum db2JDBC implements JDBC{
        LONG_VARCHAR(-1),
        CHAR(1),
        DECIMAL(3),
        INTEGER(4) ,//ok 
        SMALLINT(5) ,//ok 
        REAL(7) ,
        DOUBLE(8) ,
        VARCHAR(12) ,
        BOOLEAN(16	),
        DATE(91	),
        TIME(92	),
        TIMESTAMP(93	),
        DECFLOAT(1111	),
        XML(2009	),// remapped to SQLXMLXML				DATA_TYPE: 	1111
        DISTINCT(2001	),
        ROW(2002	),
        ARRAY(2003	),
        BLOB(2004	),
        CLOB(2005	),
        //DBCLOB(2005	),
        BIGINT(-5	),
        LONG_VARCHAR_FOR_BIT_DATA(-4	),
        VARCHAR_FOR_BIT_DATA(-3	),
        CHAR_FOR_BIT_DATA(-2),
//    VARCHAR () FOR BIT DATA(2004	),
//    CHAR () FOR BIT DATA(2004	),

        
        ;

        private final int db2JDBCCode;

        db2JDBC(int levelCode) {
            this.db2JDBCCode = levelCode;

        }
        private static final Map<String, db2JDBC> lookup = new HashMap<String, db2JDBC>();

        static {
            for (db2JDBC d : dbtype.db2JDBC.values()) {
                lookup.put(Integer.toString(d.db2JDBCCode), d);
            }
        }
        public static db2JDBC get(String abbreviation) {
            return lookup.get(abbreviation);
        }
    }


 
public static List<jdbcType>  loadDb2TYpes() {
    List<jdbcType> db2types = new ArrayList<jdbcType>();
        
    db2types.add(new jdbcType("LONG VARGRAPHIC"	,	-1	) );
    db2types.add(new jdbcType(	"CHAR"          ,	1	) );
//    db2types.add(new jdbcType(	"GRAPHIC"	,	1	) );
    db2types.add(new jdbcType(	"DECIMAL"	,	3	) );
    db2types.add(new jdbcType(	"INTEGER"	,	4	) );//ok 
    db2types.add(new jdbcType(	"SMALLINT"	,	5	) );//ok 
    db2types.add(new jdbcType(	"REAL"          ,	7	) );
    db2types.add(new jdbcType(	"DOUBLE"	,	8	) );
    db2types.add(new jdbcType(	"VARCHAR"	,	12	) );
//    db2types.add(new jdbcType(	"VARGRAPHIC"	,	12	) );
    db2types.add(new jdbcType(	"BOOLEAN"	,	16	) );
    db2types.add(new jdbcType(	"DATE"          ,	91	) );
    db2types.add(new jdbcType(	"TIME"          ,	92	) );
    db2types.add(new jdbcType(	"TIMESTAMP"	,	93	) );
    db2types.add(new jdbcType(	"DECFLOAT"	,	1111	) );
    db2types.add(new jdbcType(	"XML"           ,	2009	) );
    db2types.add(new jdbcType(	"DISTINCT"	,	2001	) );
    db2types.add(new jdbcType(	"ROW"           ,	2002	) );
    db2types.add(new jdbcType(	"ARRAY"         ,	2003	) );
    db2types.add(new jdbcType(	"BLOB"          ,	2004	) );
    db2types.add(new jdbcType(	"CLOB"          ,	2005	) );
    db2types.add(new jdbcType(	"DBCLOB"	,	2005	) );

    db2types.add(new jdbcType(	"BIGINT"                        ,	-5	) );
    db2types.add(new jdbcType(	"LONG VARCHAR FOR BIT DATA"	,	2004	) );
    db2types.add(new jdbcType(	"VARCHAR () FOR BIT DATA"	,	2004	) );
    db2types.add(new jdbcType(	"CHAR () FOR BIT DATA"          ,	2004	) );
    db2types.add(new jdbcType(	"LONG VARCHAR"                  ,	-1	) );
    db2types.add(new jdbcType(	"LONG VARGRAPHIC"               ,	-1	) );

    return db2types;    
        
        
        
}

// MYSQL Mapping

    public enum mySqlJDBC implements JDBC{
        ARRAY(2003),
                BIGINT(-5), // mysql has
        LONGBLOB_(-2), // mysql BINARY
        BIT(-7),
        LONGBLOB(2004),
        BOOLEAN(16),
        CHAR(1),
        LONGTEXT___(2005),
        DATALINK(70),
        DATE(91),
        DECIMAL(3),// mysql has
        REAL_(2001),// mysql has docuble precision but mappin gto REAL
            DOUBLE(8),
        FLOAT(6),// has float as well
                INT(4), //mysql has only inT INTEGER(4),
        JAVA_OBJECT(2000),
        //LONGNVARCHAR(-16),
        LONGTEXT(-16),
        LONGBLOB__(-4),
        LONGTEXT__(-1),
        CHAR_(-15),
        LONGTEXT_(2011),//mysql done't have nCLOB
        NULL(0),
        DECIMAL_(2),// mysql deons't have mueric to mappying to decimal
        VARCHAR_1(-9),
        OTHER(1111),
            REAL(7),//mysql has REAL
        REF(2006),
        ROWID(-8),
                SMALLINT(5), // mysql has
        SQLXML(2009),
        STRUCT(2002),
        TIME(92),
        DATETIME(93),// mysql TIMESTAMP has a range of '1970-01-01 00:00:01' UTC to '2038-01-19 03:14:07' UTC. so chnaging to Datetime
                TINYINT(-6), //mysql has tinyint
        VARBINARY(-3), // mysql VARBINARY
        VARCHAR(12)
        ;

        private final int mySqlJDBCCode;

        mySqlJDBC(int levelCode) {
            this.mySqlJDBCCode = levelCode;


        }
        private static final Map<String, mySqlJDBC> lookup = new HashMap<String, mySqlJDBC>();

        static {
            for (mySqlJDBC d : dbtype.mySqlJDBC.values()) {
                lookup.put(Integer.toString(d.mySqlJDBCCode), d);
            }
        }



        public static mySqlJDBC get(String abbreviation) {
            return lookup.get(abbreviation);
        }
    }


     public static String limit_fetchrows(){
    String slimiting = "";
            if (dbtype.db.POSTGRES.name().equals("POSTGRES"))
                slimiting  = String.format(" Limit %d", 10);
            else if (dbtype.db.DB2.name().equals("DB2"))
                slimiting   = String.format(" Fetch first %d rows only ", 10);
            else
                slimiting   = String.format(" Limit %d", 10);

        return slimiting;
     }





}

