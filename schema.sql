

 /* Creating schema: myschema in db [  DB_RAW]  */
        use role sysadmin; 
        use database   DB_RAW  ;
        create schema if not exists myschema with managed access;
        alter schema myschema set comment = 'myschema data for digital Cyber security team''s analysis  Roles created are:    DB_RAW_myschema_RO   DB_RAW_myschema_RW';
       
        

/* creating role:   DB_RAW_myschema_RO */
        use role securityadmin; 
        create role if not exists   DB_RAW_myschema_RO;
        alter role   DB_RAW_myschema_RO set comment = "Role that has read only access on all objects in within the schema: myschema";
         use role sysadmin; 
        use database   DB_RAW;
        grant usage      on schema                     myschema to role   DB_RAW_myschema_RO;
        grant select     on all tables       in schema myschema to role   DB_RAW_myschema_RO;
        grant references on all tables       in schema myschema to role   DB_RAW_myschema_RO;
        grant select     on all views        in schema myschema to role   DB_RAW_myschema_RO;

        grant select     on all views        in schema myschema to role   DB_RAW_myschema_RO;
        
        grant select     on all materialized views in schema myschema to role   DB_RAW_myschema_RO;
        grant read       on all stages       in schema myschema to role   DB_RAW_myschema_RO;
        grant usage      on all stages       in schema myschema to role   DB_RAW_myschema_RO;
        grant usage      on all file formats in schema myschema to role   DB_RAW_myschema_RO;
        grant usage      on all functions    in schema myschema to role   DB_RAW_myschema_RO;
        grant usage      on all sequences    in schema myschema to role   DB_RAW_myschema_RO;
        /*set future object grants*/
        grant select     on future tables       in schema myschema to role   DB_RAW_myschema_RO;
        grant references on future tables       in schema myschema to role   DB_RAW_myschema_RO;
        grant select     on future views        in schema myschema to role   DB_RAW_myschema_RO;

        
        grant select     on future materialized views in schema myschema to role   DB_RAW_myschema_RO;
        grant read       on future stages       in schema myschema to role   DB_RAW_myschema_RO;
        grant usage      on future stages       in schema myschema to role   DB_RAW_myschema_RO;
        grant usage      on future file formats in schema myschema to role   DB_RAW_myschema_RO;
        grant usage      on future functions    in schema myschema to role   DB_RAW_myschema_RO;
        grant usage      on future sequences    in schema myschema to role   DB_RAW_myschema_RO;
        use role securityadmin; 
        grant usage on database   DB_RAW to role   DB_RAW_myschema_RO;

        Grant role   DB_RAW_myschema_RO to role   DB_RAW_RO;
/* creating role:   DB_RAW_myschema_RW */
        use role securityadmin; 
        create role if not exists   DB_RAW_myschema_RW;
        alter role   DB_RAW_myschema_RW set comment = "Role that has crud access on all objects in in within the schema: myschema";
		use role sysadmin; 
        use database   DB_RAW;
		grant select, insert, update, delete, truncate, references on all tables  in schema  myschema to role   DB_RAW_myschema_RW;
		grant create table, create view,   create procedure, create function on schema  myschema to role   DB_RAW_myschema_RW;
    /* granting on future objects*/
    
		grant select, insert, update, delete, truncate, references 
             on future tables      in schema myschema to role   DB_RAW_myschema_RW;
		grant write  on all stages     in schema myschema to role   DB_RAW_myschema_RW;
		grant read, write  on future stages  in schema myschema to role   DB_RAW_myschema_RW;
    
        
		use role securityadmin;
		
        grant role   DB_RAW_myschema_RO to  role   DB_RAW_myschema_RW;/* creating role:   DB_RAW_myschema_RW */


        Grant role   DB_RAW_myschema_RW to role   DB_RAW_RW;

//         create role R_DA_ANALYST;
                        
        grant role   DB_RAW_RW to role R_DA_ANALYST;
