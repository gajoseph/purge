/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbutils;

import java.io.IOException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author TGAJ2
 */
public class progresDDL {
    
    
      // TODO code application logic here
 public static void main(String[] args) throws ClassNotFoundException {      
        java.sql.Connection conn1;
        String strsql;
        ResultSet objRS = null;
        Statement _Statement = null;
        String message = "Hello World!";
        
        int x=1; 
        

        try {
        
        
            // channel.basicPublish("", QUEUE_NAME, null, message.getBytes("UTF-8"));/ channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            Class.forName("org.postgresql.Driver");
            String url = "jdbc:postgresql://stgd542a/qtgdb?user=tgaj2&password=8UcREt3p&ssl=false";

            conn1 = java.sql.DriverManager.getConnection(url);
            conn1.setAutoCommit(false);
            
int i=0 ;

            _Statement = conn1.createStatement();

            
        
        while (i<=10 )
        {
            _Statement.execute("create table appdba.asd_"+ i++ +" as select * from pg_tables");
          //  _Statement.getConnection()
            conn1.commit();
        }
        _Statement.close();
            
        while (i<=10 )
        {
            _Statement.execute("truncate table appdba.asd_"+ i++ );
        }
        _Statement.close();
            
            
        } 
        catch (SQLException ex) {
            System.out.println(ex.getMessage());

        }
        finally {
            objRS = null;
            _Statement = null;
            //    _output = null;
            conn1 = null;

            
        
        }

    }
    
    
    
}
