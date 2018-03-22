/*
 * PropertyReader.java
 *
 * Created on March 20, 2006, 9:35 AM
 *
 * To change this template, choose Tools | Options and locate the template under
 * the Source Creation and Management node. Right-click the template and choose
 * Open. You can then make changes to the template in the Source Editor.
 */

package bj;
import java.lang.*;
import java.util.*;
import java.io.*;


/**
 *
 * @author Gajoseph
 */
public class  PropertyReader {
    private Properties handleProperties ;
    public static PropertyReader  _instance;
    
    //private 
    
    /** Creates a new instance of PropertyReader */
    public PropertyReader() {
      
      
   //   logger.info("Started the Property Reader");
      

        
    }

    
     public static synchronized  Properties getPropInstance(){
           if(_instance == null ){
               System.out.println("NULL instance ");
         try {    
            _instance = new PropertyReader();
            _instance.handleProperties =  new Properties();
            
        //  _instance.handleProperties.load(new FileInputStream("resources/NLP.properties"));
            
//            File file = new File("NLP.properties");

            _instance.handleProperties.load(_instance.getClass().getClassLoader().getResourceAsStream("NLP.properties"));
  
        //  _instance.handleProperties.load(new FileInputStream("resources/BJC.properties")); // DOS USE this 
             }
            catch (IOException e) {
              //logger.equals(e.getMessage()  )  ;
                System.out.println("Error reading " + e.getStackTrace());
             }

           } 
         return _instance.handleProperties;
         
         
     }
     
     public String getProperty1(String Key){
         return  handleProperties.getProperty(Key);
         
     }
// Decode the password -----     
     public String decode(String s)
	{
		try
		{
			int len = s.length();
			byte[] r = new byte[len/2];
			for (int i = 0; i < r.length; i++)
			{
				int digit1 = s.charAt(i*2);
				int digit2 = s.charAt(i*2 + 1);
				if ((digit1 >= '0') && (digit1 <= '9'))
				  digit1 -= '0';
				else if ((digit1 >= 'a') && (digit1 <= 'f'))
				  digit1 -= 'a' - 10;
				if ((digit2 >= '0') && (digit2 <= '9'))
				  digit2 -= '0';
				else if ((digit2 >= 'a') && (digit2 <= 'f'))
				  digit2 -= 'a' - 10;
				r[i] = (byte)((digit1 << 4) + digit2);
			}
			String sin = new String(r);
			String sout = "";
			for (int i = 0; i < sin.length(); i+=2)
			{
				sout += sin.substring(i, i+1);
			}
			return sout;
		}
		catch (Exception e)
		{
			//Logger.log("Problems in Decription/Encription",Logger.WARNING);
		//	Logger.log("Exception= "+e.getMessage(),Logger.WARNING);
                      //  logger.equals(e.getMessage() )  ;
		}
		return null;

	}
     
    
}


 