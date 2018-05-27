 /*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bj;

/**
 *
 * @author TGAJ2
 */

import java.io.*;
import java.lang.String;
import java.nio.channels.FileChannel;


public  class fileutils {
    
    private String _DATE_FORMAT = "yyyy-MM-dd HH:mm:ss:SSS";    
    private File  _objSrcFile;
    private String _sSrcFilename  ;
    private String _sDestFilename;
    private File  _objDestFile;
    

    
    
    public static void delallFiles(String sFilePath)
    {
        for (File file: (new File(sFilePath)).listFiles()) {
            if (file.isDirectory()) delallFiles(file.getPath());
            file.delete();
        }
    
    }
    
    
    public static boolean bkupFile(String sInputfile, String sOutputfile ) {
         FileChannel inputChannel = null;
         FileChannel outputChannel = null;
         File  _objDestFile = new File(sOutputfile);
         boolean return1 = true; 
         Writer output = null;
         try {
              //if (!(_objDestFile.exists())) {
                output = new BufferedWriter( new FileWriter(_objDestFile)  );
                output.write( "" );
                output.close();
              //}   
            
             inputChannel = new FileInputStream(sInputfile).getChannel();
             outputChannel = new FileOutputStream(sOutputfile).getChannel();
             outputChannel.transferFrom(inputChannel, 0, inputChannel.size());
             inputChannel.close();
             outputChannel.close();
         }
        catch (FileNotFoundException ex) { 
            return1= false;
        }   
         catch (IOException ex) { 
            return1= false;
        }   
         finally 
         {
             
         
         }
         return return1;
    
    }

    
    public static  void appendFile (String sFlName, String sOutputfile, String sBkupFileNamePrefix) throws IOException {
            if (sFlName.equals(""))      throw new IllegalArgumentException("File Name cannotbe null.");
                 
            File  _objSrcFile   = new File(sFlName);
            File  _objDestFile = new File(sOutputfile);
            Writer output = null;
            FileInputStream  _ofisSrc ;
            String sFileContent = "";
            
            if (_objSrcFile.isDirectory()) {
                _objSrcFile = null;
                throw new IllegalArgumentException("Should not be a directory: " + sFlName);
            }
            
            if (_objDestFile.isDirectory()) {
                _objDestFile = null;
                throw new IllegalArgumentException("Should not be a directory: " + sFlName);
            }
            
            
             if  ( bkupFile(sOutputfile,sOutputfile +"_" +sBkupFileNamePrefix )   ) 
             {
                 try {
                 _ofisSrc = new FileInputStream(_objSrcFile);    
                 byte[] data = new byte[(int) _objSrcFile.length()];
                 _ofisSrc.read(data);
                 _ofisSrc.close();
                 sFileContent = new String(data, "UTF-8");

                  output = new BufferedWriter( new FileWriter(_objDestFile, true)  );
                  
                  output.write( System.getProperty("line.separator") );
                  output.write( sFileContent );

                }
                finally {
                  //flush and close both "output" and its underlying FileWriter
                  if (output != null)  output.close();

                }
             
             
             }    
        
    }
    
    
}
