package com.collectionserver;

import org.ini4j.Ini;
import org.ini4j.InvalidFileFormatException;
import org.nmap4j.Nmap4j;
import org.nmap4j.core.nmap.NMapExecutionException;
import org.nmap4j.core.nmap.NMapInitializationException;

import java.io.File;
import java.io.IOException;

public class nmap {

    public String run (String command){
        String npx = "";
        String flag;
        String host;

        String[] array = command.split(" ");
        flag = array[0];
        host = array[array.length-1];
        for (int i = 1; i < array.length - 1; i++) {
            flag += " " + array[i];
        }

        try {
            String path = "C:\\Nmap" ;
            Ini ini = new Ini(new File("config.ini"));
            path = ini.get("config", "nmap_path");
            Nmap4j nmap4j = new Nmap4j( path ) ;
            nmap4j.addFlags( flag ) ;
            nmap4j.includeHosts( host ) ;
            nmap4j.execute() ;
            if( !nmap4j.hasError() ) {
                npx = nmap4j.getNotParsedXMLResult() ;
                String errors = nmap4j.getExecutionResults().getErrors() ;
                if (errors == null ) {
                    System.out.println("fail");
                }
                //System.out.println(npx);
            }
        } catch (NMapInitializationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.out.println("fail"); ;
        } catch (NMapExecutionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.out.println("fail");

        } catch (InvalidFileFormatException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return npx;
    }
}
