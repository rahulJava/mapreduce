import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class QueryIndex {

    public static void main(String[] args) throws Exception {
        String directory_path;
        if(args.length<1)
             throw new Exception("Please provide a valid path");
        else
        	/*
        	 * taking the path from the user which contains the reduced output.
        	 */
             directory_path = args[1] + "/*";

        /* 
         * taking the query file location from the user.
         */
        String filePath=args[0];
        String outFilePath=args[2];
        /*
         * Uisng the Java.lang.processbuilder to execute the command 
         */
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command("/bin/bash","-c","hadoop fs -cat "+ directory_path);

        try {

            Process process = processBuilder.start();

            /* 
             * taking the input stream of the process builder in the Buffered reader
             */
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(process.getInputStream()));

            
            String message;
            Map<String, String[]> queryMap = new HashMap<String, String[]>();

            /* 
             * iterating the buffered reader and adding the values in hashmap
             */
            while ((message = reader.readLine()) != null) {
             //   System.out.println(message);
                String[] keyValueArray = message.split("\t",2);
                String key = keyValueArray[0];
                String[] valueArray = keyValueArray[1].split(",");
                queryMap.put(key,valueArray);
               // System.out.println("Key:"+key +"::::"+Arrays.toString(queryMap.get(key)));
            }

            
            StringBuilder toReturn = new StringBuilder();
            File file = new File(filePath);
            Scanner scanner = new Scanner(file); 
            do{
               
                    String line1 = scanner.nextLine();
                    for (String word : line1.split("\\s")) {
                        if (!word.isEmpty())
                        {
                        	toReturn.append(word+" :"+Arrays.toString(queryMap.get(word.toLowerCase())));
                        	toReturn.append('\n');
                        	System.out.println(word+" :"+Arrays.toString(queryMap.get(word.toLowerCase())));
                        }
                       
                }
            }while(scanner.hasNext());
            
            Writer writer = null;

            try {
                writer = new BufferedWriter(new OutputStreamWriter(
                      new FileOutputStream(outFilePath), "utf-8"));
                writer.write(toReturn.toString());
            } catch (IOException ex) {
            	ex.printStackTrace();
                // Report
            } finally {
               try {writer.close();} catch (Exception ex) {/*ignore*/}
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}