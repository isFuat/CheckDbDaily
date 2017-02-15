package com.status.java;

import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

/**
 * Created by fuat on 2/10/2017.
 */
public class DailyCheck {

    public static void main(String args[]) {

        if (args.length < 3) {
            System.out.println("Enter property file name: <jar file> <connection file> <property file> <output dir>");
        }
        execute(args);


    }

    private static void execute(String[] args) {
        //Gets properties files
        Properties properties = new Properties();
        Properties properties1 = new Properties();
        InputStream input = null;
        InputStream input1 = null;
        Connection connection = null;
        HashMap<String, String> hashMap = new HashMap<String, String>();
        File output = null;
        FileWriter fileWriter = null;
        BufferedWriter bufferedWriter = null;

        try {

            input1 = new FileInputStream(args[0]);
            input = new FileInputStream(args[1]);
            //write the query result to output file.
            output = new File(args[2]);
            fileWriter  = new FileWriter(output);
            bufferedWriter = new BufferedWriter(fileWriter);
            properties.load(input);
            properties1.load(input1);

            //Get key and values in property file that matches the prefix "query"
            Enumeration enumeration = properties.propertyNames();

            while (enumeration.hasMoreElements()) {
                String key = (String) enumeration.nextElement();
                if (key.startsWith("query")) {
                    hashMap.put(key, properties.getProperty(key));
                }
            }
            //define oracle driver
            Class.forName("oracle.jdbc.driver.OracleDriver");
            //Get connection to oracle database.
            connection = DriverManager.getConnection(properties1.getProperty("url"), properties1.getProperty("user")
                    , properties1.getProperty("password"));
            //Create statement for query execution.
            Statement statement = connection.createStatement();
            //iterate over the key and value which is the table name and query in the property file.

            Calendar cal = Calendar.getInstance(); // creates calendar
            cal.setTime(new Date()); // sets calendar time/date
            SimpleDateFormat ft =
                    new SimpleDateFormat("yyyy/MM/dd 'at' hh:mm:ss a ");
            bufferedWriter.write("Date: "+ft.format(cal.getTime()));
            bufferedWriter.newLine();
            for (String key : hashMap.keySet()) {

                //execute the query.
                ResultSet resultSet = statement.executeQuery(hashMap.get(key));
                //get metadata of the return of the table.
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                //get number of columns for the resultset.
                int numOfColumn = resultSetMetaData.getColumnCount();
                //print table name
                bufferedWriter.newLine();
                bufferedWriter.write("\n############################  "+key.substring(6)+"  ###########################");
                bufferedWriter.newLine();

                StringBuilder stringBuilder = new StringBuilder();


                //get column names of the table.
                stringBuilder.append("|");
                for (int i = 1; i <= numOfColumn; i++) {
                    stringBuilder.append(resultSetMetaData.getColumnName(i));
                    stringBuilder.append("|");

                }

                bufferedWriter.write(stringBuilder.toString());
                bufferedWriter.newLine();
                bufferedWriter.write("--------------------------------------------------------------------------");
                bufferedWriter.newLine();
                //prints the rows value of the resultset.
                while (resultSet.next()) {

                    StringBuilder stringBuilder1 = new StringBuilder();
                    stringBuilder1.append("|");
                    for (int column = 1; column <= numOfColumn; column++) {

                        stringBuilder1.append(resultSet.getObject(column));
                        stringBuilder1.append("|");

                    }

                    bufferedWriter.write(stringBuilder1.toString());
                    bufferedWriter.newLine();
                }


            }
        } catch (IOException ex) {
            System.out.println("File is not found: " + ex.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (input != null && input1 != null) {
                try {
                    input.close();
                    input1.close();
                    connection.close();
                    bufferedWriter.close();
                    fileWriter.close();

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }


}


