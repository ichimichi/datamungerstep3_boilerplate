package com.stackroute.datamunger.reader;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Header;

public class CsvQueryProcessor extends QueryProcessingEngine {
    private String fileName;

    // Parameterized constructor to initialize filename
    public CsvQueryProcessor(String fileName) throws FileNotFoundException {
        this.fileName = fileName;
    }

    /*
     * Implementation of getHeader() method. We will have to extract the headers
     * from the first line of the file.
     * Note: Return type of the method will be Header
     */

    @Override
    public Header getHeader() throws IOException {
        String filePath = System.getProperty("user.dir") + "/" + fileName;
        BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(filePath)));

        // read the first line
        // populate the header object with the String array containing the header names
        String[] headers = bufferedReader.readLine().trim().split(",");
        Header header = new Header();
        header.setHeaders(headers);

        return header;
    }

    /**
     * getDataRow() method will be used in the upcoming assignments
     */

    @Override
    public void getDataRow() {

    }

    /*
     * Implementation of getColumnType() method. To find out the data types, we will
     * read the first line from the file and extract the field values from it. If a
     * specific field value can be converted to Integer, the data type of that field
     * will contain "java.lang.Integer", otherwise if it can be converted to Double,
     * then the data type of that field will contain "java.lang.Double", otherwise,
     * the field is to be treated as String.
     * Note: Return Type of the method will be DataTypeDefinitions
     */

    @Override
    public DataTypeDefinitions getColumnType() throws IOException {
        String filePath = System.getProperty("user.dir") + "/" + fileName;
        BufferedReader bufferedReader;

        try {
            bufferedReader = new BufferedReader(new FileReader(new File(filePath)));
        } catch (FileNotFoundException e) {
            return new DataTypeDefinitions();
        }

        bufferedReader.readLine();
        String[] columns = bufferedReader.readLine().trim().split(",", 18);

        List<String> dataTypeList = new ArrayList<>();

        for (String data : columns) {
            try {
                Integer.parseInt(data);
                dataTypeList.add("java.lang.Integer");
            } catch (NumberFormatException e) {
                dataTypeList.add("java.lang.String");
            }
        }
        DataTypeDefinitions dataTypeDefinitions = new DataTypeDefinitions();
        dataTypeDefinitions.setDataTypes(dataTypeList.toArray(new String[dataTypeList.size()]));

        return dataTypeDefinitions;
    }
}
