package com.avro.demo;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;


import com.avro.demo.reflection.ReflectionCustomer;

public class ReflectionRecordDemo {

	public static void main(String[] args) {	
		
		DataFileWriter<ReflectionCustomer> dataFileWriter;		
		DataFileReader<ReflectionCustomer> dataFileReader;
		
		
		// Step 1 :- Define Schema
		
		Schema schema = ReflectData.get().getSchema(ReflectionCustomer.class);	
		// System.out.println("Schema is "+schema.toString(true));
		
		// Step 2 :- Create Reflection Record
		
		ReflectionCustomer reflectedCustomer = new ReflectionCustomer("Shubh", "Sengupta", 39, 5.11, 180.0, false);
		System.out.println("customerRecord is" + reflectedCustomer.toString());
		
		// Step 3 :- Write Reflected Record to a file
		
		   final DatumWriter<ReflectionCustomer> datumWriter = new ReflectDatumWriter<>(ReflectionCustomer.class);
		   File reflectAvroFile = new File("src/main/resources/avro/customer-reflected.avro"); 
		   
		   try {
				dataFileWriter = new DataFileWriter<>(datumWriter);
				dataFileWriter.create(schema,reflectAvroFile) ;
				dataFileWriter.append(reflectedCustomer);
				dataFileWriter.close();

			} catch (IOException e) {
				e.printStackTrace();
			}
		   
		// Step 4 :- Read Generic Record from a file
		   
		   DatumReader<ReflectionCustomer> datumReader = new ReflectDatumReader<>(ReflectionCustomer.class);
		   ReflectionCustomer reflectedCustomerRecord = null;	
		   
		   try {
				
				System.out.println("Avro File Path :- "+reflectAvroFile.getAbsoluteFile());		
				dataFileReader = new DataFileReader<ReflectionCustomer>(reflectAvroFile, datumReader);			
				FileReader<ReflectionCustomer> fileReader = DataFileReader.openReader(reflectAvroFile,datumReader); 


			// Step 5 :- Interpret as a Generic Record	
				
				System.out.println("File exists is "+dataFileReader.hasNext());			
				reflectedCustomerRecord = fileReader.next() ;
				System.out.println("customerRecord is "+reflectedCustomerRecord.toString());
				dataFileReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
	}

}
