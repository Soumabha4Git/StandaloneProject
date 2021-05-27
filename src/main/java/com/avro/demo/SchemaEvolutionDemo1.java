package com.avro.demo;

import java.io.File;
import java.io.IOException;


import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import com.example.CustomerV1;
import com.example.CustomerV2;

public class SchemaEvolutionDemo1 {

	public static void main(String[] args) {
	
		// Step 1 :- Define Schema
		
		
		
		// Step 2 :- Create Specific Record
		
		CustomerV2.Builder customer2Builder = CustomerV2.newBuilder();
		
		customer2Builder.setFirstName("Shubham");
		customer2Builder.setLastName("Sengupta");
		customer2Builder.setAge(40);
		customer2Builder.setHeight(6);
		customer2Builder.setWeight(90);
		
		
		CustomerV2 customer2 = customer2Builder.build();
		
		System.out.println("CustomerV2 is "+customer2);
		
		// Step 3 :- Write Generic Record to a file
		
		final DatumWriter<CustomerV2> datumWriter = new SpecificDatumWriter<CustomerV2>(CustomerV2.class);
		File avroFile = new File("src/main/resources/avro/customer2-specific.avro"); 
		
		try {
			DataFileWriter<CustomerV2> dataFileWriter = new DataFileWriter<CustomerV2>(datumWriter);
			dataFileWriter.create(customer2.getSchema(),avroFile) ;
			dataFileWriter.append(customer2);
			dataFileWriter.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		// Step 4 :- Read Generic Record from a file of CustomerV2 using CustomerV1 Schema 
		
		final DatumReader<CustomerV1> datumReader = new SpecificDatumReader<CustomerV1>(CustomerV1.class);
		final DataFileReader<CustomerV1> dataFileReader;
		CustomerV1 customer1Record = null;		
			
		try {
			
			System.out.println("Avro File Path :- "+avroFile.getAbsoluteFile());		
			dataFileReader = new DataFileReader<CustomerV1>(avroFile, datumReader);			
			FileReader<CustomerV1> fileReader = DataFileReader.openReader(avroFile,datumReader); 		
			
			// Step 5 :- Interpret as a Generic Record	
			
			System.out.println("File exists is "+dataFileReader.hasNext());			
			customer1Record = fileReader.next() ;
			System.out.println("customer1Record is "+customer1Record.toString());
			dataFileReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}				
		
		// Step 5 :- Interpret as a Generic Record

	}

}
