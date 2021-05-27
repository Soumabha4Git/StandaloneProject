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

public class SchemaEvolutionDemo2 {

	public static void main(String[] args) {
	
		// Step 1 :- Define Schema
		
		
		
		// Step 2 :- Create Specific Record
		
		CustomerV1.Builder customer1Builder = CustomerV1.newBuilder();
		
		customer1Builder.setFirstName("Shubham");
		customer1Builder.setLastName("Sengupta");
		customer1Builder.setAge(40);
		customer1Builder.setHeight(6);
		customer1Builder.setWeight(90);
		customer1Builder.setPhone("+91-907-343-1293");
		customer1Builder.setEmail("simply.shubham@gmail.com");
		
		CustomerV1 customer1 = customer1Builder.build();
		
		System.out.println("CustomerV1 is "+customer1);
		
		// Step 3 :- Write Generic Record to a file
		
		final DatumWriter<CustomerV1> datumWriter = new SpecificDatumWriter<CustomerV1>(CustomerV1.class);
		File avroFile = new File("src/main/resources/avro/customer1-specific.avro"); 
		
		try {
			DataFileWriter<CustomerV1> dataFileWriter = new DataFileWriter<CustomerV1>(datumWriter);
			dataFileWriter.create(customer1.getSchema(),avroFile) ;
			dataFileWriter.append(customer1);
			dataFileWriter.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		// Step 4 :- Read Generic Record from a file of CustomerV1 using CustomerV2 Schema 
		
		final DatumReader<CustomerV2> datumReader = new SpecificDatumReader<CustomerV2>(CustomerV2.class);
		final DataFileReader<CustomerV2> dataFileReader;
		CustomerV2 customer2Record = null;		
			
		try {
			
			System.out.println("Avro File Path :- "+avroFile.getAbsoluteFile());		
			dataFileReader = new DataFileReader<CustomerV2>(avroFile, datumReader);			
			FileReader<CustomerV2> fileReader = DataFileReader.openReader(avroFile,datumReader); 		
			
			// Step 5 :- Interpret as a Generic Record	
			
			System.out.println("File exists is "+dataFileReader.hasNext());			
			customer2Record = fileReader.next() ;
			System.out.println("customer2Record is "+customer2Record.toString());
			dataFileReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}				
		
		// Step 5 :- Interpret as a Generic Record

	}

}
