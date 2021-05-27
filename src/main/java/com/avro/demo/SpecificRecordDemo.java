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

import com.example.Customer;

public class SpecificRecordDemo {

	public static void main(String[] args) {
	
		// Step 1 :- Define Schema
		
		
		
		// Step 2 :- Create Specific Record
		
		Customer.Builder customerBuilder = Customer.newBuilder();
		
		customerBuilder.setFirstName("Shubham");
		customerBuilder.setLastName("Sengupta");
		customerBuilder.setAge(40);
		customerBuilder.setHeight(6);
		customerBuilder.setWeight(90);
		customerBuilder.setAutomatedEmail(false);
		
		Customer customer = customerBuilder.build();
		
		System.out.println("Customer is "+customer);
		
		// Step 3 :- Write Generic Record to a file
		
		final DatumWriter<Customer> datumWriter = new SpecificDatumWriter<Customer>(Customer.class);
		File avroFile = new File("src/main/resources/avro/customer-specific.avro"); 
		
		try {
			DataFileWriter<Customer> dataFileWriter = new DataFileWriter<Customer>(datumWriter);
			dataFileWriter.create(customer.getSchema(),avroFile) ;
			dataFileWriter.append(customer);
			dataFileWriter.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		// Step 4 :- Read Generic Record from a file
		
		final DatumReader<Customer> datumReader = new SpecificDatumReader<Customer>(Customer.class);
		final DataFileReader<Customer> dataFileReader;
		Customer customerRecord = null;		
			
		try {
			
			System.out.println("Avro File Path :- "+avroFile.getAbsoluteFile());		
			dataFileReader = new DataFileReader<Customer>(avroFile, datumReader);			
			FileReader<Customer> fileReader = DataFileReader.openReader(avroFile,datumReader); 		
			
			// Step 5 :- Interpret as a Generic Record	
			
			System.out.println("File exists is "+dataFileReader.hasNext());			
			customerRecord = fileReader.next() ;
			System.out.println("customerRecord is "+customerRecord.toString());
			dataFileReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}				
		
		// Step 5 :- Interpret as a Generic Record

	}

}
