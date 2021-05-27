package com.avro.demo;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

public class GenericRecordDemo {

	private static DataFileWriter<GenericRecord> dataFileWriter;
	private static DataFileReader<GenericRecord> dataFileReader;

	public static void main(String[] args) {

		Schema.Parser parser = null;
		Schema schema = null;

		// Step 1 :- Define Schema

		File avscFile = new File("src/main/resources/avro/customer.avsc");
		if (avscFile.exists()) {
			System.out.println("File name: " + avscFile.getName());
			System.out.println("Absolute path: " + avscFile.getAbsolutePath());
		}

		try {
			parser = new Schema.Parser();
			schema = parser.parse(avscFile);
		} catch (IOException e) {

			e.printStackTrace();
		}

		// Step 2 :- Create Generic Record

		GenericRecordBuilder customerBuilder = new GenericRecordBuilder(schema);

		customerBuilder.set("firstName", "Shubham");
		customerBuilder.set("lastName", "Sengupta");
		customerBuilder.set("age", 38);
		customerBuilder.set("height", 6);
		customerBuilder.set("weight", 90);

		GenericData.Record customer = customerBuilder.build();
		System.out.println("customer is " + customer);

		// Step 3 :- Write Generic Record to a file

		final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		File avroFile = new File("src/main/resources/avro/customer-generic.avro"); 
		
		try {
			dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
			dataFileWriter.create(customer.getSchema(),avroFile) ;
			dataFileWriter.append(customer);
			dataFileWriter.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// Step 4 :- Read Generic Record from a file
		
		final DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		GenericRecord customerRecord;
		
		try {
			dataFileReader = new DataFileReader<GenericRecord>(avroFile, datumReader);
			FileReader<GenericRecord> fileReader = DataFileReader.openReader(avroFile,datumReader); 	
			
		// Step 5 :- Interpret as a Generic Record	
			
			System.out.println("File exists is "+fileReader.hasNext());			
			customerRecord = dataFileReader.next() ;
			dataFileReader.close();
			System.out.println("customerRecord is "+customerRecord.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}		

		

	}

}
