package com.readInput.demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ReadThruInputStreamReader {

	public static void main(String[] args) {
		
		int firstNum, secondNum;
	      BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
	      try {
	         // System.out.println("Enter a first number:");
	         firstNum = Integer.parseInt(br.readLine());
	         // System.out.println("Enter a second number:");
	         secondNum = Integer.parseInt(br.readLine());
	         
	         System.out.println(firstNum);
	         System.out.println(secondNum);
	      } catch (IOException ioe) {
	         System.out.println(ioe);
	      }

	}

}
