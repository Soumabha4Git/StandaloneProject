package com.readInput.demo;

import java.util.Scanner;

public class ReadThruScanner {

	public static void main(String[] args) {

		int firstNum, secondNum;
		Scanner scanner = new Scanner(System.in);
		// System.out.println("Enter a first number:");
		firstNum = Integer.parseInt(scanner.nextLine());
		// System.out.println("Enter a second number:");
		secondNum = Integer.parseInt(scanner.nextLine());
		System.out.println(firstNum);
		System.out.println(secondNum);

		scanner.close();

	}

}
