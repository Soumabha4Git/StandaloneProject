package com.avro.demo.reflection;

import org.apache.avro.reflect.Nullable;

public class ReflectionCustomer {
	
	   private String firstName;
	   private String lastName;
	   private Integer age;
	   private Double height;
	   private Double weight;
	   @Nullable private Boolean automatedEmail = true;
	
	   public ReflectionCustomer() {
		super();
		// TODO Auto-generated constructor stub
	   }

	public ReflectionCustomer(String firstName, String lastName, Integer age, Double height, Double weight,
			Boolean automatedEmail) {
		super();
		this.firstName = firstName;
		this.lastName = lastName;
		this.age = age;
		this.height = height;
		this.weight = weight;
		this.automatedEmail = automatedEmail;
	}

	

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public Integer getAge() {
		return age;
	}

	public void setAge(Integer age) {
		this.age = age;
	}

	public Double getHeight() {
		return height;
	}

	public void setHeight(Double height) {
		this.height = height;
	}

	public Double getWeight() {
		return weight;
	}

	public void setWeight(Double weight) {
		this.weight = weight;
	}

	public Boolean getAutomatedEmail() {
		return automatedEmail;
	}

	public void setAutomatedEmail(Boolean automatedEmail) {
		this.automatedEmail = automatedEmail;
	}

	@Override
	public String toString() {
		return "{firstName=" + firstName + ", lastName=" + lastName + ", age=" + age + ", height="
				+ height + ", weight=" + weight + ", automatedEmail=" + automatedEmail + "}";
	}
	
	
	
	   
	   
	   

}
