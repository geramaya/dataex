package de.aspera.dataexport.dataFaker;

import com.github.javafaker.Faker;

public class DataFaker {
	
	private Faker faker;
	
	public DataFaker() {
		this.faker = new Faker();
	}
	public  String fakeDate() {
		return null;
	}
	public  String fakeNumber() {
		return null;
	}
	public  String fakeString() {
		return null;
	}
	public  String fakeStringWithLength(int maxLength) {
		return faker.regexify("[a-zA-Z]{3,"+maxLength+"}$");
	}
	public  String fakeNumberWithinRange(int firstNum, int secondNum) {
		return "fakeNum called";
	}
	public String fakeBoolean() {
		return null;
	}
	public String fakeSHA256() {
		return null;
	}
	public String fakeEmail() {
		return null;
	}

}
