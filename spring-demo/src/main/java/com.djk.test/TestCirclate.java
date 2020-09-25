package com.djk.test;

public class TestCirclate {
	public static void main(String[] args) {
		A a = new A();
	}
}

class A{
	B b = new B();
}

class B{
	A a = new A();
}
