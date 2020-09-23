package com.djk.test;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;

public class SpringTest {
	public static void main(String[] args) {
		BeanFactory beanFactory = new XmlBeanFactory(new ClassPathResource(""));
		beanFactory.getBean("");
		// 示例
		ApplicationContext ac = new ClassPathXmlApplicationContext("applicationContext.xml");
		ac.getBean("studentService");


	}
}
