package com.duomai.bigdata.textmining;

public class TestStaticInit {
	
	public static void main(String args[]){
		
		System.out.println(App.a);
		
		App.a = 10;
		
		System.out.println(App.a);
		
		String a = "hdfs://192.168.127.130:8020/user/duomai/classification/testDictionaryDir/dictionary.file-0";
		
		System.out.println(a.split("user")[1]);
	}

}
