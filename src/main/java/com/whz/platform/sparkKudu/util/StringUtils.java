package com.whz.platform.sparkKudu.util;

public class StringUtils {
	public static String getVolidString(String value) {
		if (value.contains("\r\n")) {
			value = value.replaceAll("\r\n", "");
		}
		if (value.contains("\t")) {
			value = value.replaceAll("\t", "");
		}
		if (value.contains("\n")) {
			value = value.replaceAll("\n", "");
		}
		if (value.contains("\r")) {
			value = value.replaceAll("\r", "");
		}
		return value;
	}
}
