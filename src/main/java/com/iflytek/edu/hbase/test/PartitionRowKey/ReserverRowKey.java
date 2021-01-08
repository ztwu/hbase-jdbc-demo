package com.iflytek.edu.hbase.test.PartitionRowKey;

public class ReserverRowKey {

    public static String stringToAscii(String value)
    {
        StringBuffer sbu = new StringBuffer();
        char[] chars = value.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            if(i != chars.length - 1)
            {
                sbu.append((int)chars[i]).append(",");
            }
            else {
                sbu.append((int)chars[i]);
            }
        }
        return sbu.toString();
    }

    public static String asciiToString(String value)
    {
        StringBuffer sbu = new StringBuffer();
        String[] chars = value.split(",");
        for (int i = 0; i < chars.length; i++) {
            sbu.append((char) Integer.parseInt(chars[i]));
        }
        return sbu.toString();
    }

    public static void main(String[] args) {

        /**
         * 对于自增长的key,可以逆转
         */
        int a = 1234;
        String as = String.valueOf(a);
        String rs = new StringBuilder(as).reverse().toString();
        System.out.println(rs);

        String str = "{name:1234,password:4444}";
        String asciiResult = stringToAscii(str);
        System.out.println(asciiResult);
        String stringResult = asciiToString(asciiResult);
        System.out.println(stringResult);

        /**
         *
         * 1，取rowkey的部分字符
         * 2，获取字符的assii码，累加求和
         * 3，累加值除分区数，求余数即为分区号
         * 4，rowkey+分区号为新的key
         * 5，splitkeys ['0','1','2']
         *
         */

    }
}
