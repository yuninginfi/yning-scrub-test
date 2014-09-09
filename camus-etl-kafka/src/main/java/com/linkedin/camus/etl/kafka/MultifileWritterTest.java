package com.linkedin.camus.etl.kafka;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class MultifileWritterTest {
	public static void main(String args[]) throws Exception {
		List<Writer> writters = new ArrayList<Writer>();
		FileOutputStream out = null;

		try {
			// Create 1000 file outputs
			for (int i = 0; i < 1000; i++) {
				out = new FileOutputStream("/tmp/test/" + String.valueOf(i) + ".txt");
				Writer writer = new BufferedWriter(new OutputStreamWriter(out));
				writters.add(writer);
			}
			
			
			SecureRandom random = new SecureRandom();
			for(int i = 0 ; i < 1000000 ; i ++)
			{
				HashMap<String, String> map = new HashMap<String, String>();
		        for(int j = 0 ; j < 300 ; j++){
		            map.put(new BigInteger(130, random).toString(32), new BigInteger(130, random).toString(32));
		        }
				for(Writer writer : writters)
				{
					String a = new String(String.valueOf(i));
					writer.write(String.valueOf(i));
				}
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			for(Writer writer : writters)
			{
				try {
					writer.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

	}
	
	private static List<Object> consumeAvailableMemory()
			throws Exception
			{
			    LinkedList<Object> holder = new LinkedList<Object>();
			    while (true)
			    {
			        try
			        {
			            holder.add(new byte[128 * 1024]);
			        }
			        catch (OutOfMemoryError ex)
			        {
			            holder.removeLast();
			            return holder;
			        }
			    }
			}
}
