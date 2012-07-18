package org.ek.mapper;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.ek.mapper.util.Log;

/**
 * Port-mapper launcher.
 * 
 * @author ekandlen
 */
public class PortMapper
{

	public static void main(final String[] args) throws IOException
	{
		// read and parse properties
		// create mappers
		// run all mappers
		final Map<Integer, Mapper> mappers = new HashMap<Integer, Mapper>();
		final Properties properties = new Properties();
		properties.load(new FileInputStream("proxy.properties"));

		for(final String key : properties.stringPropertyNames())
		{
			final int lpIndex = key.lastIndexOf(".localPort");
			if(lpIndex < 0)
			{
				continue; // unknown property name, skip
			}
			final String section = key.substring(0, lpIndex);
			final String lpValue = properties.getProperty(key);
			final String host = properties.getProperty(section + ".remoteHost");
			final String rpValue = properties.getProperty(section + ".remotePort");
			if((host == null) || (rpValue == null))
			{
				Log.err("Remote address not configured for %s.", section);
				System.exit(1);
			}
			final int localPort = Integer.valueOf(lpValue);
			final int remotePort = Integer.valueOf(rpValue);
			if(mappers.containsKey(localPort))
			{
				Log.err("local port %s[%s] already used.", localPort, section);
				System.exit(2);
			}
			// XXX validate port numbers?
			mappers.put(localPort, new Mapper(localPort, host, remotePort));
		}
		for(final Mapper mapper : mappers.values())
		{
			mapper.initialize();
		}
	}

}
