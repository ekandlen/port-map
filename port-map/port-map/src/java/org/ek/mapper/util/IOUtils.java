package org.ek.mapper.util;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.Selector;

public class IOUtils
{
	public static void safeClose(final Closeable closable)
	{
		if (closable == null)
		{
			return;
		}
		try
		{
			closable.close();
		}
		catch (final IOException e)
		{
			e.printStackTrace();
		}
	}

	public static void safeClose(final Selector closable)
	{
		if (closable == null)
		{
			return;
		}
		try
		{
			closable.close();
		}
		catch (final IOException e)
		{
			e.printStackTrace();
		}
	}
}
