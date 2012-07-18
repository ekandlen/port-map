package org.ek.mapper.util;

import java.io.PrintStream;

public class Log
{

	private static final boolean isDebug = System.getProperty("DEBUG") != null;

	private static boolean log(final PrintStream out, final String format, final Object... args)
	{
		final String message = format == null ? null : String.format(format, args);
		out.println(message);
		return true;
	}

	public static boolean debug(final String format, final Object... args)
	{
		return isDebug ? log(System.out, format, args) : true;
	}

	public static boolean log(final String format, final Object... args)
	{
		return log(System.out, format, args);
	}

	public static boolean err(final String format, final Object... args)
	{
		return log(System.err, format, args);
	}
}
