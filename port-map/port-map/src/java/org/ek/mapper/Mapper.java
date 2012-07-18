package org.ek.mapper;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.ek.mapper.util.ByteBufferPool;
import org.ek.mapper.util.IOUtils;
import org.ek.mapper.util.Log;

/**
 * Single mapper instance. Maintain all connections for single port.
 * 
 * @author eugene
 * 
 */
public class Mapper implements Runnable
{

	private final int port;

	// all active connections
	private final Map<Object, Connection> connections = new HashMap<Object, Connection>();

	private final ServerSocketChannel listenChannel;

	private final SocketAddress target;

	private final Selector selector;

	private boolean stop = false;

	public Mapper(final int port, final String targetHost, final int targetPort) throws IOException
	{
		this.port = port;
		listenChannel = ServerSocketChannel.open();
		selector = Selector.open();
		// resolve address here
		// to make sure we are ready to setup remote connection
		final InetAddress address = Inet4Address.getByName(targetHost);
		target = new InetSocketAddress(address, targetPort);
		Log.debug("pm: " + port + ", " + targetHost + ":" + targetPort);
		Log.debug("bb: %s", ByteBufferPool.Instance);
	}

	public Selector getSelector()
	{
		return selector;
	}

	public SocketAddress getTarget()
	{
		return target;
	}

	public void initialize() throws IOException
	{
		// listen to src port
		// start a new thread
		listenChannel.socket().bind(new InetSocketAddress(port));
		listenChannel.configureBlocking(false);
		listenChannel.register(selector, SelectionKey.OP_ACCEPT);
		final Thread thread = new Thread(this);
		thread.start();
	}

	/**
	 * Notify mapper connection is closed
	 */
	public void onClose(final Connection connection)
	{
		connections.values().remove(connection);
	}

	private void processSelector() throws IOException
	{
		// main loop
		// select channels
		// run channel
		final int count = selector.select(1000);
		if (count <= 0)
		{
			return; // no channels are ready
		}
		final Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
		while (iterator.hasNext())
		{
			final SelectionKey key = iterator.next();
			iterator.remove();

			boolean acceptable;
			boolean readable;
			boolean writable;
			synchronized (key)
			{
				if (!key.isValid())
				{
					continue;
				}
				acceptable = key.isAcceptable();
				readable = key.isReadable();
				writable = key.isWritable();
			}

			Log.debug("selected: %s", key.channel());
			if (acceptable)
			{
				final ServerSocketChannel channel = (ServerSocketChannel) key.channel();
				final Connection connection = new Connection(this, channel);
				// TODO: rate limit connections
				new Thread(connection).start();
			}
			else if (readable || writable)
			{
				// notify connection, it has channel to operate
				final Connection connection = connections.get(key.attachment());
				if (connection == null)
				{
					continue; // connection already closed
				}
				connection.transfer((SocketChannel) key.channel(), readable);
			}
			else
			{
				assert false : "not expected";
			}
		}
	}

	/**
	 * Register connection
	 * 
	 * @return connection key, which will be used to lookup channel owner
	 */
	public Object register(final Connection connection)
	{
		assert !connections.containsValue(connection) : "connection already registered: " + connection;

		final Object key = new Object();
		connections.put(key, connection);
		return key;
	}

	@Override
	public void run()
	{
		try
		{
			while (!stop)
			{
				processSelector();
			}
		}
		catch (final Throwable e)
		{
			e.printStackTrace();
		}
		finally
		{
			IOUtils.safeClose(listenChannel);
			IOUtils.safeClose(selector);
			for (final Connection connection : connections.values())
			{
				connection.destroy();
			}
		}
	}

	/**
	 * Shutdown mapper
	 */
	public void stop()
	{
		stop = true;
	}
}
