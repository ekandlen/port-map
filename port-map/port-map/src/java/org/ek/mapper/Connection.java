package org.ek.mapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.ek.mapper.util.ByteBufferPool;
import org.ek.mapper.util.IOUtils;
import org.ek.mapper.util.Log;

/**
 * Single connection. Maintains incoming and outgoing channels, transfers data
 * in both directions.
 * 
 * @author eugene
 * 
 */
public class Connection implements Runnable
{
	// maximum number of buffers to read from channel
	private static final int MAX_QUEUE_SIZE = 16;

	// shared pool for IO operations
	private static final ThreadPoolExecutor tp = new ThreadPoolExecutor(100, 1000, 60L, TimeUnit.SECONDS,
			new SynchronousQueue<Runnable>());

	private final ServerSocketChannel listenChannel;

	private final Mapper mapper;

	private SocketChannel targetChannel;
	private SocketChannel sourceChannel;

	// buffers to receive data
	// data read from source, that should be written to target channel
	private final BlockingQueue<ByteBuffer> targetWrite = new LinkedBlockingQueue<ByteBuffer>();

	// data read from target, that should be written to source channel
	private final BlockingQueue<ByteBuffer> sourceWrite = new LinkedBlockingQueue<ByteBuffer>();

	private SelectionKey targetKey;
	private SelectionKey sourceKey;

	// end of stream marks for each channel
	private boolean targetEoF = false;
	private boolean sourceEoF = false;

	// IO operation queue
	private final BlockingQueue<Runnable> tasks = new LinkedBlockingQueue<Runnable>();

	// true if IO operation is running
	private final AtomicBoolean running = new AtomicBoolean(false);

	public Connection(final Mapper mapper, final ServerSocketChannel channel)
	{
		this.mapper = mapper;
		this.listenChannel = channel;
	}

	@Override
	public void run()
	{
		// accept connection
		// connect to target host
		// register channels
		try
		{
			synchronized (listenChannel)
			{
				sourceChannel = listenChannel.accept();
			}
			if (sourceChannel == null)
			{
				return; // already accepted
			}
			sourceChannel.configureBlocking(false);

			targetChannel = SocketChannel.open(mapper.getTarget());
			if (!targetChannel.finishConnect())
			{
				Log.log("Failed to connect: %s", mapper.getTarget());
				stopConnection();
				return;
			}
			targetChannel.configureBlocking(false);

			final Object key = mapper.register(this);

			sourceKey = sourceChannel.register(mapper.getSelector(), SelectionKey.OP_READ, key);
			targetKey = targetChannel.register(mapper.getSelector(), SelectionKey.OP_READ, key);
		}
		catch (final IOException e)
		{
			Log.err("Failed to connect: %s: %s", mapper.getTarget(), e);
			e.printStackTrace();
			stopConnection();
		}
	}

	/**
	 * Channel is available for IO. transfer data to or from this channel
	 */
	public void transfer(final SocketChannel channel, final boolean readable)
	{
		if (!channel.isConnected())
		{
			return; // avoid errors
		}
		assert channel == sourceChannel | channel == targetChannel : "unknown channel: " + channel;

		// data is from/to source channel
		final boolean incoming = channel == sourceChannel;

		final Runnable task;
		final Queue<ByteBuffer> queue;

		if (readable)
		{
			// stop reading, read task will resume read
			setSingleOp(incoming ? targetKey : sourceKey, SelectionKey.OP_READ, true);

			// we read into another channel's queue
			queue = incoming ? targetWrite : sourceWrite;
			if (incoming && targetEoF || !incoming && sourceEoF)
			{
				return; // channel already closed
			}
			if (queue.size() > MAX_QUEUE_SIZE)
			{
				return; // we have read enough for this channel, delay read
			}
			// ready to read
			task = new Runnable()
			{
				@Override
				public void run()
				{
					read(incoming);
				}
			};
		}
		else
		{
			// stop writing, write task will resume write
			setSingleOp(incoming ? sourceKey : targetKey, SelectionKey.OP_WRITE, true);
			// we write from this channel's queue
			queue = incoming ? sourceWrite : targetWrite;
			if (queue.isEmpty())
			{
				return; // nothing to write
			}
			// ready to write
			task = new Runnable()
			{
				@Override
				public void run()
				{
					write(incoming);
				}

			};
		}
		executeTask(task);
	}

	private void read(final boolean incoming)
	{
		final SocketChannel channel = incoming ? sourceChannel : targetChannel;
		final BlockingQueue<ByteBuffer> queue = incoming ? targetWrite : sourceWrite;
		ByteBuffer buffer = null;
		try
		{
			if (incoming && sourceEoF || !incoming && targetEoF)
			{
				return; // connection closed
			}
			if (!channel.isConnected())
			{
				return;
			}
			if (queue.size() > MAX_QUEUE_SIZE)
			{
				return; // delay data until we write all buffers
			}
			buffer = ByteBufferPool.Instance.allocate();
			final int numRead = channel.read(buffer);
			Log.debug("read: %d", numRead);
			if (numRead <= 0)
			{
				if (numRead == 0)
				{
					return;
				}
				// end of stream
				if (incoming)
				{
					sourceEoF = true;
				}
				else
				{
					targetEoF = true;
				}
				return;
			}
			buffer.flip();
			queue.add(buffer); // add to write queue
			buffer = null; // forget it to not return into pool
			// allow write to another channel
			setSingleOp(incoming ? targetKey : sourceKey, SelectionKey.OP_WRITE, false);
			// allow read
			setSingleOp(incoming ? targetKey : sourceKey, SelectionKey.OP_READ, false);
		}
		catch (final IOException e)
		{
			Log.err("read error: %s %s", channel, e.getMessage());
			e.printStackTrace();
			stopConnection();
		}
		finally
		{
			if (buffer != null)
			{
				// make sure we return buffer into pool
				ByteBufferPool.Instance.free(buffer);
			}
		}
	}

	private void write(final boolean incoming)
	{
		// write all buffers, while channel is ready for write
		final SocketChannel channel = incoming ? sourceChannel : targetChannel;
		final Queue<ByteBuffer> queue = incoming ? sourceWrite : targetWrite;
		synchronized (channel)
		{
			try
			{
				if (!channel.isConnected())
				{
					return;
				}
				ByteBuffer buffer;
				// lookup data, leave in queue
				while ((buffer = queue.peek()) != null)
				{
					final int numWrite = channel.write(buffer);
					Log.debug("write: %d", numWrite);
					if (buffer.hasRemaining())
					{
						// stop write, wait until channel is ready
						break;
					}
					queue.poll(); // remove buffer from queue
					ByteBufferPool.Instance.free(buffer);
				}
				if (!queue.isEmpty())
				{
					// we have more data to write, resume when channel is ready
					setSingleOp(incoming ? sourceKey : targetKey, SelectionKey.OP_WRITE, false);
				}
			}
			catch (final IOException e)
			{
				Log.err("write error: %s %s", channel, e.getMessage());
				e.printStackTrace();
				stopConnection();
			}
		}
	}

	/**
	 * Add or remove option from selection key
	 * 
	 * @param key
	 *            target key
	 * @param target
	 *            target option, ex: SelectionKey.OP_READ
	 * @param clean
	 *            true to remove option
	 */
	private void setSingleOp(final SelectionKey key, final int target, final boolean clean)
	{
		if (key == null)
		{
			return;
		}
		synchronized (key)
		{
			if (!key.isValid())
			{
				return;
			}
			int ops = key.interestOps();
			if (clean)
			{
				ops &= ~target; // clean option key
			}
			else
			{
				ops |= target; // add option key
			}
			key.interestOps(ops);
		}
	}

	private void executeTask(final Runnable task)
	{
		tasks.add(task);
		wakeUpExecutor();
	}

	private void wakeUpExecutor()
	{
		checkEoF();
		synchronized (tasks)
		{
			if (tasks.isEmpty())
			{
				Log.debug("no tasks");
				return; // nothing to execute
			}
			if (running.getAndSet(true))
			{
				// System.err.println("already running 2");
				Log.debug("already running");
				return; // already running
			}
		}
		tp.submit(new Runnable()
		{

			@Override
			public void run()
			{
				try
				{
					Runnable task;
					while ((task = tasks.poll()) != null)
					{
						try
						{
							task.run();
						}
						catch (final Throwable t)
						{
							t.printStackTrace();
						}
					}
				}
				finally
				{
					running.set(false);
					wakeUpExecutor();
				}
			}
		});
	}

	private void checkEoF()
	{
		// check if channel closed connection, clear it's buffers
		if (targetEoF && !targetWrite.isEmpty())
		{
			ByteBufferPool.Instance.free(targetWrite);
		}
		if (sourceEoF && !sourceWrite.isEmpty())
		{
			ByteBufferPool.Instance.free(sourceWrite);
		}
		if (targetEoF && sourceWrite.isEmpty() || sourceEoF && targetWrite.isEmpty())
		{
			// one side closed connection and we have nothing to write
			stopConnection();
		}
	}

	private void stopConnection()
	{
		// shutdown
		// notify mapper, we are closed now
		destroy();
		mapper.onClose(this);
	}

	/**
	 * Shutdown connection, cleanup resources
	 */
	public void destroy()
	{
		tasks.clear(); // drop tasks

		targetEoF = true;
		sourceEoF = true;
		// free buffers
		ByteBufferPool.Instance.free(targetWrite);
		ByteBufferPool.Instance.free(sourceWrite);
		// close
		if (sourceKey != null)
		{
			sourceKey.cancel();
		}
		if (targetKey != null)
		{
			targetKey.cancel();
		}
		IOUtils.safeClose(targetChannel);
		IOUtils.safeClose(sourceChannel);
	}
}
