package org.ek.mapper.util;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Simple fixed size buffer pool.
 * 
 * @author eugene
 * 
 */
public class ByteBufferPool
{
	// single buffer size
	private static final int BUFFER_SIZE = 32 * 1024;

	private static final int BUFFERS_NUMBER = 2048;

	public static final ByteBufferPool Instance = new ByteBufferPool();

	private final LinkedBlockingQueue<ByteBuffer> queue = new LinkedBlockingQueue<ByteBuffer>();

	private ByteBufferPool()
	{
		// Add bunch of buffers in pool
		for (int i = 0; i < BUFFERS_NUMBER; i++)
		{
			createBuffer();
		}
	}

	/**
	 * Add single buffer into pool
	 */
	private void createBuffer()
	{
		final ByteBuffer buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
		queue.add(buffer);
	}

	/**
	 * Borrow buffer from pool. Make sure you use {@link #free(ByteBuffer)} when
	 * buffer is not used any more
	 * 
	 * @return ByteBuffer from memory pool
	 */
	public ByteBuffer allocate()
	{
		ByteBuffer buffer = null;
		try
		{
			// XXX wait for free buffer or allocate a new buffer?
			while ((buffer = queue.poll(1000, TimeUnit.MILLISECONDS)) == null)
			{
				// wait until we get free buffer
			}
		}
		catch (final InterruptedException e)
		{
			// ignore
		}
		Log.debug("allocate: %s, %s", queue.size(), buffer);
		if (buffer == null)
		{
			Log.err("Out of memory error.");
			assert false;
			return null;
		}
		buffer.clear(); // reset buffer
		return buffer;
	}

	/**
	 * Return buffer to memory pool
	 * 
	 * @param buffer
	 *            borrowed with {@link #allocate()} byte buffer
	 */
	public void free(final ByteBuffer buffer)
	{
		queue.add(buffer); // return
		Log.debug("free: %s, %s", queue.size(), buffer);
	}

	/**
	 * Return all buffers to memory pool
	 * 
	 * @param source
	 *            collection of buffers borrowed with {@link #allocate()}
	 */
	public void free(final BlockingQueue<ByteBuffer> source)
	{
		Log.debug("free: %s, %s", queue.size(), source.size());
		source.drainTo(queue);
	}
}
