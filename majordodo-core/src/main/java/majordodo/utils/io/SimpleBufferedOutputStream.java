package majordodo.utils.io;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import majordodo.utils.SystemPropertyUtils;

/**
 * Versione di java.io.BufferedOutputStream final e non sincronizzata
 *
 * @author enrico.olivelli
 */
public final class SimpleBufferedOutputStream extends FilterOutputStream {

    protected byte buf[];
    protected int count;

    private static final int COPY_BUFFER_SIZE = SystemPropertyUtils.getIntSystemProperty("majordodo.io.simplebufferstream.buffersize", 64 * 1024);

    public SimpleBufferedOutputStream(OutputStream out) {
        this(out, COPY_BUFFER_SIZE);
    }

    public SimpleBufferedOutputStream(OutputStream out, int size) {
        super(out);
        if (size <= 0) {
            throw new IllegalArgumentException("Buffer size <= 0");
        }
        buf = new byte[size];
    }

    private void flushBuffer() throws IOException {
        if (count > 0) {
            out.write(buf, 0, count);
            count = 0;
        }
    }

    /**
     * Writes the specified byte to this buffered output stream.
     *
     * @param b the byte to be written.
     * @exception IOException if an I/O error occurs.
     */
    @Override
    public void write(int b) throws IOException {
        if (count >= buf.length) {
            flushBuffer();
        }
        buf[count++] = (byte) b;
    }

    /**
     * Writes <code>len</code> bytes from the specified byte array starting at
     * offset <code>off</code> to this buffered output stream.
     *
     * <p>
     * Ordinarily this method stores bytes from the given array into this
     * stream's buffer, flushing the buffer to the underlying output stream as
     * needed. If the requested length is at least as large as this stream's
     * buffer, however, then this method will flush the buffer and write the
     * bytes directly to the underlying output stream. Thus redundant
     * <code>BufferedOutputStream</code>s will not copy data unnecessarily.
     *
     * @param b the data.
     * @param off the start offset in the data.
     * @param len the number of bytes to write.
     * @exception IOException if an I/O error occurs.
     */
    @Override
    public void write(byte b[], int off, int len) throws IOException {
        if (len >= buf.length) {
            /* If the request length exceeds the size of the output buffer,
             flush the output buffer and then write the data directly.
             In this way buffered streams will cascade harmlessly. */
            flushBuffer();
            out.write(b, off, len);
            return;
        }
        if (len > buf.length - count) {
            flushBuffer();
        }
        System.arraycopy(b, off, buf, count, len);
        count += len;
    }

    /**
     * Flushes this buffered output stream. This forces any buffered output
     * bytes to be written out to the underlying output stream.
     *
     * @exception IOException if an I/O error occurs.
     * @see java.io.FilterOutputStream#out
     */
    @Override
    public void flush() throws IOException {
        flushBuffer();
        out.flush();
    }
}
