import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class MakeIndex
{
    private final int         num_buckets     = 16384;
    private final int         key_size        = 12;
    private final int         int_size        = 4;
    private Bucket[]          table           = new Bucket[num_buckets];
    private Bucket            overflow_bucket = new Bucket();
    private ArrayList<Bucket> overflow        = new ArrayList<Bucket>();
    private int               count           = 0;
    private int               incount         = 0;
    private final int         record_size     = 20;

    public void add_record(String key, int page_num, int offset)
    {
        int hash = get_hash(key);
        byte[] DATA_SRC = null;
        byte[] temp = new byte[key_size];
        Record record = null;
        try
        {
            DATA_SRC = key.trim().getBytes("utf-8");
            copy(DATA_SRC, 12, 0, temp);
            record = new Record(DATA_SRC, page_num, offset);
        }
        catch (UnsupportedEncodingException e)
        {
            System.err.println(e.getMessage());
        }
        if (table[hash] == null)
        {
            Bucket new_bucket = new Bucket();
            new_bucket.add_record(record);
            table[hash] = new_bucket;
        }
        else
        {
            if (table[hash].is_full())
            {
                if (overflow_bucket.is_full())
                {
                    Bucket bucket = overflow_bucket;
                    overflow.add(bucket);
                    overflow_bucket = new Bucket();
                }
                overflow_bucket.add_record(record);
            }
            else
            {
                table[hash].add_record(record);
            }
        }
    }

    public int get_hash(String key)
    {
        return key.hashCode() & num_buckets - 1;
    }

    // write the hash file - iterate over the main array
    // the overflow array and the final overflow bucket
    // printing all to file
    public void write_hash(int pagesize)
    {
        File hashfile = new File("hash." + pagesize);
        FileOutputStream fos = null;
        byte[] output = new byte[record_size];
        try
        {
            fos = new FileOutputStream(hashfile);
            for (int table_index = 0; table_index < table.length; ++table_index)
            {
                try
                {
                    for (Record record : table[table_index].get_records())
                    {
                        output = create_record_for_print(output, record);
                        fos.write(output);
                        count++;
                    }
                    eofByteAddOn(fos, table[table_index].get_size(), pagesize);
                }
                catch (NullPointerException e)
                {
                    eofByteAddOn(fos, 0, pagesize);
                }
            }
            for (int index = 0; index < overflow.size(); ++index)
            {
                Bucket bucket = overflow.get(index);
                for (Record record : bucket.get_records())
                {
                    // System.out.println(record.get_key());
                    output = create_record_for_print(output, record);
                    fos.write(output);
                    count++;
                }
                eofByteAddOn(fos, bucket.get_size(), pagesize);
            }
            for (Record record : overflow_bucket.get_records())
            {
                output = create_record_for_print(output, record);
                fos.write(output);
                count++;
            }
            eofByteAddOn(fos, overflow_bucket.get_size(), pagesize);
            System.out.println(count);
            System.out.println(incount);
        }
        catch (IOException e)
        {
            System.err.println("Stream Error: " + e.getMessage());
        }
    }

    // extract the info from the record and put it into byte arrays for printing
    private byte[] create_record_for_print(byte[] output, Record record) throws UnsupportedEncodingException
    {
        ByteBuffer page_num = ByteBuffer.allocate(int_size);
        ByteBuffer offset = ByteBuffer.allocate(int_size);
        page_num.putInt(record.get_page_num());
        offset.putInt(record.get_offset());
        copy(record.get_key(), key_size, 0, output);
        System.arraycopy(page_num.array(), 0, output, key_size, int_size);
        System.arraycopy(offset.array(), 0, output, key_size + int_size, int_size);
        return output;
    }

    public void copy(byte[] entry, int SIZE, int DATA_OFFSET, byte[] rec) throws UnsupportedEncodingException
    {
        byte[] DATA = new byte[SIZE];
        if (entry != null)
        {
            if (entry.length > key_size)
            {
                System.arraycopy(entry, 0, DATA, 0, key_size);
            }
            else
            {
                System.arraycopy(entry, 0, DATA, 0, entry.length);
            }
        }
        System.arraycopy(DATA, 0, rec, DATA_OFFSET, DATA.length);
    }

    public void eofByteAddOn(FileOutputStream fos, int r_num, int pagesize) throws IOException
    {
        byte[] fPadding = new byte[pagesize - (record_size * r_num)];
        fos.write(fPadding);
    }
}
