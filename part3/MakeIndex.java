import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class MakeIndex
{
    private Bucket[] table = new Bucket[64];
    private Bucket overflow_bucket = new Bucket();
    private ArrayList<Bucket> overflow = new ArrayList<Bucket>();
    private int count = 0;
    private int incount = 0;
    
    public void add_record(String key, int page_num, int offset)
    {
        int hash = get_hash(key);
        Record record = new Record(key, page_num, offset);
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
        return key.hashCode() & 63;
    }
    
    private void sizes()
    {
        int os = 0;
        int ms = 0;
        for (Bucket a : overflow)
        {
            System.out.println(a.get_size());
            os += a.get_size();
        }
        os += overflow_bucket.get_size();
        for (int index = 0; index < table.length; ++index)
        {
            if (table[index] != null)
            ms += table[index].get_size();
        }
        int total = os + ms;
        System.out.printf("in overflow %d| in main %d| total %d\n", os, ms, total);
    }
    
    public void write_hash()
    {
        //sizes();
        File hashfile = new File("hash." + 4096);
        FileOutputStream fos = null;
        byte[] output = new byte[208];
        try
        {
            fos = new FileOutputStream(hashfile);
            for (int table_index = 0; table_index < table.length; ++table_index)
            {
                try {
                for (Record record : table[table_index].get_records())
                {
                    output = create_record_for_print(output, record);
                    fos.write(output);
                    count++;
                }
                eofByteAddOn(fos, table[table_index].get_size());
                }
                catch(NullPointerException e) { eofByteAddOn(fos, 0);}
            }
            for (int index = 0; index < overflow.size(); ++index)
            {
                Bucket bucket = overflow.get(index);
                for (Record record : bucket.get_records())
                {
                    //System.out.println(record.get_key());
                    output = create_record_for_print(output, record);
                    fos.write(output);
                    count++;
                }
                eofByteAddOn(fos, bucket.get_size());
            }
            for (Record record : overflow_bucket.get_records())
            {
                output = create_record_for_print(output, record);
                fos.write(output);
                count++;
            }
            eofByteAddOn(fos, overflow_bucket.get_size());
            System.out.println(count);
            System.out.println(incount);

        }
        catch (IOException e)
        {
            System.err.println("Stream Error: " + e.getMessage());
        }
    }
    
    private byte[] create_record_for_print(byte[] output, Record record) throws UnsupportedEncodingException
    {
        ByteBuffer page_num = ByteBuffer.allocate(4);
        ByteBuffer offset = ByteBuffer.allocate(4);
        page_num.putInt(record.get_page_num());
        offset.putInt(record.get_offset());
        copy(record.get_key(), 200, 0, output);
        System.arraycopy(page_num.array(), 0, output, 200, 4);
        System.arraycopy(offset.array(), 0, output, 204, 4);
        
        incount++;
        if (record.get_key().toLowerCase().contains("RUSSELL CORPORATE ADVISORY CLUB CONSULTING".toLowerCase()))
        {
            //System.out.println(record.get_key() + "|" + record.get_page_num() + "|" + record.get_offset());
        }
        return output;
    }
    
    public void copy(String entry, int SIZE, int DATA_OFFSET, byte[] rec) throws UnsupportedEncodingException
    {
        byte[] DATA = new byte[SIZE];
        byte[] DATA_SRC = entry.trim().getBytes("utf-8");
        if (entry != "")
        {
            System.arraycopy(DATA_SRC, 0, DATA, 0, DATA_SRC.length);
        }
        System.arraycopy(DATA, 0, rec, DATA_OFFSET, DATA.length);
    }

    
    public void eofByteAddOn(FileOutputStream fos, int r_num) 
            throws IOException
     {
        byte[] fPadding = new byte[12288-(208*r_num)];
        fos.write(fPadding);
     }

}
