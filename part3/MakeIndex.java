import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class MakeIndex
{
    private Bucket[] table = new Bucket[64];
    private ArrayList<Record> overflow_bucket = new ArrayList<>();
    private ArrayList<ArrayList<Record>> overflow = new ArrayList<>();
    
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
                if (overflow_bucket.size() == 59)
                {
                    overflow.add(overflow_bucket);
                    overflow_bucket.clear();
                }
                overflow_bucket.add(record);
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
    
    public void write_hash()
    {
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
                }
                eofByteAddOn(fos, table[table_index].get_size());
                }
                catch(NullPointerException e) { eofByteAddOn(fos, 0);}
            }
            for (ArrayList<Record> bucket : overflow)
            {
                for (Record record : bucket)
                {
                    System.out.println(record.get_key());
                    output = create_record_for_print(output, record);
                    fos.write(output);
                }
                eofByteAddOn(fos, bucket.size());
            }
            for (Record record : overflow_bucket)
            {
                output = create_record_for_print(output, record);
                fos.write(output);
            }
            eofByteAddOn(fos, overflow_bucket.size());

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
        offset.putInt(record.get_page_num());
        byte[] key = record.get_key().getBytes("utf-8");
        System.arraycopy(key, 0, output, 0, key.length);
        System.arraycopy(page_num.array(), 0, output, key.length, 4);
        System.arraycopy(offset.array(), 0, output, (key.length + page_num.array().length), 4);
        
        return output;
    }
    
    public void eofByteAddOn(FileOutputStream fos, int r_num) 
            throws IOException
     {
        byte[] fPadding = new byte[12288-(208*r_num)];
        fos.write(fPadding);
     }

}
