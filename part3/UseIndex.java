import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class UseIndex implements dbimpl
{
    private int hash;
    private int page_num;
    private int offset;
    private boolean found = false;
    private final int num_buckets = 16384;
    
    public static void main(String args[])
    {
       UseIndex load = new UseIndex();

       // calculate query time
       long startTime = System.currentTimeMillis();
       load.readArguments(args);
       long endTime = System.currentTimeMillis();

       System.out.println("Query time: " + (endTime - startTime) + "ms");
    }

    // opens the hash file, seeks to the right bucket
    // and sends it off to be checked for the search key
    public void run(String key, int pagesize)
    {
        hash = get_hash(key);
        RandomAccessFile raf;
        try
        {
            raf = new RandomAccessFile("hash." + pagesize, "r");
            raf.seek(hash * pagesize);
            byte[] bucket = new byte[pagesize];
            raf.read(bucket);
            check_bucket(bucket, key);
        }
        catch (ArrayIndexOutOfBoundsException e)
        {
            if (!found)
            {
                check_overflow(pagesize, key);
            }
        }
        catch (FileNotFoundException e)
        {
            System.err.println(e.getMessage());
        }
        catch (IOException e)
        {
            System.err.println(e.getMessage());
        }
        if (found)
        {
            get_record(pagesize, key);
        }
    }

    // process the loaded bucket into individual records
    private void check_bucket(byte[] bucket, String key)
    {
        int next = 0;
        while (true)
        {
            byte[] key_bytes = new byte[12];
            byte[] page_num = new byte[4];
            byte[] offset = new byte[4];
            System.arraycopy(bucket, next, key_bytes, 0, 12);
            System.arraycopy(bucket, next + 12, page_num, 0, 4);
            System.arraycopy(bucket, next + 16, offset, 0, 4);
            if (check_key(key_bytes, page_num, offset, key))
            {
                found = true;
                break;
            }
            else
            {
                next += 20;
            }
        }
    }

    // if the search key was found in the hash, we now go to 
    // the correct page and offset in the heap and load the record
    private void get_record(int pagesize, String key)
    {
        RandomAccessFile raf;
        try
        {
            raf = new RandomAccessFile("heap." + pagesize, "r");
            raf.seek((page_num * pagesize) + (offset * RECORD_SIZE));
            byte[] record = new byte[RECORD_SIZE];
            raf.read(record);
            printRecord(record, key);
        }
        catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        
    }
    
    // if we havent found it in the bucket the search key 
    // points to, we check the overflow
    private void check_overflow(int pagesize, String key)
    {
        RandomAccessFile raf;
        try
        {
            raf = new RandomAccessFile("hash." + pagesize, "r");
            raf.seek(num_buckets * pagesize);
            byte[] bucket = new byte[pagesize];
            while (true)
            {
                int EOF = raf.read(bucket);
                if (EOF == -1)
                {
                    if (!found)
                    {
                        System.out.println("record doesnt exist");
                    }
                    break;
                }
                try
                {
                    check_bucket(bucket, key);
                }
                catch (ArrayIndexOutOfBoundsException e)
                {
                    //System.out.println(e.getMessage());
                }
            }            
        }
        catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

    }
    
    private boolean check_key(byte[] index, byte[] page_num, byte[] offset, String key)
    {
        String record = new String(index);
        String BN_NAME = record.substring(0, 12);
        if (key.startsWith(BN_NAME) || BN_NAME.startsWith(key) || key.equals(BN_NAME))
        {
            this.page_num = ByteBuffer.wrap(page_num).getInt();
            this.offset =  ByteBuffer.wrap(offset).getInt();
            return true;
        }
        return false;
    }

    private int get_hash(String key)
    {
        return key.hashCode() & 16383;
    }
    
    public void printRecord(byte[] rec, String input)
    {
        String record = new String(rec);
        String BN_NAME = get_field(record, RID_SIZE + REGISTER_NAME_SIZE, RID_SIZE + REGISTER_NAME_SIZE + BN_NAME_SIZE).trim();
        if (BN_NAME.toLowerCase().contains(input.toLowerCase()))
        {
            String BN_STATUS = get_field(record, BN_STATUS_OFFSET, BN_REG_DT_OFFSET).trim();
            String BN_REG_DT = get_field(record, BN_REG_DT_OFFSET, BN_CANCEL_DT_OFFSET).trim();
            String BN_CANCEL_DT = get_field(record, BN_CANCEL_DT_OFFSET, BN_RENEW_DT_OFFSET).trim();
            String BN_RENEW_DT = get_field(record, BN_RENEW_DT_OFFSET, BN_STATE_NUM_OFFSET).trim();
            String BN_STATE_NUM = get_field(record, BN_STATE_NUM_OFFSET, BN_STATE_OF_REG_OFFSET).trim();
            String BN_STATE_OF_REG = get_field(record, BN_STATE_OF_REG_OFFSET, BN_ABN_OFFSET).trim();
            String BN_ABN = get_field(record, BN_ABN_OFFSET, BN_ABN_OFFSET + BN_ABN_SIZE).trim();
            System.out.println(BN_NAME + " | "
                             + BN_STATUS + " | "
                             + BN_REG_DT + " | "
                             + BN_CANCEL_DT + " | " 
                             + BN_RENEW_DT + " | " 
                             + BN_STATE_NUM + " | " 
                             + BN_STATE_OF_REG + " | " 
                             + BN_ABN);
        }
    }
    
    private String get_field(String record, int start, int end)
    {
        String field = record.substring(start, end).trim();
        return field;
    }

    @Override
    public void readArguments(String args[])
    {
       if (args.length == 2)
       {
          if (isInteger(args[1]))
          {
             run(args[0], Integer.parseInt(args[1]));
          }
       }
       else
       {
           System.out.println("Error: only pass in two arguments");
       }
    }

    @Override
    public boolean isInteger(String s)
    {
       boolean isValidInt = false;
       try
       {
          Integer.parseInt(s);
          isValidInt = true;
       }
       catch (NumberFormatException e)
       {
          System.out.println("Usage: java UseIndex \"Some business name\" pagesize");
       }
       return isValidInt;
    }
}
