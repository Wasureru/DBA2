import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class UseIndex implements dbimpl
{
    private final String key;
    private int hash;
    private int page_num;
    private int offset;
    private boolean found = false;
    
    public static void main(String args[])
    {
       UseIndex load = new UseIndex(args[0]);

       // calculate query time
       long startTime = System.currentTimeMillis();
       load.run();
       long endTime = System.currentTimeMillis();

       System.out.println("Query time: " + (endTime - startTime) + "ms");
    }

    public UseIndex(String key)
    {
        this.key = key;
    }
    
    public void run()
    {
        hash = get_hash(key);
        RandomAccessFile raf;
        try
        {
            raf = new RandomAccessFile("hash.4096", "r");
            raf.seek(hash * 4096);
            byte[] bucket = new byte[4096];
            raf.read(bucket);
            check_bucket(bucket);
        }
        catch (ArrayIndexOutOfBoundsException e)
        {
            if (!found)
            {
                check_overflow();
            }
        }
        catch (FileNotFoundException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        if (found)
        {
            get_record();
        }
    }

    private void check_bucket(byte[] bucket)
    {
        int next = 0;
        while (true)
        {
            byte[] key = new byte[12];
            byte[] page_num = new byte[4];
            byte[] offset = new byte[4];
            System.arraycopy(bucket, next, key, 0, 12);
            System.arraycopy(bucket, next + 12, page_num, 0, 4);
            System.arraycopy(bucket, next + 16, offset, 0, 4);
            if (check_key(key, page_num, offset))
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

    private void get_record()
    {
        RandomAccessFile raf;
        try
        {
            raf = new RandomAccessFile("heap.4096", "r");
            raf.seek((page_num * 4096) + ((offset - 1) * RECORD_SIZE));
            byte[] record = new byte[RECORD_SIZE];
            raf.read(record);
            printRecord(record, key);
        }
        catch (FileNotFoundException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }
    
    private void check_overflow()
    {
        RandomAccessFile raf;
        try
        {
            raf = new RandomAccessFile("hash.4096", "r");
            raf.seek(16384 * 4096);
            byte[] bucket = new byte[4096];
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
                    check_bucket(bucket);
                }
                catch (ArrayIndexOutOfBoundsException e)
                {
                    //System.out.println(e.getMessage());
                }
            }            
        }
        catch (FileNotFoundException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
    
    private boolean check_key(byte[] index, byte[] page_num, byte[] offset)
    {
        String record = new String(index);
        String BN_NAME = record.substring(0, 12);
        if (key.startsWith(BN_NAME) || BN_NAME.startsWith(key))
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
             //readHeap(args[0], Integer.parseInt(args[1]));
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
          e.printStackTrace();
       }
       return isValidInt;
    }
}
