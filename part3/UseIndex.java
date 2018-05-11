import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class UseIndex
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
            raf.seek(hash * 12288);
            byte[] bucket = new byte[12288];
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
    }

    private void check_bucket(byte[] bucket)
    {
        int next = 0;
        while (true)
        {
            byte[] key = new byte[200];
            byte[] page_num = new byte[4];
            byte[] offset = new byte[4];
            System.arraycopy(bucket, next, key, 0, 200);
            System.arraycopy(bucket, next + 200, page_num, 0, 4);
            System.arraycopy(bucket, next + 204, offset, 0, 4);
            if (check_key(key, page_num, offset))
            {
                found = true;
                break;
            }
            else
            {
                next += 208;
            }
        }
    }

    private void check_overflow()
    {
        RandomAccessFile raf;
        try
        {
            raf = new RandomAccessFile("hash.4096", "r");
            raf.seek(64 * 12288);
            byte[] bucket = new byte[12288];
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
        String BN_NAME = record.substring(0, 200);
        if (BN_NAME.toLowerCase().contains(key.toLowerCase()))
        {
//            = ByteBuffer.wrap(bRid).getInt()
            this.page_num = ByteBuffer.wrap(page_num).getInt();
            this.offset =  ByteBuffer.wrap(offset).getInt();
            System.out.println(BN_NAME + " | " + this.page_num + " | " + this.offset);
            return true;
        }
        return false;
    }

    private int get_hash(String key)
    {
        return key.hashCode() & 63;
    }
}
