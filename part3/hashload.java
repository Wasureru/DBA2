import java.nio.ByteBuffer;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.File;
import java.io.FileInputStream;

public class hashload implements dbimpl
{
    // object for creating the hash index while heap is being built
    MakeIndex hi = new MakeIndex();

    // initialize
    public static void main(String args[])
    {
        hashload load = new hashload();
        // calculate query time
        long startTime = System.currentTimeMillis();
        load.readArguments(args);
        long endTime = System.currentTimeMillis();
        System.out.println("Query time: " + (endTime - startTime) + "ms");
    }

    // reading command line arguments
    public void readArguments(String args[])
    {
        if (args.length == 1)
        {
            if (isInteger(args[0]))
            {
                readHeap(Integer.parseInt(args[0]));
            }
        }
        else
        {
            System.out.println("Error: only pass in one argument");
        }
    }

    // check if pagesize is a valid integer
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
            System.out.println("just a page size, thanks");
            System.exit(0);
        }
        return isValidInt;
    }

    // read heapfile by page
    public void readHeap(int pagesize)
    {
        File heapfile = new File(HEAP_FNAME + pagesize);
        int intSize = 4;
        int pageCount = 0;
        int recCount = 0;
        int recordLen = 0;
        int rid = 0;
        boolean isNextPage = true;
        boolean isNextRecord = true;
        try
        {
            FileInputStream fis = new FileInputStream(heapfile);
            // reading page by page
            while (isNextPage)
            {
                byte[] bPage = new byte[pagesize];
                byte[] bPageNum = new byte[intSize];
                fis.read(bPage, 0, pagesize);
                System.arraycopy(bPage, bPage.length - intSize, bPageNum, 0, intSize);
                // reading by record, return true to read the next record
                isNextRecord = true;
                while (isNextRecord)
                {
                    byte[] bRecord = new byte[RECORD_SIZE];
                    byte[] bRid = new byte[intSize];
                    try
                    {
                        System.arraycopy(bPage, recordLen, bRecord, 0, RECORD_SIZE);
                        System.arraycopy(bRecord, 0, bRid, 0, intSize);
                        rid = ByteBuffer.wrap(bRid).getInt();
                        if (rid != recCount)
                        {
                            isNextRecord = false;
                        }
                        else
                        {
                            send_to_hash(bRecord, pageCount, recCount);
                            recordLen += RECORD_SIZE;
                        }
                        recCount++;
                        // if recordLen exceeds pagesize, catch this to reset to next page
                    }
                    catch (ArrayIndexOutOfBoundsException e)
                    {
                        isNextRecord = false;
                        recordLen = 0;
                        recCount = 0;
                        rid = 0;
                    }
                }
                // check to complete all pages
                if (ByteBuffer.wrap(bPageNum).getInt() != pageCount)
                {
                    isNextPage = false;
                }
                pageCount++;
            }
            // write the hash when all records have been processed
            hi.write_hash(pagesize);
        }
        catch (FileNotFoundException e)
        {
            System.out.println("File: " + HEAP_FNAME + pagesize + " not found.");
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    // returns records containing the argument text from shell
    public void send_to_hash(byte[] rec, int page_num, int offset)
    {
        String record = new String(rec);
        String BN_NAME = record.substring(RID_SIZE + REGISTER_NAME_SIZE, RID_SIZE + REGISTER_NAME_SIZE + BN_NAME_SIZE);
        // send the record off to be added to the hash index
        hi.add_record(BN_NAME.trim(), page_num, offset);
    }
}
