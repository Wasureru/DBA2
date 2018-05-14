import java.io.UnsupportedEncodingException;

public class Record
{
    private byte[] key = new byte[12];
    private final int page_num;
    private final int offset;
    
    public Record(byte[] key, int page_num, int offset) throws UnsupportedEncodingException
    {
        this.key = key;
        this.page_num = page_num;
        this.offset = offset;
    }
    
    public byte[] get_key()
    {
        return key;
    }
    
    public int get_page_num()
    {
        return page_num;
    }
    
    public int get_offset()
    {
        return offset;
    }
}
