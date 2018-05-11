
public class Record
{
    private final String key;
    private final int page_num;
    private final int offset;
    
    public Record(String key, int page_num, int offset)
    {
        this.key = key;
        this.page_num = page_num;
        this.offset = offset;
    }
    
    public String get_key()
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
