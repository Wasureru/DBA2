import java.util.ArrayList;

public class Bucket
{
    private ArrayList<Record> records = new ArrayList<>();

    public ArrayList<Record> get_records()
    {
        return records;
    }

    public void add_record(Record record)
    {
        records.add(record);
    }
    
    public boolean is_full()
    {
        return records.size() == 59;
    }
    
    public int get_size()
    {
        return records.size();
    }
}
