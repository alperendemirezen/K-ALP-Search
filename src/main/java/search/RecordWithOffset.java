package search;

public class RecordWithOffset implements Comparable<RecordWithOffset> {
    private long offset;
    private String jsonValue;

    public RecordWithOffset(long offset, String jsonValue) {
        this.offset = offset;
        this.jsonValue = jsonValue;
    }

    public long getOffset() {
        return offset;
    }

    public String getJsonValue() {
        return jsonValue;
    }

    @Override
    public int compareTo(RecordWithOffset o) {
        return Long.compare(this.offset, o.offset);
    }
}