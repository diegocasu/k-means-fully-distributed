package it.unipi.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


public class IdTypePair implements WritableComparable {
    private long id; // Natural key
    private PointType type; // Secondary key
    
    
    public IdTypePair() {
        id = 0;
        type = PointType.DATA;
    }
    
    public IdTypePair(long id, PointType type) {
        this();
        this.set(id, type);
    }
    
    public void set(long id, PointType type) {
        this.id = id;
        this.type = type;
    }
    
    public long getId() {
        return this.id;
    }
    
    public PointType getType() {
        return this.type;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
        WritableUtils.writeEnum(out, type);   
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readLong();
        type = WritableUtils.readEnum(in, PointType.class);
    }

    // Controls the sort order of the keys, enabling secondary sorting.
    // Two IdTypePair with the same id must be ordered so that a DATA one comes before a MEAN point.
    // If they have a different id, the order is not important.
    @Override
    public int compareTo(Object o) {
        IdTypePair thatPair = (IdTypePair) o;
        int compareValue = ((Long) this.id).compareTo((Long) thatPair.getId());
        
        if (compareValue == 0) {
            if (this.type == thatPair.getType())
                compareValue = 0;
            else if (this.type == PointType.DATA)
                compareValue = -1;
            else if (this.type == PointType.MEAN)
                compareValue = 1;
        }
        
        return compareValue;
    }
}
