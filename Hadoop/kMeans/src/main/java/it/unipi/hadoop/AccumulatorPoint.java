package it.unipi.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class AccumulatorPoint extends Point {
    private long numberOfPoints;
    
    public AccumulatorPoint() {
        super();
        this.numberOfPoints = 0;
    }
    
    public long getNumberOfPoints() {
        return numberOfPoints;
    }
    
    public void add(Point p) {
        if (numberOfPoints == 0)
            super.set(p);
        else
            super.add(p);
        
        numberOfPoints++;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeLong(numberOfPoints);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        numberOfPoints = in.readLong();
    }
}
