package it.unipi.hadoop;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;


public class Point implements WritableComparable {
    private ArrayList<Double> coordinates;
    private PointType type;
    private long id;

    public Point() {
        coordinates = new ArrayList();
        type = PointType.DATA;
        id = 0;
    }
    
    public Point(ArrayList<Double> coordinates, PointType type, long id) {
        this();       
        this.set(coordinates, type, id);
    }

    public void set(ArrayList<Double> coordinates, PointType type, long id) {
        this.coordinates = coordinates;
        this.type = type;
        this.id = id;
    }
    
    public void set(Point p) {
        this.set(p.getCoordinates(), p.getType(), p.getId());
    }
    
    public ArrayList<Double> getCoordinates() {
        return this.coordinates;
    }
    
    public long getId() {
        return this.id;
    }
    
    public PointType getType() {
        return this.type;
    }
    
    public int getNumberOfDimensions() {
        return this.coordinates.size();
    }
    
    public double getSquaredDistance(Point that) {
        if (this.getNumberOfDimensions() != that.getNumberOfDimensions()) {
            System.err.println("The points " + this.toString() + " " + that.toString() + " have different dimensions. The distance is not defined.");
            return -1;
        }
        
        double sum = 0;
        ArrayList<Double> thisCoordinates = this.getCoordinates();
        ArrayList<Double> thatCoordinates = that.getCoordinates();

        for (int i = 0; i < thisCoordinates.size(); i++){
            sum += (thisCoordinates.get(i) - thatCoordinates.get(i))*(thisCoordinates.get(i) - thatCoordinates.get(i));
        }

        return sum;
    }

    public void add(Point that) {
        if (this.getNumberOfDimensions() != that.getNumberOfDimensions()) {
            System.err.println("The points " + this.toString() + " " + that.toString() + " have different dimensions. The sum is not defined.");
            return;
        }
        
        ArrayList<Double> thisCoordinates = this.getCoordinates();
        ArrayList<Double> thatCoordinates = that.getCoordinates();

        for (int i = 0; i < thisCoordinates.size(); i++){
            thisCoordinates.set(i, thisCoordinates.get(i) + thatCoordinates.get(i));
        }
    }
 
    public void div(long n) {
        for (int i = 0; i < coordinates.size(); i++){
            coordinates.set(i, coordinates.get(i)/n);
        }
    }

    public boolean isMean() {
        return this.type == PointType.MEAN;
    }
    
    public boolean isData() {
        return this.type == PointType.DATA;
    }

    public Point copy() {  
        return new Point(this.coordinates, this.type, this.id);
    }

    @Override
    public void write(DataOutput out) throws IOException {       
        out.writeInt(coordinates.size());
        for (Double c: coordinates)
            out.writeDouble(c);
        
        WritableUtils.writeEnum(out, type);
        out.writeLong(id);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();

        coordinates = new ArrayList<>();
        for (int i = 0; i < size; i++)
            coordinates.add(in.readDouble());
        
        type = WritableUtils.readEnum(in, PointType.class);
        id = in.readLong();
    }
    
    @Override
    public String toString() {
        String[] coordinatesString = new String[this.coordinates.size()];
        
        for (int i = 0; i < coordinatesString.length; i++)
            coordinatesString[i] = Double.toString(this.coordinates.get(i));
        
        return this.type.toString() + "," + this.id + "," + String.join(",", coordinatesString);        
    }

    @Override
    public int compareTo(Object o) {
        Point thatPoint = (Point) o;
        int compareId = ((Long) this.id).compareTo((Long) thatPoint.getId());
        
        if (compareId == 0) {
            if (this.type == thatPoint.getType()) {
                
                for (int i = 0; i < this.coordinates.size(); i++) {
                    if (this.coordinates.get(i) < thatPoint.getCoordinates().get(i)){
                        return -1;
                    }

                    if (this.coordinates.get(i) > thatPoint.getCoordinates().get(i)) {
                        return 1;
                    }
                }

                return 0;
                
            } else if (this.type == PointType.DATA) {
                return -1;
            } else if (this.type == PointType.MEAN) {
                return 1;
            }
        }
        return compareId;
    }
    
    @Override
    public int hashCode() {
        return new Text(this.toString()).hashCode();
    }
    
    public static Point parse(String value){
        String[] valueElements = value.split(",");
        
        PointType parsedType = PointType.valueOf(valueElements[0]);
        long parsedId = Long.valueOf(valueElements[1]);
        ArrayList<Double> parsedCoordinates = new ArrayList();

        for (int i = 2; i < valueElements.length; i++) {
            parsedCoordinates.add(Double.parseDouble(valueElements[i]));
        }
        
        return new Point(parsedCoordinates, parsedType, parsedId);
    }
}