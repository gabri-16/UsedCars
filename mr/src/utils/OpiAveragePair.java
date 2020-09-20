package utils;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OpiAveragePair implements Writable {
           
    private static final int DEFAULT_QUANTITY_VALUE = 0;
    private static final double DEFAULT_AVERAGE_VALUE = 0.0;
  
    private int quantity;
    private double average;  

    public OpiAveragePair() {
        average = DEFAULT_AVERAGE_VALUE;
        quantity = DEFAULT_QUANTITY_VALUE; 
    }
           
     public OpiAveragePair(double average, int quantity) {
         this.quantity = quantity;
         this.average = average;
     }
          
     public void write(DataOutput out) throws IOException {
         out.writeDouble(average);
         out.writeInt(quantity);
     }

     public void readFields(DataInput in) throws IOException {
         average = in.readDouble();
         quantity = in.readInt();
     } 

     public int getQuantity() {
         return this.quantity;
     }

     public double getAverage() {
         return this.average;
     }

     @Override
     public String toString() {
         return average + " " + quantity;
     }
}
