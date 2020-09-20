package utils;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BrandQuantityPair implements Writable {
           
    private static final int DEFAULT_QUANTITY_VALUE = 0;
    private static final String DEFAULT_BRAND_VALUE = "";
  
    private int quantity;
    private String brand;  

    public BrandQuantityPair() {
        brand = DEFAULT_BRAND_VALUE;
        quantity = DEFAULT_QUANTITY_VALUE; 
    }
           
     public BrandQuantityPair(String brand, int quantity) {
         this.quantity = quantity;
         this.brand = brand;
     }
          
     public void write(DataOutput out) throws IOException {
         out.writeUTF(brand);
         out.writeInt(quantity);
     }

     public void readFields(DataInput in) throws IOException {
         brand = in.readUTF();
         quantity = in.readInt();
     } 

     public int getQuantity() {
         return this.quantity;
     }

     public String getBrand() {
         return this.brand;
     }

     @Override
     public String toString() {
         return brand + " " + quantity;
     }
}
