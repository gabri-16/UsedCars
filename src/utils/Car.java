package utils;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Car implements Writable {
           
          private static final int INT_DEFAULT = 0;
          private static final String STRING_DEFAULT = "";

          private String region;
          private int price;
          private String brand;
          private String fuel;
          private int odometer;          

          public Car() {
            region = STRING_DEFAULT;
            price = INT_DEFAULT;
            brand = STRING_DEFAULT;
            fuel = STRING_DEFAULT;
            odometer = INT_DEFAULT;
          }
           
          public Car(String region, int price, String brand, String fuel, int odometer) {
            this.region = region;
            this.price = price;
            this.brand = brand;
            this.fuel = fuel;
            this.odometer = odometer;
          }
          
          public void write(DataOutput out) throws IOException {
            out.writeUTF(region);
            out.writeInt(price);
            out.writeUTF(brand);
            out.writeUTF(fuel);
            out.writeInt(odometer);
          }

          public void readFields(DataInput in) throws IOException {
            region = in.readUTF();
            price = in.readInt();
            brand = in.readUTF();
            fuel = in.readUTF();
            odometer = in.readInt();
          } 

          public boolean containsMissingData() {
            return region.equals("") || price == 0 || brand.equals("") || fuel.equals("") || odometer == 0;
          }

          @Override
          public String toString() {
            return  region + " " + price + " " + brand + " " + fuel + " " + odometer;
          }
        }
