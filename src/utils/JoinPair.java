package utils;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class JoinPair implements Writable {
           
    private static final int DEFAULT_FLAG_VALUE = 0;
    private static final String DEFAULT_VALUE_VALUE = "";

    private int flag;
    private String value;  

    public JoinPair() {
        flag = DEFAULT_FLAG_VALUE;
        value = DEFAULT_VALUE_VALUE; 
    }
           
     public JoinPair(int flag, String value) {
         this.flag = flag;
         this.value = value;
     }
          
     public void write(DataOutput out) throws IOException {
         out.writeInt(flag);
         out.writeUTF(value);
     }

     public void readFields(DataInput in) throws IOException {
         flag = in.readInt();
         value = in.readUTF();
     } 

     public int getFlag() {
         return this.flag;
     }

     public String getValue() {
         return this.value;
     }

     @Override
     public String toString() {
         return flag + " " + value;
     }
}
