import com.google.common.collect.Lists;
import com.google.errorprone.annotations.Var;
import org.apache.arrow.gandiva.evaluator.Filter;
import org.apache.arrow.gandiva.evaluator.SelectionVectorInt16;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.expression.Condition;
import org.apache.arrow.gandiva.expression.TreeBuilder;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.nio.charset.CharacterCodingException;
import java.util.List;

public class Start {

    public static void main(String[] args1) throws CharacterCodingException, GandivaException {

        /*
        System.out.println("HELLO WORLD");


        //Vektor wird erstellt
        RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);

       //Integer in 1d Vektor gespeichert

        IntVector vector = new IntVector("Euro", allocator);

        //Vektor wird allocated

        vector.allocateNew(1);

        //Vektor Mutation
        vector.set(1,25);

        //Set Value count
        vector.setValueCount(1);
        int value = vector.get(1);
        System.out.println(value);

        vector.close();



        // String in 1-D Vector gespeichert

        VarCharVector vcVector = new VarCharVector("vctest", allocator);
        vcVector.allocateNew();
        String test ="hallo";

        vcVector.setSafe(0,new Text(test));
        vcVector.setValueCount(test.length());

        String val = Text.decode(vcVector.get(0));
        System.out.println(val);

        */

        Helper helper = new Helper();

        ArrowType int32 = new ArrowType.Int(32,true);
        Field a = Field.nullable("a", int32);
        Field b = Field.nullable("b", int32);
        List<Field> args = Lists.newArrayList(a, b);

        Condition condition = TreeBuilder.makeCondition("less_than", args);

        Schema schema = new Schema(args);
        Filter filter = Filter.make(schema, condition);

        int numRows = 16;
        byte[] validity = new byte[]{(byte) 255, 0};
        // second half is "undefined"
        int[] values_a = new int[]{1, 2, 3, 4, 5, 6, 7, 8,  9, 10, 11, 12, 13, 14, 15, 16};
        int[] values_b = new int[]{2, 1, 4, 3, 6, 5, 8, 7, 10,  9, 12, 11, 14, 13, 14, 15};
        int[] expected = {0, 2, 4, 6};


        BufferAllocator allo = new RootAllocator();
        ArrowBuf validitya = allo.buffer(validity.length);
        ArrowBuf valuesa = helper.intBuf(values_a,allo);
        ArrowBuf validityb = allo.buffer(validity.length);
        ArrowBuf valuesb = helper.intBuf(values_b,allo);
        ArrowRecordBatch batch = new ArrowRecordBatch(
                numRows,
                Lists.newArrayList(new ArrowFieldNode(numRows, 0), new ArrowFieldNode(numRows, 0)),
                Lists.newArrayList(validitya, valuesa, validityb, valuesb));

        ArrowBuf selectionBuffer = allo.buffer(numRows * 2);
        SelectionVectorInt16 selectionVector = new SelectionVectorInt16(selectionBuffer);

        filter.evaluate(batch, selectionVector);

        // free buffers
        int[] actual = helper.selectionVectorToArray(selectionVector);


        System.out.println("---------- Evaluierter Vektor----------");
        for(int i=0; i <actual.length;i++){
            System.out.println(actual[i]);
        }
        List<ArrowBuf> buffers = batch.getBuffers();
        batch.close();
        selectionBuffer.close();
        filter.close();


    }

}
