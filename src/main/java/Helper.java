import org.apache.arrow.gandiva.evaluator.SelectionVector;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;

public class Helper {

    ArrowBuf intBuf(int [] intBuf, BufferAllocator allo){
        ArrowBuf buffer= allo.buffer(intBuf.length*4);
        for(int i=0; i< intBuf.length;i++){
            buffer.writeInt(intBuf[i]);
        }
        return buffer;
    }
    public int[] selectionVectorToArray(SelectionVector vector) {
        int[] actual = new int[vector.getRecordCount()];
        for (int i = 0; i < vector.getRecordCount(); ++i ) {
            actual[i] = vector.getIndex(i);
        }
        return actual;
    }

}
