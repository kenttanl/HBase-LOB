package org.apache.hadoop.hbase.mob.compactions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mob.compactions.SweepMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@Category(SmallTests.class)
public class TestSweepMapper {

  @Test
  public void TestMap() throws Exception {
    Configuration configuration = new Configuration();
    String prefix = "00000000";
    final String fileName = "19691231f2cd014ea28f42788214560a21a44cef";
    final String mobFilePath = prefix + fileName;

    ImmutableBytesWritable r = new ImmutableBytesWritable(Bytes.toBytes("r"));
    final KeyValue[] kvList = new KeyValue[1];
    kvList[0] = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("family"),
            Bytes.toBytes("column"), Bytes.toBytes(mobFilePath));

    Result columns = mock(Result.class);
    when(columns.rawCells()).thenReturn(kvList);

    Mapper<ImmutableBytesWritable, Result, Text, KeyValue>.Context ctx =
            mock(Mapper.Context.class);
    when(ctx.getConfiguration()).thenReturn(configuration);
    SweepMapper map = new SweepMapper();
    doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        Text text = (Text) invocation.getArguments()[0];
        KeyValue kv = (KeyValue) invocation.getArguments()[1];

        assertEquals(Bytes.toString(text.getBytes(), 0, text.getLength()), fileName);
        assertEquals(0, Bytes.compareTo(kv.getKey(), kvList[0].getKey()));

        return null;
      }
    }).when(ctx).write(any(Text.class), any(KeyValue.class));

    map.map(r, columns, ctx);

  }


}
