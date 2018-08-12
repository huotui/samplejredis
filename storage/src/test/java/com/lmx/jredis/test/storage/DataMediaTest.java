package com.lmx.jredis.test.storage;

import com.google.common.base.Charsets;
import com.lmx.jredis.storage.DataHelper;
import com.lmx.jredis.storage.DataMedia;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

public class DataMediaTest {

    DataMedia test;

    @Before
    public void before() {
        try {
            test = new DataMedia("test", 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void after() {
        try {
            test.clean();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void add() {
        final ByteBuffer b = ByteBuffer.allocateDirect(128);
        final String value = "0123456789";
        final int length = value.getBytes().length;
        b.putInt(length);
        b.put(value.getBytes(Charsets.UTF_8));
        b.flip();
        try {
            //add
            DataHelper ih = test.add(b);
            ih.setLength(length);
            //get
            byte[] bytes = test.get(ih);
            String getValue = new String(bytes, Charsets.UTF_8);
            Assert.assertEquals(value, getValue);
            //update
            String value1 = "12345";
            DataHelper up1 = test.update(ih, value1.getBytes(Charsets.UTF_8));
            ih.setLength(value1.getBytes().length);
            String getValue1 = new String(test.get(up1), Charsets.UTF_8);
            Assert.assertEquals(value1, getValue1);
            //del
            test.remove(up1);
            Assert.assertTrue("value:" + test.get(up1), test.get(up1) == null);
        } catch (
                Exception e)

        {
            e.printStackTrace();
        }
    }
}

            }