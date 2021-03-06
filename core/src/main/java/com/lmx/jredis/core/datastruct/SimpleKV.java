package com.lmx.jredis.core.datastruct;

import com.google.common.base.Charsets;
import com.lmx.jredis.storage.DataHelper;
import com.lmx.jredis.storage.DataMedia;
import com.lmx.jredis.storage.DataTypeEnum;
import com.lmx.jredis.storage.IndexHelper;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

/**
 * 基于内存读写key value操作,数据可持久,零延迟
 * Created by lmx on 2017/4/14.
 */
@Slf4j
public class SimpleKV extends BaseOP {
    int storeSize;
    int kvSize;

    SimpleKV(int storeSize) {
        this.storeSize = storeSize;
    }

    public void init(int db) {
        try {
            store = new DataMedia(db, "valueData", storeSize);
/*            ih = new IndexHelper(db, "keyIndex", storeSize / 2) {
                public void wrapData(DataHelper dataHelper) {
                    if (dataHelper.getType().equals(DataTypeEnum.KV.getDesc())) {
                        if (!kv.containsKey(dataHelper.getKey())) {
                            kv.put(dataHelper.getKey(), dataHelper);
                            expire.put(dataHelper.getKey(), dataHelper.getExpire());
                            kvSize++;
                        }
                    }
                }
            };
            ih.recoverIndex();
            log.info("db: {},recover data kv size: {}", db, kvSize);*/
        } catch (Exception e) {
            log.error("init store file error", e);
        }
    }

    public boolean write(String key, String value) {
        try {
            if (super.isCanWrite(key, value)) {
                DataHelper dataHelper = (DataHelper) ih.type(key);
                if (dataHelper != null) {
                    dataHelper = store.update(dataHelper, value.getBytes(Charsets.UTF_8));
                    ih.updateIndex(dataHelper);
                    return true;
                } else {
                    ByteBuffer b = ByteBuffer.allocateDirect(128);
                    int length = value.getBytes().length;
                    b.putInt(length);
                    b.put(value.getBytes(Charsets.UTF_8));
                    b.flip();
                    DataHelper dh = store.add(b);
                    dh.setKey(key);
                    dh.setLength(length);
                    ih.add(dh);
                    return true;
                }
            }
        } catch (Exception e) {
            log.error("write data error", e);
        }
        return false;
    }

    public byte[] read(String key) {
        try {
            if (super.isExpire(key)) {
                return null;
            }
            long start = System.currentTimeMillis();
            DataHelper posIh = (DataHelper) ih.type(key);
            //check null, value not may be not exist
            if (posIh == null){
                log.debug("read IndexHelper null key:"+key);
                return null;
            }
            byte[] data = store.get(posIh);
            String resp = new String(data, Charsets.UTF_8);
            log.debug("key={},value={} cost={}ms", key, resp, (System.currentTimeMillis() - start));
            return data;
        } catch (Exception e) {
            log.error("read data error key:"+key, e);
        }
        return null;
    }

    @Override
    public boolean checkKeyType(String key) {
        return isExist(key) ? ih.type(key) instanceof DataHelper : true;
    }

    @Override
    public void removeData(String key) {
        //TODO debug read error null
        DataHelper dataHelper = (DataHelper) ih.type(key);
        ih.remove(dataHelper);
        store.remove(dataHelper);
    }
}
