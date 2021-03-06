package com.lmx.jredis.core.datastruct;

import com.lmx.jredis.storage.DataMedia;
import com.lmx.jredis.storage.IndexHelper;
import lombok.Data;

/**
 * Created by limingxin on 2017/8/7.
 */
@Data
public abstract class BaseOP {
    DataMedia store;
    IndexHelper ih;

    public boolean isExpire(String key) {
        long time = ih.getExpire(key);
        if (time == 0)
            return false;
        if (System.currentTimeMillis() - time > 0) {
            remove(key);
            ih.rmExpire(key);
            return true;
        }else {
            return false;
        }
    }

    public boolean isExist(String key) {
        return ih.exist(key);
    }

    public abstract boolean checkKeyType(String key);

    public abstract void removeData(String key);

    public void remove(String key) {
        removeData(key);
        ih.kv.remove(key);
    }

    public boolean isCanWrite(String key, String value) {
        return checkKeyType(key);
    }

    public boolean isCanWrite(int db, String key, String value) {
        return checkKeyType(key);
    }
}
