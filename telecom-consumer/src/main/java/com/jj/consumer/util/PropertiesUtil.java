package com.jj.consumer.util;

import com.jj.consumer.hbase.CalleeWriteBackCoprocessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtil {
    private static Logger logger = LoggerFactory.getLogger(PropertiesUtil.class);
    public static Properties prop = null;

    static {
        initProp();
    }
    private static void initProp(){
        prop = new Properties();
        InputStream is1 = ClassLoader.getSystemResourceAsStream("kafka.properties");
        InputStream is2 = ClassLoader.getSystemResourceAsStream("hbase.properties");
        logger.info("before load prop:{}",prop);
        try {
            prop.load(is1);
            prop.load(is2);
            logger.info("after load prop:{}",prop);
        } catch (IOException e) {
            logger.error("prop load error",e);
            e.printStackTrace();
        } finally {
            try {
                if (is2 != null) is2.close();
                if (is1 != null) is1.close();
            } catch (IOException e) {
                logger.error("inputstream close error",e);
                e.printStackTrace();
            }
        }
    }

    /**
     * 获取配置属性
     * @param key
     * @return
     */
    public static String getProperty(String key) {
        //**注意协处理器使用反射来构建 这时候并没有加载这个类 prop对象还没初始化所以构建失败
        if(prop==null)initProp();
        logger.info("logger obj:{}",logger);
        String value = prop.getProperty(key);
        logger.info("com.jj.consumer.util.PropertiesUtil.getProperty key:{} value:{}",key,value);
        return value;
    }

//    public static void main(String[] args) {
//        String property = prop.getProperty("telecom.table");
//        System.out.println(property);
//        System.out.println(CalleeWriteBackCoprocessor.class.getCanonicalName());
//    }
}
