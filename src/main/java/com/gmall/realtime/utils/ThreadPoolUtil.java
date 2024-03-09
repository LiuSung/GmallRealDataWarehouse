package com.gmall.realtime.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
public class ThreadPoolUtil {
    private static ThreadPoolExecutor threadPoolExecutor;
    private ThreadPoolUtil() {

    }
    //双重校验锁实现单例设计模式
    public static ThreadPoolExecutor getThreadPoolExecutor(){
        if (threadPoolExecutor == null){
            synchronized (ThreadPoolUtil.class) {
                if (threadPoolExecutor == null) {
                    threadPoolExecutor = new ThreadPoolExecutor(5,
                            20,
                            100,
                            TimeUnit.SECONDS,
                            new LinkedBlockingDeque<>());
                }
            }
        }
        return threadPoolExecutor;
    }
}
