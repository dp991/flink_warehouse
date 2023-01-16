package org.example.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 线程池
 */
public class ThreadPoolUtil {

    static ThreadPoolExecutor threadPoolExecutor;

    private ThreadPoolUtil() {
    }


    /**
     * todo 注意：线程池必须等队列满了才会去创建maximumPoolSize-corePoolSize线程
     * @return
     */
    public static ThreadPoolExecutor getThreadPoolExecutor() {
        if (threadPoolExecutor == null) {
            synchronized (ThreadPoolUtil.class) {
                if (threadPoolExecutor == null) {
                    threadPoolExecutor = new ThreadPoolExecutor(
                            8,16,1L, TimeUnit.MINUTES,new LinkedBlockingDeque<>());

                }
            }
        }
        return threadPoolExecutor;
    }
}
