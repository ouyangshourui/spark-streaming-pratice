package com.whz.platform.sparkKudu.util;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName: ThreadPoolUtil
 * @date 2016年03月30日 
 * @author spirit
 */
public class ThreadPoolUtil {
	private static ThreadPoolExecutor executor;
    private static BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<Runnable>(1000);
	public  static ThreadPoolUtil threadPoolUtil;
	public  final static Logger logger = LoggerFactory.getLogger(ThreadPoolUtil.class);
	private final static int KEEP_ALIVE_TIME = 0;
	
	private ThreadPoolUtil(){
		super();
	}

    private static ThreadFactory threadFactory = new ThreadFactory() {
        private final AtomicInteger integer = new AtomicInteger();
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "myThreadPool thread:" + integer.getAndIncrement());
        }
    };
    
	public static ThreadPoolUtil getInstance(int corecount,int maxcount){
		if(threadPoolUtil == null) {
			logger.info("线程池资源为空，创建线程池！");
			threadPoolUtil = new ThreadPoolUtil();
			executor = new ThreadPoolExecutor(corecount,
					maxcount, KEEP_ALIVE_TIME, TimeUnit.SECONDS, workQueue,threadFactory);
			executor.prestartAllCoreThreads();
		 }
		 return threadPoolUtil;
	}

	public boolean isRunning() {
		return !executor.isTerminated() && !executor.isTerminating();
	}

	public boolean isStarted() {
		return !executor.isTerminated() && !executor.isTerminating();
	}

	public boolean isStopped() {
		return executor.isTerminated();
	}

	public boolean isStopping() {
		return executor.isTerminating();
	}

	public void start() throws Exception {
		if (executor.isTerminated() || executor.isTerminating()
				|| executor.isShutdown()) {
			throw new IllegalStateException("Cannot restart");
		}
	}

	public void stop(){
		try {
			executor.shutdown();
			if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
				executor.shutdownNow();
			}
		} catch (Exception e) {
			logger.error("error message:", e);
		}
	}
	public ThreadPoolExecutor getExecutor() {
		return executor;
	}

}
