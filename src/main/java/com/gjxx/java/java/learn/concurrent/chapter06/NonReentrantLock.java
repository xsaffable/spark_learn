package com.gjxx.java.java.learn.concurrent.chapter06;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * @author Sxs
 * @description 不可重入 独占锁
 * @date 2019/11/5 18:22
 */
public class NonReentrantLock implements Lock, Serializable {

    /**
     * 内部帮助类
     */
    private static class Sync extends AbstractQueuedSynchronizer {
        /**
         * 是否锁已经被持有
         * 0->没有被线程持有
         * 1->被某一个线程持有
         * @return 是否被持有
         */
        @Override
        protected boolean isHeldExclusively() {
            return getState() == 1;
        }

        /**
         * 如果state为0，则尝试获取锁
         * @param acquires
         * @return 是否获取成功
         */
        @Override
        protected boolean tryAcquire(int acquires) {
            assert acquires == 1;
            // 看state在本类中是否为0，如果为0，则置为1
            // 表示获取到锁
            if (compareAndSetState(0, 1)) {
                // 当前线程获取到锁
                setExclusiveOwnerThread(Thread.currentThread());
                return true;
            }
            return false;

        }

        /**
         * 尝试释放锁，设置state为0
         * @param releases
         * @return 是否释放成功
         */
        @Override
        protected boolean tryRelease(int releases) {
            assert releases == 1;
            if (getState() == 0) {
                throw new IllegalMonitorStateException();
            }
            setExclusiveOwnerThread(null);
            setState(0);
            return true;
        }

        /**
         * 提供条件接口变量
         * @return Condition
         */
        Condition newCondition() {
            return new ConditionObject();
        }
    }

    /**
     * 创建一个Sync来做具体的工作
     */
    private final Sync sync = new Sync();

    @Override
    public void lock() {
        sync.acquire(1);
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        sync.acquireInterruptibly(1);
    }

    @Override
    public boolean tryLock() {
        return sync.tryAcquire(1);
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return sync.tryAcquireNanos(1, unit.toNanos(time));
    }

    @Override
    public void unlock() {
        sync.release(1);
    }

    @Override
    public Condition newCondition() {
        return sync.newCondition();
    }

    public boolean isLocked() {
        return sync.isHeldExclusively();
    }

}
