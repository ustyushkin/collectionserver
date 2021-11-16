package com.collectionserver;

import java.util.PriorityQueue;

public class MyQueue<E> extends PriorityQueue<E> {
    private mysqldb db;
    MyQueue(mysqldb db){
       super();
       this.db = db;
    }
    @Override
    public boolean add(E e){
        Task task = (Task) e;
        task.setStatus(TaskStatus.QUEUED);
        this.db.saveQueuedTask((Task)e);
        return super.add(e);
    }
}
