package com.collectionserver;

import org.ini4j.Ini;
import org.joda.time.DateTime;

import java.io.File;
import java.io.IOException;
import java.util.*;

enum TaskStatus{
    AWAIT,
    QUEUED,
    LAUNCHED,
    PARSING,
    COMPLETED,
    INTERRUPTED
}
public class Main {
    private static int COUNT_TASK_IN_QUEUE = 10;
    private static int PAUSE_BETWEEN_TASK = 100;
    private static int COUNT_RUNING_THREAD = 5;
    private static boolean SHOW_DEBUG_MESSAGE = false;
    //private static String path = "C:\\Nmap" ;

    public static void main(String[] args) throws IOException {

        Ini ini = new Ini(new File("config.ini"));
        //path = ini.get("config", "nmap_path");
        COUNT_TASK_IN_QUEUE = Integer.valueOf(ini.get("config", "COUNT_TASK_IN_QUEUE"));
        PAUSE_BETWEEN_TASK = Integer.valueOf(ini.get("config", "PAUSE_BETWEEN_TASK"));
        COUNT_RUNING_THREAD = Integer.valueOf(ini.get("config", "COUNT_RUNING_THREAD"));
        SHOW_DEBUG_MESSAGE = Boolean.valueOf(ini.get("config", "SHOW_DEBUG_MESSAGE"));

        mysqldb db = new mysqldb();
        Queue<Task> pq = new MyQueue<>(db);
        ArrayList<Task> tasks;
        ArrayList<ThreadNmap> ThreadList = new ArrayList<>();
        ArrayList<Task> priorityTasks;
        int countRuningThread = 0;
        int positionInTasks = 0;
        int positionInpriorityTasks = 0;
        Task task;

        if (SHOW_DEBUG_MESSAGE) System.out.println("start collecting queue");

        DateTime now;
        priorityTasks = new ArrayList<Task>();
        tasks = db.getPriorityTask(0);
        db.clearQueue();

        while (true) {
            if (tasks.isEmpty()){
                tasks = db.getPriorityTask(0);
            }

            positionInpriorityTasks = 0;
            if (priorityTasks.isEmpty()) {
                priorityTasks = db.getPriorityTask(1);
            }
            while(!priorityTasks.isEmpty() && positionInpriorityTasks<priorityTasks.size()){
                Task prioritytask = priorityTasks.get(positionInpriorityTasks);
                now = new DateTime();
                if (prioritytask.getNextRun().getMillis() < now.getMillis()) {
                    if (SHOW_DEBUG_MESSAGE) System.out.println("priority task processing " + prioritytask.getCommand());
                    prioritytask.calculateNextRun();
                    db.saveTask(prioritytask);
                    Collections.reverse(tasks);
                    tasks.add(prioritytask);
                    Collections.reverse(tasks);
                    priorityTasks.remove(positionInpriorityTasks);
                }else{
                    //System.out.println("not yet " + prioritytask.getCommand());
                }
                positionInpriorityTasks++;
            }

            positionInTasks = 0;
            if (db.getCountTaskInQueue()<=COUNT_TASK_IN_QUEUE) {
                while (!tasks.isEmpty() && db.getCountTaskInQueue()<=COUNT_TASK_IN_QUEUE && positionInTasks<tasks.size()) {
                    task = tasks.get(positionInTasks);
                    now = new DateTime();
                    if (task.getNextRun().getMillis() < now.getMillis() || task.getPriority()>0) {
                        if (SHOW_DEBUG_MESSAGE) System.out.println("task processing " + task.getCommand());

                        task.calculateNextRun();
                        db.saveTask(task);

                        pq.add(task);
                        if (SHOW_DEBUG_MESSAGE) System.out.println("add in queue task id "+ task.getQueueId());
                        tasks.remove(positionInTasks);
                    }else{
                        //System.out.println("time for task has not come yet " + task.getId() + " " + task.getCommand());
                    }
                    positionInTasks++;
                }
            }

            countRuningThread = COUNT_RUNING_THREAD;
            while (countRuningThread>0) {
                if (db.getCountActiveTaskInQueue() < countRuningThread) {
                Task taskQueued = pq.poll();
                if (taskQueued != null)
                    switch (taskQueued.getStatus()) {
                        case QUEUED:
                            taskQueued.setStatus(TaskStatus.LAUNCHED);
                            taskQueued.setLastRun(new DateTime());
                            taskQueued.calculateNextRun();
                            db.saveQueuedTask(taskQueued);
                            ThreadNmap thread = new ThreadNmap(taskQueued, db);
                            ThreadList.add(thread);
                            int thIndex = ThreadList.indexOf(thread);
                            ThreadList.get(thIndex).start();
                            //thread.start();
                            if (SHOW_DEBUG_MESSAGE) System.out.println("run id= " + taskQueued.getQueueId() + " command=" + taskQueued.getCommand());
                            if (SHOW_DEBUG_MESSAGE) if (Integer.valueOf(taskQueued.getPriority())!=0) System.out.println("--------> priority = " + taskQueued.getPriority());
                            break;
                        case COMPLETED:
                            //pq.remove(taskQueued);
                            break;
                    }
                try {
                    Thread.sleep(PAUSE_BETWEEN_TASK);
                } catch (InterruptedException e) {
                    System.out.println("Thread has been interrupted");
                }
                }
                countRuningThread--;
            }
            if (SHOW_DEBUG_MESSAGE) System.out.println("--------------------------------------------");
            Iterator<ThreadNmap> tn = ThreadList.iterator();
            while (tn.hasNext()) {
                ThreadNmap tnInL = tn.next();
                if (tnInL.getState().toString().equals("TERMINATED")) {
                    if (SHOW_DEBUG_MESSAGE) System.out.println("remove terminated link " + tnInL.getName() + " "+tnInL.getState());
                    //tnInL.interrupt();
                    tn.remove();
                }else
                if (SHOW_DEBUG_MESSAGE) System.out.println(tnInL.getName() + " "+tnInL.getState());
            }
            if (SHOW_DEBUG_MESSAGE) System.out.println("--------------------------------------------");

            System.gc();
        }
    }
}
