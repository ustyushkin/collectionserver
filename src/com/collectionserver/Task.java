package com.collectionserver;

import org.joda.time.DateTime;

public class Task implements Comparable<Task> {
    private Integer id;
    private DateTime lastRun;
    private Integer period;
    private DateTime nextRun;
    private String command;
    private String type;
    private Integer active;
    private TaskStatus status;
    private Integer duration;
    private String xmlResult;
    private String generation;
    private Integer queueId;
    private TaskStatus queueStatus;
    private Integer priority;

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    public TaskStatus getQueueStatus() {
        return queueStatus;
    }

    public void setQueueStatus(TaskStatus queueStatus) {
        this.queueStatus = queueStatus;
    }

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

    public String getGeneration() {
        return generation;
    }

    public void setGeneration(String generation) {
        this.generation = generation;
    }

    public Integer getId() {
        return id;
    }

    public DateTime getLastRun() {
        return lastRun;
    }

    public Integer getPeriod() {
        return period;
    }

    public DateTime getNextRun() {
        return nextRun;
    }

    public String getCommand() {
        return command;
    }

    public String getType() {
        return type;
    }

    public Integer getActive() {
        return active;
    }

    public TaskStatus getStatus() {
        return status;
    }

    public Integer getDuration() {
        return duration;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public void setLastRun(DateTime lastRun) {
        this.lastRun = lastRun;
    }

    public void setPeriod(Integer period) {
        this.period = period;
    }

    public void setNextRun(DateTime nextRun) {
        this.nextRun = nextRun;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setActive(Integer active) {
        this.active = active;
    }

    public void setStatus(TaskStatus status) {
        this.status = status;
    }

    public void setDuration(Integer duration) {
        this.duration = duration;
    }

    public void calculateNextRun(){
        DateTime lastRun = new DateTime();
        this.setLastRun(lastRun);
        Integer period = this.getPeriod();
        this.setNextRun(lastRun.plusSeconds(period));
    }

    public String getXmlResult() {
        return xmlResult;
    }

    public void setXmlResult(String xmlResult) {
        this.xmlResult = xmlResult;
    }

    @Override
    public int compareTo(Task emp) {
        return this.getQueueId().compareTo(emp.getQueueId());
    }

    @Override
    public String toString() {
        return "Task [id=" + queueId + ", command=" + command + ", generation=" + generation + "]";
    }
}
