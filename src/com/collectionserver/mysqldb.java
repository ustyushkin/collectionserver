package com.collectionserver;


import org.ini4j.Ini;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class mysqldb {
    private Connection con;
    //Statement stmt = null;
    private boolean SHOW_DEBUG_MESSAGE;
    private String DB_LOGIN = "login";
    private String DB_PASSWORD = "password";
    private String DB_IP = "127.0.0.1";
    private String DB_PORT = "3306";
    private String DB_NAME = "datebase";
    //private ArrayList<String> excludeList;

    public mysqldb() throws IOException {
        /*this.excludeList = new ArrayList<String>();
        this.excludeList.add("172.16.1.38");
        this.excludeList.add("172.16.1.39");
        this.excludeList.add("172.16.2.49");*/

        Ini ini = new Ini(new File("config.ini"));
        SHOW_DEBUG_MESSAGE = Boolean.valueOf(ini.get("config", "SHOW_DEBUG_MESSAGE"));
        DB_LOGIN = String.valueOf(ini.get("config", "DB_LOGIN"));
        DB_PASSWORD = String.valueOf(ini.get("config", "DB_PASSWORD"));
        DB_IP = String.valueOf(ini.get("config", "DB_IP"));
        DB_PORT = String.valueOf(ini.get("config", "DB_PORT"));
        DB_NAME = String.valueOf(ini.get("config", "DB_NAME"));

        try {
            Class.forName("com.mysql.jdbc.Driver");
            this.con = DriverManager.getConnection(
                    "jdbc:mysql://"+DB_IP+":"+DB_PORT+"/"+DB_NAME, DB_PASSWORD, DB_LOGIN);
            //stmt = con.createStatement();
        } catch (Exception e) {
            if (SHOW_DEBUG_MESSAGE) System.out.println(e);
        }

    }

    synchronized public Connection getConnection() {
        return this.con;
    }

    public ArrayList<Task> getPriorityTask(int priority) {
        ArrayList<Task> result = new ArrayList<Task>();
        try {
            Connection con = this.getConnection();
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("select * from task where active = 1 and priority = " + priority);
            while (rs.next()) {
                String generation = rs.getString("generation");
                if (generation != null && !generation.equals("")) {
                    try {
                        Statement stmtIn = con.createStatement();
                        ResultSet rsIn = stmtIn.executeQuery("select distinct(value) from " + generation + " order by value"); //+ " where active = 1"
                        //Integer subId = 1;
                        while (rsIn.next()) {
                            //if (!this.excludeList.contains(rsIn.getString("value"))) {
                                Task task = createTask(rs, generation, rsIn.getString("value"));
                                result.add(task);
                            //}
                            //subId++;
                        }
                    } catch (Exception e) {
                        if (SHOW_DEBUG_MESSAGE) System.out.println(e);
                    }
                } else {
                    Task task = createTask(rs);
                    result.add(task);
                }
            }
            rs.close();
            stmt.close();
        } catch (Exception e) {
            if (SHOW_DEBUG_MESSAGE) System.out.println(e);
        }
        return result;
    }

    private Task createTask(ResultSet rs) {
        Task task = new Task();
        try {
            task.setId(rs.getInt("id"));
            task.setLastRun(new DateTime(rs.getTimestamp("lastrun")));
            task.setPeriod(rs.getInt("period"));
            task.setNextRun(new DateTime(rs.getTimestamp("nextrun")));
            task.setCommand(rs.getString("command"));
            task.setType(rs.getString("type"));
            task.setActive(rs.getInt("active"));
            task.setStatus(TaskStatus.valueOf(rs.getString("status")));
            task.setDuration(rs.getInt("duration"));
            task.setGeneration(rs.getString("generation"));
            task.setPriority(rs.getInt("priority"));
        } catch (Exception e) {
            if (SHOW_DEBUG_MESSAGE) System.out.println(e);
        }
        return task;
    }

    private Task createTask(ResultSet rs, String paramName, String paramValue) {
        Task task = new Task();
        try {
            task.setId(rs.getInt("id"));
            task.setLastRun(new DateTime(rs.getTimestamp("lastrun")));
            task.setPeriod(rs.getInt("period"));
            task.setNextRun(new DateTime(rs.getTimestamp("nextrun")));
            String newCommand = rs.getString("command");
            newCommand = newCommand.replace(":" + paramName, paramValue);
            task.setCommand(newCommand);
            task.setType(rs.getString("type"));
            task.setActive(rs.getInt("active"));
            task.setStatus(TaskStatus.valueOf(rs.getString("status")));
            task.setQueueStatus(TaskStatus.valueOf(rs.getString("status")));
            task.setDuration(rs.getInt("duration"));
            task.setGeneration(rs.getString("generation"));
            task.setPriority(rs.getInt("priority"));
        } catch (Exception e) {
            if (SHOW_DEBUG_MESSAGE) System.out.println(e);
        }
        return task;
    }

    public void saveTask(Map<String, Task> tasks) {
        for (Map.Entry<String, Task> entry : tasks.entrySet()) {
            Task task = entry.getValue();
            this.saveTask(task);
        }

    }

    public void saveTask(ArrayList<Task> tasks) {
        for (Task task : tasks) {
            this.saveTask(task);
        }

    }

    synchronized public void deleteQueuedTask(Task task) {
        try {
            Connection con = this.getConnection();
            Statement stmt = con.createStatement();
            String sql = "DELETE FROM queue WHERE id = " + task.getQueueId() + ";";
            stmt.executeUpdate(sql);
            stmt.close();
            //con.close();
            //System.out.println("delete "+ String.valueOf(task.getQueueId()));
        } catch (Exception e) {
            if (SHOW_DEBUG_MESSAGE) System.out.println(e);
        }
    }

    synchronized public void clearQueue() {
        try {
            Connection con = this.getConnection();
            Statement stmt = con.createStatement();
            String sql = "DELETE FROM queue ;";
            stmt.executeUpdate(sql);
            stmt.close();
            //con.close();
            if (SHOW_DEBUG_MESSAGE) System.out.println("queue cleared ");
        } catch (Exception e) {
            if (SHOW_DEBUG_MESSAGE) System.out.println(e);
        }
    }

    synchronized public void saveTask(Task task) {
        try {
            Connection con = this.getConnection();
            Statement stmt = con.createStatement();
            DateTimeFormatter dtfOut = getDateFormat("yyyy-MM-dd HH:mm:ss");
            String sql = "UPDATE task SET lastrun = '" + dtfOut.print(task.getLastRun()) + "', nextrun = '" + dtfOut.print(task.getNextRun()) + "', duration = '" + task.getDuration() + "' WHERE id = " + task.getId() + ";";
            stmt.executeUpdate(sql);
            stmt.close();
            //con.close();
        } catch (Exception e) {
            if (SHOW_DEBUG_MESSAGE) System.out.println(e);
        }
    }

    public void updateQueuedTask(Task task) {
        try {
            Connection con = this.getConnection();
            Statement stmt = con.createStatement();
            DateTimeFormatter dtfOut = getDateFormat("yyyy-MM-dd HH:mm:ss");
            String sql = "UPDATE queue SET status = '" + task.getStatus() + "' WHERE id = " + task.getQueueId() + ";";
            stmt.executeUpdate(sql);
            stmt.close();
            //con.close();
        } catch (Exception e) {
            if (SHOW_DEBUG_MESSAGE) System.out.println(e);
        }
    }

    public Integer insert(String insertQuery){
        int returnId = 0;
        try {
            Connection con = this.getConnection();
            Statement stmt = con.createStatement();
            if (stmt.executeUpdate(insertQuery) > 0) {
                java.sql.ResultSet generatedKeys = stmt.getGeneratedKeys();
                if (generatedKeys.next()) {
                    returnId = generatedKeys.getInt(1);
                }
                generatedKeys.close();
            }
            stmt.close();
            //con.close();
        } catch (Exception e) {
            if (SHOW_DEBUG_MESSAGE) System.out.println(e);
        }
        return returnId;
    }

    public Integer addHistory(String insertQuery){
        int returnId = 0;
        try {
            Connection con = this.getConnection();
            Statement stmt = con.createStatement();
            stmt.executeUpdate(insertQuery);
            stmt.close();
            //con.close();
        } catch (Exception e) {
            if (SHOW_DEBUG_MESSAGE) System.out.println(e);
        }
        return returnId;
    }

    public ResultSet select(String selectQuery){
        ResultSet rs = null;
        try {
            Connection con = this.getConnection();
            Statement stmt = con.createStatement();
            rs = stmt.executeQuery(selectQuery);
            //stmt.close();
            //con.close();
        } catch (Exception e) {
            if (SHOW_DEBUG_MESSAGE) System.out.println(e);
        }
        return rs;
    }

    public void update(String updateQuery){
        try {
            Connection con = this.getConnection();
            Statement stmt = con.createStatement();
            stmt.executeUpdate(updateQuery);
            stmt.close();
            //con.close();
        } catch (Exception e) {
            if (SHOW_DEBUG_MESSAGE) System.out.println(e);
        }
    }

    synchronized public void saveQueuedTask(Task task) {
        //select from id queue
        //if exist insert
        //else update
        int returnId = 0;
        try {
            Connection con = this.getConnection();
            Statement stmt = con.createStatement();
            DateTimeFormatter dtfOut = getDateFormat("yyyy-MM-dd HH:mm:ss");
            String sql = "SELECT id from queue where id='" + task.getQueueId() + "' and command='" + task.getCommand() + "';";
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next() == false) {
                String sqlInsertMac = "INSERT INTO queue SET id_task = '" + task.getId() + "', command = '" + task.getCommand() + "', status = '" + task.getStatus() + "', duration = '" + task.getDuration() + "', startrun = '" + dtfOut.print(new DateTime()) + "';";
                if (stmt.executeUpdate(sqlInsertMac) > 0) {
                    java.sql.ResultSet generatedKeys = stmt.getGeneratedKeys();
                    if (generatedKeys.next()) {
                        returnId = generatedKeys.getInt(1);
                    }
                    generatedKeys.close();
                }
            } else {
                    returnId = rs.getInt("id");
                    String sqlUpdateDateScan = "UPDATE queue SET status = '" + task.getStatus() + "' WHERE id=" + returnId + ";";
                    Statement stmt2 = con.createStatement();
                    stmt2.executeUpdate(sqlUpdateDateScan);
                    stmt2.close();
            }
            rs.close();
            stmt.close();
            //con.close();
        } catch (Exception e) {
            if (SHOW_DEBUG_MESSAGE) System.out.println(e);
        }
        task.setQueueId(returnId);
    }

    synchronized public void saveMac(String mac) {
        try {
            DateTimeFormatter dtfOut = getDateFormat("yyyy-MM-dd HH:mm:ss");
            String sql = "SELECT id from mac where value='" + mac + "';";
            ResultSet rs = select(sql);
            if (rs.next() == false) {
                String sqlInsertMac = "INSERT INTO mac SET value = '" + mac + "', date = '" + dtfOut.print(new DateTime()) + "';";
                Integer id = insert(sqlInsertMac);
                String sqlInsertHistory = "INSERT INTO mac_history SET id_mac = '" + id + "', date = '" + dtfOut.print(new DateTime()) + "';";
                addHistory(sqlInsertHistory);
            } else {
                    Integer id = rs.getInt("id");
                    String sqlUpdateDateScan = "UPDATE mac SET date = '" + dtfOut.print(new DateTime()) + "' WHERE id=" + String.valueOf(id) + ";";
                    update(sqlUpdateDateScan);

                    String sqlInsertHistory = "INSERT INTO mac_history SET id_mac = '" + id + "', date = '" + dtfOut.print(new DateTime()) + "';";
                    addHistory(sqlInsertHistory);
            }
            Statement stmt = rs.getStatement();
            rs.close();
            stmt.close();
        } catch (Exception e) {
            if (SHOW_DEBUG_MESSAGE) System.out.println("saveMac "+e);
        }
    }

    public void saveVendor(String mac, String vendor) {
        try {
            DateTimeFormatter dtfOut = getDateFormat("yyyy-MM-dd HH:mm:ss");
            String sql = "SELECT id,vendor from mac where value='" + mac + "';";
            ResultSet rs = select(sql);
            if (rs.next() == false) {
            } else {
                    Integer id = rs.getInt("id");
                    String vendorSaved = rs.getString("vendor");
                    if (vendor != null || !vendor.equals(vendorSaved)) {
                        String vendorCleared = vendor.replace("'", "");
                        String sqlUpdateVendorScan = "UPDATE mac SET vendor='" + vendorCleared + "', date='" + dtfOut.print(new DateTime()) + "' WHERE id=" + String.valueOf(id) + ";";
                        update(sqlUpdateVendorScan);
                    }
            }
            Statement stmt = rs.getStatement();
            rs.close();
            stmt.close();
        } catch (Exception e) {
            if (SHOW_DEBUG_MESSAGE) System.out.println("saveVen "+e);
        }
    }
    public DateTimeFormatter getDateFormat(String formatString){
        return DateTimeFormat.forPattern(formatString);
    }

    public void saveIP(String mac, String ip) {
        try {
            DateTimeFormatter dtfOut = this.getDateFormat("yyyy-MM-dd HH:mm:ss");
            String sql = "SELECT id from mac where value='" + mac + "';";
            ResultSet rs = select(sql);
            if (rs.next() == false) {
                this.saveMac(mac);
            } else {
                do {
                    Integer id = rs.getInt("id");
                    String sql2 = "SELECT id,value from ip where id_mac=" + String.valueOf(id) + " and value='" + ip + "' ;"; //and DATE_FORMAT(date, \"%Y-%m-%d %H\")='"+dtfOut2.print(new DateTime())+"'
                    ResultSet rs2 = select(sql2);
                    if (rs2.next() == false) {
                        String sqlInsertIP = "INSERT INTO ip SET value = '" + ip + "', date = '" + dtfOut.print(new DateTime()) + "', id_mac = '" + String.valueOf(id) + "';";
                        Integer insertedId = insert(sqlInsertIP);
                        String sqlInsertHistory = "INSERT INTO ip_history SET id_ip = '" + insertedId + "', date = '" + dtfOut.print(new DateTime()) + "';";
                        addHistory(sqlInsertHistory);
                    } else {
                            String ipSaved = rs2.getString("value");
                            Integer idIP = rs2.getInt("id");
                            String sqlUpdateIP = "UPDATE ip SET date = '" + dtfOut.print(new DateTime()) + "' WHERE id=" + String.valueOf(idIP) + ";"; //value = '" +  ip + "'
                            update(sqlUpdateIP);

                            String sqlInsertHistory = "INSERT INTO ip_history SET id_ip = '" + idIP + "', date = '" + dtfOut.print(new DateTime()) + "';";
                            addHistory(sqlInsertHistory);
                    }
                    Statement stmt2 = rs2.getStatement();
                    rs2.close();
                    stmt2.close();
                } while (rs.next());
            }
            Statement stmt = rs.getStatement();
            rs.close();
            stmt.close();
        } catch (Exception e) {
            if (SHOW_DEBUG_MESSAGE) System.out.println("saveIP "+e);
        }
    }

    //todo переделать - привязать порты к IP (не к MACу)
    public void savePort(String mac, String port, String desc) {
        try {
            //Statement stmt=con.createStatement();
            DateTimeFormatter dtfOut = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
            DateTimeFormatter dtfOut2 = DateTimeFormat.forPattern("yyyy-MM-dd HH");
            String sql = "SELECT id from mac where value='" + mac + "';";
            ResultSet rs = select(sql);
            if (rs.next() == false) {
                this.saveMac(mac);
                //String sqlInsertMac = "INSERT INTO mac SET value = '" + mac + "', date = '" + dtfOut.print(new DateTime()) + "';";
                //stmt.executeUpdate(sqlInsertMac);
            } else {
                do {
                    Integer id = rs.getInt("id");
                    String sql2 = "SELECT id,value from port where id_mac=" + String.valueOf(id) + " and value=" + port + " ;"; //and DATE_FORMAT(date, \"%Y-%m-%d %H\")='"+dtfOut2.print(new DateTime())+"'
                    Statement stmt2 = con.createStatement();
                    Statement stmt3 = con.createStatement();
                    ResultSet rs2 = stmt2.executeQuery(sql2);
                    if (rs2.next() == false) {
                        String sqlInsertIP = "INSERT INTO port SET value=" + String.valueOf(port) + ", state='open', descr='" + desc + "', date = '" + dtfOut.print(new DateTime()) + "', id_mac = " + String.valueOf(id) + ";";
                        stmt3.executeUpdate(sqlInsertIP);
                    } else {
                        do {
                            Integer portSaved = rs2.getInt("value");
                            Integer idPort = rs2.getInt("id");
                            /*if (!portSaved.equals(port))
                            {*/
                            String sqlUpdateIP = "UPDATE port SET date='" + dtfOut.print(new DateTime()) + "' WHERE id=" + String.valueOf(idPort) + ";"; //value=" + String.valueOf(port) + ",
                            stmt3.executeUpdate(sqlUpdateIP);
                            //}
                        }
                        while (rs2.next());
                    }
                } while (rs.next());
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public String getIdMAC(String mac) {
        Integer id = null;
        try {
            String sqlGim = "SELECT id from mac where value='" + mac + "';";
            ResultSet rsGim = select(sqlGim);
            if (rsGim.next() == false) {

            } else {
                id = rsGim.getInt("id");
            }
            Statement stmt = rsGim.getStatement();
            rsGim.close();
            stmt.close();
        } catch (Exception e) {
            if (SHOW_DEBUG_MESSAGE) System.out.println("get id mac "+e);
        }
        return String.valueOf(id);
    }

    public void saveIPPort(String mac, String ip, String port, String desc) {
        String macId = this.getIdMAC(mac);
        try {
            DateTimeFormatter dtfOut = getDateFormat("yyyy-MM-dd HH:mm:ss");
            String sql = "SELECT id from ip where value='" + ip + "' and id_mac='" + macId + "';";
            ResultSet rs = select(sql);
            if (rs.next() == false) {
            } else {
                do {
                    Integer id = rs.getInt("id");
                    String sql2 = "SELECT id,value from port where id_ip=" + String.valueOf(id) + " and value=" + port + " ;"; //and DATE_FORMAT(date, \"%Y-%m-%d %H\")='"+dtfOut2.print(new DateTime())+"'
                    ResultSet rs2 = select(sql2);
                    if (rs2.next() == false) {
                        String sqlInsertIP = "INSERT INTO port SET value=" + String.valueOf(port) + ", state='open', descr='" + desc + "', date = '" + dtfOut.print(new DateTime()) + "', id_ip = " + String.valueOf(id) + ";";
                        Integer insertedId = insert(sqlInsertIP);
                        String sqlInsertHistory = "INSERT INTO port_history SET id_port = '" + insertedId + "', date = '" + dtfOut.print(new DateTime()) + "';";
                        addHistory(sqlInsertHistory);
                    } else {
                            Integer portSaved = rs2.getInt("value");
                            Integer idPort = rs2.getInt("id");
                            String sqlUpdateIP = "UPDATE port SET date='" + dtfOut.print(new DateTime()) + "' WHERE id=" + String.valueOf(idPort) + ";"; //value=" + String.valueOf(port) + ",
                            update(sqlUpdateIP);

                            String sqlInsertHistory = "INSERT INTO port_history SET id_port = '" + idPort + "', date = '" + dtfOut.print(new DateTime()) + "';";
                            addHistory(sqlInsertHistory);
                    }
                    rs2.close();
                } while (rs.next());
            }
            Statement stmt = rs.getStatement();
            rs.close();
            stmt.close();
        } catch (Exception e) {
            if (SHOW_DEBUG_MESSAGE) System.out.println("saveip port "+e);
        }
    }

    public void saveNbtName(String mac, String ip, String nbtName) {
        String macId = this.getIdMAC(mac);
        try {
            //Connection con = this.getConnection();
            //Statement stmt = con.createStatement();
            DateTimeFormatter dtfOut = getDateFormat("yyyy-MM-dd HH:mm:ss");
            String sql = "SELECT id from ip where value='" + ip + "' and id_mac='" + macId + "';";
            ResultSet rs = select(sql);
            if (rs.next() == false) {
            } else {
                do {
                    Integer id = rs.getInt("id");
                    String sql2 = "SELECT id,value from nbtname where id_ip=" + String.valueOf(id) + " and value='" + nbtName + "' ;"; //and DATE_FORMAT(date, \"%Y-%m-%d %H\")='"+dtfOut2.print(new DateTime())+"'
                    ResultSet rs2 = select(sql2);
                    if (rs2.next() == false) {
                        String sqlInsertNbtName = "INSERT INTO nbtname SET value='" + String.valueOf(nbtName) + "', date = '" + dtfOut.print(new DateTime()) + "', id_ip = " + String.valueOf(id) + ";";
                        Integer insertedId = insert(sqlInsertNbtName);

                        String sqlInsertHistory = "INSERT INTO nbtname_history SET id_nbtname = '" + insertedId + "', date = '" + dtfOut.print(new DateTime()) + "';";
                        addHistory(sqlInsertHistory);
                    } else {
                            Integer idNbtName = rs2.getInt("id");
                            String sqlUpdateIP = "UPDATE nbtname SET date='" + dtfOut.print(new DateTime()) + "' WHERE id=" + String.valueOf(idNbtName) + ";"; //value=" + String.valueOf(port) + ",
                            update(sqlUpdateIP);

                            String sqlInsertHistory = "INSERT INTO nbtname_history SET id_nbtname = '" + idNbtName + "', date = '" + dtfOut.print(new DateTime()) + "';";
                            addHistory(sqlInsertHistory);
                    }
                    rs2.close();
                } while (rs.next());
            }
            Statement stmt = rs.getStatement();
            rs.close();
            stmt.close();
            //con.close();
        } catch (Exception e) {
            if (SHOW_DEBUG_MESSAGE) System.out.println("save nbtname "+e);
        }
    }

    synchronized public int getCountActiveTaskInQueue() {
        int result = 0;
        String sql = "SELECT count(*) as c from queue where status='" + TaskStatus.LAUNCHED.toString() + "';";
        try {
            ResultSet rs = select(sql);
            if (rs.next() != false) {
                result = Integer.valueOf(rs.getInt("c"));
            }
            Statement stmt = rs.getStatement();
            rs.close();
            stmt.close();
        } catch (Exception e) {
            if (SHOW_DEBUG_MESSAGE) System.out.println(e);
        }
        return result;
    }

    public int getCountTaskInQueue() {
        int result = 0;
        String sql = "SELECT count(*) as c from queue;";
        try {
            ResultSet rs = select(sql);
            if (rs.next() != false) {
                result = Integer.valueOf(rs.getInt("c"));
            }
            Statement stmt = rs.getStatement();
            rs.close();
            stmt.close();
        } catch (Exception e) {
            if (SHOW_DEBUG_MESSAGE) System.out.println(e);
        }
        return result;
    }
}
