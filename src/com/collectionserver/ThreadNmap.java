package com.collectionserver;

import org.ini4j.Ini;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;

class ThreadNmap extends Thread {

    Task task;
    nmap nmap;
    mysqldb db;
    xmlParam xmlParam;
    private boolean SHOW_DEBUG_MESSAGE;

    ThreadNmap(Task task, mysqldb db) throws IOException {
        super(String.valueOf(task.getQueueId()));
        Ini ini = new Ini(new File("config.ini"));
        SHOW_DEBUG_MESSAGE = Boolean.valueOf(ini.get("config", "SHOW_DEBUG_MESSAGE"));
        this.task = task;
        this.db = db;
        this.xmlParam = new xmlParam(null);
        ini = null;
    }

    public void run(){

        if (SHOW_DEBUG_MESSAGE) System.out.printf("%s started... \n", Thread.currentThread().getName());
        try{
            int benchMark;
            DateTime startThreadTime = new DateTime();
            nmap = new nmap();
            task.setXmlResult(nmap.run(task.getCommand()));
            task.setStatus(TaskStatus.PARSING);
            //db.saveTask(task);
            db.updateQueuedTask(task);
            Thread.sleep(500);
            this.parsingXML(task.getXmlResult());
            task.setStatus(TaskStatus.COMPLETED);
            db.deleteQueuedTask(task);
            DateTime endThreadTime = new DateTime();
            benchMark = Seconds.secondsBetween(startThreadTime, endThreadTime).getSeconds();
            task.setDuration(benchMark);
            db.saveTask(task);
            nmap = null;
        }
        catch(InterruptedException e){
            if (SHOW_DEBUG_MESSAGE) System.out.println("Thread has been interrupted");
        }
        if (SHOW_DEBUG_MESSAGE) System.out.printf("%s fiished... \n", Thread.currentThread().getName());
    }

    public void parsingXML(String xml){
        try{
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            InputSource is = new InputSource(new StringReader(xml));
            Document doc = builder.parse(is);
            doc.getDocumentElement().normalize();

            Node root = doc.getDocumentElement();

            NodeList books = root.getChildNodes();
            for (int i = 0; i < books.getLength(); i++) {
                Node book = books.item(i);
                if (book.getNodeType() != Node.TEXT_NODE) {

                    if (book.getNodeName().equals("host")){
                        Node l2m = this.xmlParam.getNodeWithEqualAttrVal(book,"address","addrtype","mac");
                        if (l2m!=null) {
                            String mac = this.xmlParam.getAttrValue(l2m, "addr");
                            String vendor = this.xmlParam.getAttrValue(l2m, "vendor");
                            db.saveMac(mac);
                            db.saveVendor(mac, vendor);
                            Node l2i = this.xmlParam.getNodeWithEqualAttrVal(book, "address", "addrtype", "ipv4");
                            String ip = this.xmlParam.getAttrValue(l2i, "addr");
                            db.saveIP(mac, ip);
                        }
                    }

                    if (book.getNodeName().equals("host")) {
                        Node l2m = this.xmlParam.getNodeWithEqualAttrVal(book,"address","addrtype","mac");
                        if (l2m!=null) {
                            String mac = this.xmlParam.getAttrValue(l2m, "addr");
                            Node l2i = this.xmlParam.getNodeWithEqualAttrVal(book, "address", "addrtype", "ipv4");
                            String ip = this.xmlParam.getAttrValue(l2i, "addr");
                            NodeList childHost = book.getChildNodes();
                            for (int j = 0; j < childHost.getLength(); j++) {
                                Node bookProp = childHost.item(j);
                                if (bookProp.getNodeName().equals("ports")) {
                                    NodeList chilePorts = bookProp.getChildNodes();
                                    for (int p = 0; p < chilePorts.getLength(); ++p) {
                                        Node portsNode = chilePorts.item(p);
                                        if (portsNode.getNodeName().equals("port")) {
                                            String port = this.xmlParam.getAttrValue(portsNode, "portid");
                                            Node s = this.xmlParam.getNodeWithEqualAttrVal(portsNode, "service", "method", "table");
                                            String desc = this.xmlParam.getAttrValue(s, "name");
                                            Node stNode = this.xmlParam.getFirstEqualNode(portsNode, "state");
                                            String status = this.xmlParam.getAttrValue(stNode, "state");
                                            if (status.equals("open")) {
                                                db.saveIPPort(mac, ip, port, desc);
                                            } else {
                                                //System.out.println("port " + port + " is closed");
                                            }
                                        }
                                    }

                                }
                                if (bookProp.getNodeName().equals("hostscript")) {
                                    NodeList childHostScriptNode = bookProp.getChildNodes();
                                    for (int p = 0; p < childHostScriptNode.getLength(); ++p) {
                                        Node nbtNode = childHostScriptNode.item(p);
                                        if (nbtNode.getNodeName().equals("script")) {
                                            String nbtInfo = this.xmlParam.getAttrValue(nbtNode, "output");
                                            String[] nbtInfoArray = nbtInfo.split(", ");
                                            for (String item : nbtInfoArray) {
                                                String fieldName = item.split(":")[0];
                                                if (fieldName.equals("NetBIOS name")) {
                                                    String nbtName = item.split(":")[1];
                                                    db.saveNbtName(mac, ip, nbtName.trim());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

        }
        catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        }
    }
}
