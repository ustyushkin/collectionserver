package com.collectionserver;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class xmlParam {
    public xmlParam(String xml) {
    }
    public Node getFirstEqualNode(Node root,String tag){
        Node result = null;
        NodeList childNodes = root.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node cnItems = childNodes.item(i);
            if (cnItems.getNodeType() != Node.TEXT_NODE && cnItems.getNodeName()==tag) {
                result = cnItems;
            }
        }
        return result;
    }
    public Node getNodeWithEqualAttrVal(Node root,String tag,String Attr,String Val){
        Node result = null;
        NodeList childNodes = root.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node cnItems = childNodes.item(i);
            if (cnItems.getNodeType() != Node.TEXT_NODE && cnItems.getNodeName()==tag) {
                NamedNodeMap nodeAttr = cnItems.getAttributes();
                for (int j = 0; j < nodeAttr.getLength(); ++j)
                {
                    Node attr = nodeAttr.item(j);
                    if (attr.getNodeName().equalsIgnoreCase( Attr ) &&  attr.getNodeValue().equalsIgnoreCase( Val )){
                        result = cnItems;
                    }
                }
            }
        }
        return result;
    }

    public String getAttrValue(Node root,String Attr){
        String result = null;
        NamedNodeMap nodeAttr = root.getAttributes();
        for (int j = 0; j < nodeAttr.getLength(); ++j)
        {
            Node attr = nodeAttr.item(j);
            if (attr.getNodeName().equalsIgnoreCase( Attr )){
                result = attr.getNodeValue();
                break;
            }
        }
        return result;
    }
}
