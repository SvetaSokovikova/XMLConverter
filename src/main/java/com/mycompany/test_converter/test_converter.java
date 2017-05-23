package com.mycompany.test_converter;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.BitSet;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;

public class test_converter {
    public static void main(String[] args){
        System.setProperty("entityExpansionLimit", "10000000");
        
        String XMLFileName = "C:/DBLP/dblp-2016-11-02.xml"; //Name of xml file (parameter)
        String csvFileName = "C:/Users/User/Desktop/dblp1percKmeans.csv"; //Name of csv file with types (parameter)
        String url = "jdbc:postgresql://127.0.0.1:5432/dblp1perc"; // Name of table database (parameter)
        double ratio = 0.01; //Ratio (parameter)
        
        String user = "postgres"; // Username in Postgres (parameter)
        String password = "Anjelika"; //Password in Postgres (parameter)
        Connection conn = null;
        Statement stmt = null;
        
        try{
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(url, user, password);
            stmt = conn.createStatement();
            stmt.execute("create table navigate(unique_identifier bigint primary key, type integer)");
        } catch(Exception e){
            e.printStackTrace();
        }
                
        try{
            XMLStreamReader xmlr = XMLInputFactory.newInstance().createXMLStreamReader(XMLFileName, new FileInputStream(XMLFileName));
            
            int nBegins = 0;
            ArrayList<java.lang.Integer> startTags = new ArrayList();
            
            BitSet isLeaf = new BitSet();
            
            while (xmlr.hasNext()){
                xmlr.next();
                
                if (xmlr.isStartElement()){
                    nBegins++;
                    startTags.add(nBegins);
                }
                
                if (xmlr.isCharacters() && xmlr.getText().trim().length()>0){
                    isLeaf.set(startTags.get(startTags.size()-1));
                }
                
                if (xmlr.isEndElement()){
                    startTags.remove(startTags.size()-1);
                }  
            }
            
                        
            Map<String, java.lang.Integer> howManyMustBe = new HashMap();
            ArrayList<String> attrs_stack = new ArrayList();
            nBegins = 0;
            int lvl = 0;
            int k;
            int L;
            int until;
            startTags.clear();
            int inside_lvl;
            HashSet<String> multipled_attributes = new HashSet();
            
            xmlr = XMLInputFactory.newInstance().createXMLStreamReader(XMLFileName, new FileInputStream(XMLFileName));
            
            while (xmlr.hasNext()){
                xmlr.next();
                
                if (xmlr.isStartElement()){
                    nBegins++;
                    startTags.add(nBegins);
                    if (!isLeaf.get(nBegins)){
                        if (lvl>1){
                            if (attrs_stack.lastIndexOf(xmlr.getLocalName()+"_id") > attrs_stack.lastIndexOf("$"))
                                multipled_attributes.add(xmlr.getLocalName()+"_id");
                            attrs_stack.add(xmlr.getLocalName() +"_id");
                        }
                        attrs_stack.add("$");
                        attrs_stack.add("unique_identifier");
                        for (int i=0; i<xmlr.getAttributeCount(); i++)
                            attrs_stack.add(xmlr.getAttributeLocalName(i));
                        if (!howManyMustBe.keySet().contains(xmlr.getLocalName()))
                            howManyMustBe.put(xmlr.getLocalName(), 1);
                        else {
                            k = howManyMustBe.get(xmlr.getLocalName());
                            howManyMustBe.put(xmlr.getLocalName(), k+1);
                        }
                    }
                    else{
                        if (attrs_stack.lastIndexOf(xmlr.getLocalName()) > attrs_stack.lastIndexOf("$"))
                            multipled_attributes.add(xmlr.getLocalName());
                        attrs_stack.add(xmlr.getLocalName());
                        
                        inside_lvl = 1;
                        do{
                            xmlr.next();
                            if (xmlr.isStartElement()){
                                inside_lvl++;
                                nBegins++;
                            }
                            if (xmlr.isEndElement())
                                inside_lvl--;
                        }
                        while (!(xmlr.isEndElement() && inside_lvl==0));
                    }
                    lvl++;
                }
                
                if (xmlr.isEndElement()){
                    if (!isLeaf.get(startTags.get(startTags.size()-1)) && lvl!=1){
                        L = attrs_stack.size();
                        until = attrs_stack.lastIndexOf("$");
                        for (int i=L-1; i>=until; i--){
                            attrs_stack.remove(i);
                        }
                    }
                    startTags.remove(startTags.size()-1);
                    lvl--;     
                }
                
            }
            
            
            
            for (String str: howManyMustBe.keySet()){
                k = howManyMustBe.get(str);
                howManyMustBe.put(str, (int)(k*ratio));
            }
            
            Map<String,java.lang.Integer> howManyThereAre = new HashMap();
            
            for (String str: howManyMustBe.keySet())
                howManyThereAre.put(str, 0);
            
            howManyMustBe.put("mastersthesis", 0);
            
            
            
            BufferedReader br = new BufferedReader(new FileReader(csvFileName));
            nBegins = 0;
            lvl = 0;
            startTags.clear();
            attrs_stack.clear();
            ArrayList<String> values_stack = new ArrayList();
            ArrayList<String> id_stack = new ArrayList();
            int id = -1;
            String s;
            String s1;
            String attr;
            String tmp;
            String query;
            String attr_names_fragment;
            String values_fragment;
            Map<String, HashSet<String>> columns = new HashMap();
            Map<String, String> stepQuery = new HashMap();
            int progress = 0;
            
            xmlr = XMLInputFactory.newInstance().createXMLStreamReader(XMLFileName, new FileInputStream(XMLFileName));
            
            while (xmlr.hasNext()){
                xmlr.next();
                
                if (xmlr.isStartElement()){
                    nBegins++;
                    startTags.add(nBegins);
                    if (!isLeaf.get(nBegins)){
                        id++;
                        if (lvl>1){
                            attrs_stack.add(xmlr.getLocalName() +"_id");
                            values_stack.add(String.valueOf(id));
                        }
                        attrs_stack.add("$");
                        values_stack.add("$");
                        attrs_stack.add("unique_identifier");
                        values_stack.add(String.valueOf(id));
                        id_stack.add(String.valueOf(id));
                        for (int i=0; i<xmlr.getAttributeCount(); i++){
                            attrs_stack.add(xmlr.getAttributeLocalName(i));
                            values_stack.add("'"+xmlr.getAttributeValue(i).replace("\\", "\\\\").replace("'", "''")+"'");
                        }
                    }
                    else {
                        attrs_stack.add(xmlr.getLocalName());
                        s = "'";
                        
                        inside_lvl = 1;
                        do{
                            xmlr.next();
                            if (xmlr.isStartElement()){
                                inside_lvl++;
                                nBegins++;
                                s += "<"+xmlr.getLocalName()+">";
                            }
                            
                            if (xmlr.isCharacters() && xmlr.getText().trim().length()>0){
                                s += xmlr.getText().replace("\\", "\\\\").replace("'", "''");
                            }
                            
                            if (xmlr.isEndElement()){
                                inside_lvl--;
                                if (inside_lvl != 0)
                                    s += "</"+xmlr.getLocalName()+">";
                            }
                        }
                        while (!(xmlr.isEndElement() && inside_lvl==0));
                        
                        s += "'";
                        values_stack.add(s);
                    }
                    lvl++;
                }
                
                if (xmlr.isEndElement()){
                    if (!isLeaf.get(startTags.get(startTags.size()-1)) && lvl!=1){
                        if (howManyThereAre.get(xmlr.getLocalName()) < howManyMustBe.get(xmlr.getLocalName())){
                            k = howManyThereAre.get(xmlr.getLocalName());
                            howManyThereAre.put(xmlr.getLocalName(), k+1);
                            stepQuery.clear();
                            s = br.readLine();
                            stmt.execute("insert into navigate (unique_identifier, type) values ("
                                    +id_stack.remove(id_stack.size()-1)+","+s+")");
                            if (!columns.containsKey(s)){
                                columns.put(s, new HashSet());
                                columns.get(s).add("unique_identifier");
                                stmt.execute("create table t_"+s+" (unique_identifier bigint primary key)");
                            }
                            L = attrs_stack.size();
                            until = attrs_stack.lastIndexOf("$");
                            for (int i=L-1; i>until; i--){
                                attr = attrs_stack.get(i);
                                if (!columns.get(s).contains(attr)){
                                    if (multipled_attributes.contains(attr))
                                        stmt.execute("alter table t_"+s+" add "+attr+" jsonb NULL");
                                    else 
                                        if (attr.endsWith("_id"))
                                            stmt.execute("alter table t_"+s+" add "+attr+" bigint NULL");
                                        else 
                                            stmt.execute("alter table t_"+s+" add "+attr+" text NULL");
                                    columns.get(s).add(attr);
                                }
                                   
                                if (!stepQuery.containsKey(attr))
                                    if (multipled_attributes.contains(attr)){
                                        s1 = values_stack.get(i);
                                        if (s1.startsWith("'"))
                                            s1 = "\""+s1.substring(1,s1.length()-1).replace("\"", "\\\"")+"\"";
                                        stepQuery.put(attr, "'["+s1+",");
                                    }
                                    else 
                                        stepQuery.put(attr, values_stack.get(i));
                                else{
                                    tmp = stepQuery.get(attr);
                                    s1 = values_stack.get(i);
                                    if (s1.startsWith("'"))
                                        s1 = "\""+s1.substring(1,s1.length()-1).replace("\"", "\\\"")+"\"";
                                    tmp += s1+",";
                                    stepQuery.put(attr, tmp);
                                    //stepQuery.get(attr).concat(values_stack.get(i).replace("'", "\"")+",");
                                }
                                attrs_stack.remove(i);
                                values_stack.remove(i);
                            }
                            attr_names_fragment = "(";
                            values_fragment = "(";
                            for (String str: stepQuery.keySet()){
                                tmp = stepQuery.get(str);
                                if (multipled_attributes.contains(str)){
                                    tmp = tmp.substring(0, tmp.length()-1);
                                    tmp += "]'";
                                }
                                attr_names_fragment += str+",";
                                values_fragment += tmp+",";
                            }
                            attr_names_fragment = attr_names_fragment.substring(0, attr_names_fragment.length()-1);
                            attr_names_fragment += ")";
                            values_fragment = values_fragment.substring(0, values_fragment.length()-1);
                            values_fragment += ")";
                            query = "insert into t_"+s+" "+attr_names_fragment+" values "+values_fragment;
                            stmt.execute(query);
                            progress++;
                            System.out.println(progress);
                            attrs_stack.remove(until);
                            values_stack.remove(until);    
                        }
                        else {
                            L = attrs_stack.size();
                            until = attrs_stack.lastIndexOf("$");
                            for (int i=L-1; i>=until; i--){
                                attrs_stack.remove(i);
                                values_stack.remove(i);
                            }
                            id_stack.remove(id_stack.size()-1);
                        }
                    }
                    startTags.remove(startTags.size()-1);
                    lvl--;
                }
            }
                        
        }
        catch(Exception e){
            e.printStackTrace();
        }
        
        
    }
}
