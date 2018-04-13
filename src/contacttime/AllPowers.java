/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package contacttime;

import fyp_parser.DBclass;
import static fyp_parser.DBclass.connect;
import fyp_parser.Event;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Timestamp;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Queue;
import java.util.TimeZone;

/**
 *
 * @author daniel
 */
public class AllPowers {

    public static final int NUM_DEVICES = 16;
    public static final int FLOOR = -90;
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws FileNotFoundException, IOException {
        if(args.length < 4 && args.length > 6) {
            System.out.println("Incorrect args. Should be dbname floor startnode resolution (now/notnow) (peak/5ptaverage/5ptewma)");
            return;
        }             
        Connection conn = connect(args[0]);
        int threshold = Integer.parseInt(args[1]);
        int startNode = Integer.parseInt(args[2]);
        int resSeconds = Integer.parseInt(args[3]);
        boolean fromNow = args[4].equals("now");
        String mode = args[5];
        
        File file = new File("allpowers" + args[0] + "th" + threshold + "sn" + startNode + "r" + resSeconds + mode + ".csv");
        FileOutputStream fop = new FileOutputStream(file);

        // if file doesnt exists, then create it
        if (!file.exists()) {
                file.createNewFile();
        }
        
        if(args.length == 6) {
            mode = args[5];
        }
        ArrayList<Event> events = DBclass.getContactEvents(conn, threshold, startNode);
        if(events.size() == 0)
        {
            System.out.println("No events returned.");
            return;
        }
        long startTime = events.get(0).UTC*1000;
        long endTime = events.get(events.size()-1).UTC*1000;
        if(fromNow)
            endTime = Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTimeInMillis();
        Timestamp startTS2 = new Timestamp(startTime);
        Timestamp endTS2 = new Timestamp(endTime);
        int resolution = resSeconds*1000;
        int chunks = ((int) (endTime-startTime))/resolution;
        int[][] contact = new int[chunks][NUM_DEVICES];
        for(int i = 0; i < chunks; i++)
            Arrays.fill(contact[i], FLOOR); 
        boolean[] present = new boolean[NUM_DEVICES];
        
        if(mode.equals("peak")) {
            for(Event e : events) {
                int otherTag;
                if(e.tag1 == startNode)
                    otherTag = e.tag2;
                else
                    otherTag = e.tag1;
                int contactIndex = Math.min((int) (((e.UTC*1000-startTime) + resolution/2)/resolution), chunks-1);
                if(contact[contactIndex][otherTag] < e.dBm)
                    contact[contactIndex][otherTag] = e.dBm;
                present[otherTag] = true;
            }
            
            // fill in blank values
            for(int i = 1; i < chunks; i++) {
                int[] prevValues = new int[NUM_DEVICES];
                for(int j = 0; j < NUM_DEVICES;j++) {
                    if(contact[i][j] == FLOOR || contact[i][j] == 0)
                        contact[i][j] = contact[i-1][j];
                }
            }
        }
        else if (mode.equals("5ptaverage")) {
            HashMap<Integer, Queue<Integer>> tagIdTo5LastPoints = new HashMap<>();
            chunks = ((int) (endTime-startTime))/1000;
            contact = new int[chunks][NUM_DEVICES];
            for(int i = 0; i < chunks; i++)
                Arrays.fill(contact[i], 0); 
            for(Event e : events) {
                int otherTag;
                if(e.tag1 == startNode)
                    otherTag = e.tag2;
                else
                    otherTag = e.tag1;
                
                int contactIndex = Math.min((int) (((e.UTC*1000-startTime) + 1000/2)/1000), chunks-1);
                if(!tagIdTo5LastPoints.containsKey(otherTag)) {
                    Queue<Integer> temp = new ArrayDeque<>();
                    temp.add(e.dBm);
                    tagIdTo5LastPoints.put(otherTag, temp);
                }
                Queue<Integer> currentPoints = tagIdTo5LastPoints.get(otherTag);
                if(currentPoints.size() > 5)
                    currentPoints.remove();
                currentPoints.add(e.dBm);
                int average = (int) currentPoints.stream().mapToInt(i -> i).average().orElse(0);
                contact[contactIndex][otherTag] = average;
                present[otherTag] = true;
            }
            // fill in blank values
            for(int i = 1; i < chunks; i++) {
                int[] prevValues = new int[NUM_DEVICES];
                for(int j = 0; j < NUM_DEVICES;j++) {
                    if(contact[i][j] == FLOOR || contact[i][j] == 0)
                        contact[i][j] = contact[i-1][j];
                }
            }
        }
        else if (mode.equals("5ptewma")) {
            HashMap<Integer, Queue<Integer>> tagIdTo5LastPoints = new HashMap<>();
            chunks = ((int) (endTime-startTime))/1000;
            contact = new int[chunks][NUM_DEVICES];
            for(int i = 0; i < chunks; i++)
                Arrays.fill(contact[i], 0); 
            for(Event e : events) {
                int otherTag;
                if(e.tag1 == startNode)
                    otherTag = e.tag2;
                else
                    otherTag = e.tag1;
                
                int contactIndex = Math.min((int) (((e.UTC*1000-startTime) + 1000/2)/1000), chunks-1);
                if(!tagIdTo5LastPoints.containsKey(otherTag)) {
                    Queue<Integer> temp = new ArrayDeque<>();
                    temp.add(e.dBm);
                    tagIdTo5LastPoints.put(otherTag, temp);
                }
                Queue<Integer> currentPoints = tagIdTo5LastPoints.get(otherTag);
                if(currentPoints.size() > 5)
                    currentPoints.remove();
                currentPoints.add(e.dBm);
                double oldValue = 0.0;
                double alpha = 0.2;
                for(Integer i : currentPoints) {
                    if(oldValue == 0.0)
                        oldValue = i;
                    else
                        oldValue = oldValue + alpha* ((double) i - oldValue);
                }
                
                contact[contactIndex][otherTag] = (int)oldValue;
                present[otherTag] = true;
            }
            // fill in blank values
            for(int i = 1; i < chunks; i++) {
                int[] prevValues = new int[NUM_DEVICES];
                for(int j = 0; j < NUM_DEVICES;j++) {
                    if(contact[i][j] == FLOOR || contact[i][j] == 0)
                        contact[i][j] = contact[i-1][j];
                }
            }
        }
        
        String csvHeader = "datetime,";
        for(int i = 0; i < NUM_DEVICES; i++) {
            if(present[i])
                csvHeader += String.format("%1X", i) + ",";
        }
        csvHeader += "main contact,\n";
        fop.write(csvHeader.getBytes());
        fop.flush();
        
        int prevMainContact = -1;
        for(int i = 0; i < chunks; i++) {
            String csvLine = "";
            String line = "" + new Timestamp(startTime + i*resolution);
            csvLine += "" + new Timestamp(startTime + i*resolution) + ",";
            int maxDBm = -100;
            int mainContact = -1;
            for(int j = 0; j < NUM_DEVICES; j++) {
                if(present[j] && contact[i][j] > FLOOR && contact[i][j] != 0) {
                        line += "   " + String.format("%03d", contact[i][j]);
                        csvLine += "" + contact[i][j] + ",";
                    if(contact[i][j] >= maxDBm) {
                        maxDBm = contact[i][j];
                        mainContact = j;
                        prevMainContact = j;
                    }
                }
                else if (present[j])
                {
                        line += "      ";
                        csvLine += ",";
                }
            }
            if(mainContact == -1) {
                line += "    " + prevMainContact;
                csvLine += prevMainContact +"\n";
            }
            else {
                line += "    " + mainContact;
                csvLine += mainContact +"\n";
            }
            fop.write(csvLine.getBytes());
            fop.flush();
            System.out.println(line);
        }
        
        System.out.println("START=" + startTS2);
        System.out.println("END=  " + endTS2);
        fop.close();
    }
    
}
