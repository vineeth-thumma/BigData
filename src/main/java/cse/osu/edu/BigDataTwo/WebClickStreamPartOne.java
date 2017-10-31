package cse.osu.edu.BigDataTwo;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Scanner;

public class WebClickStreamPartOne {
	public static void main( String[] args )
    {
        Scanner sc = new Scanner(System.in);
        int userNumbers = sc.nextInt();
        int webPageNumbers = sc.nextInt();
        int webCategoryNumbers = sc.nextInt();
        int timeRange = sc.nextInt();
        
        int numLines = 1000*userNumbers;
        PrintWriter out;
		try {
			out = new PrintWriter(new FileWriter("data.txt"));
			 for(int i=0; i<numLines; i++) {
		        	out.println((int)(Math.random()*userNumbers + 1)+" "+(int)(Math.random()*webPageNumbers + 1)
		        			+" "+(int)(Math.random()*webCategoryNumbers + 1)+" "+(float)(Math.random()*timeRange+ 1));
		      }
			 out.close();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		sc.close();
    }

}
