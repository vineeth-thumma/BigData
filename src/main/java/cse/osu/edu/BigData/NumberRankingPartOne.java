package cse.osu.edu.BigData;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Scanner;

public class NumberRankingPartOne {
	public static void main( String[] args )
    {
        Scanner sc = new Scanner(System.in);
        int N = sc.nextInt();
        int numLines = (int)(1.5*(double)N);
        PrintWriter out;
		try {
			out = new PrintWriter(new FileWriter("sample_data.txt"));
			 for(int i=0; i<numLines; i++) {
		        	out.println((int )(Math.random()*N + 1));
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
