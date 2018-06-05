package org.julien.datastuff;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

public class Generator {

	private final List<String> firstNamesMale;
	private final List<String> firstNamesFemale;
	private final List<String> lastNames;
	private final List<String> states;
	private final Random rand;

	public List<String> getFirstNamesMale() {
		return firstNamesMale;
	}

	public List<String> getFirstNamesFemale() {
		return firstNamesFemale;
	}

	public List<String> getLastNames() {
		return lastNames;
	}

	public List<String> getStates() {
		return states;
	}

	public Random getRand() {
		return rand;
	}

	public Generator() {
		firstNamesMale = new ArrayList<String>();
		firstNamesFemale = new ArrayList<String>();
		lastNames = new ArrayList<String>();
		states = new ArrayList<String>();
		rand = new Random(System.currentTimeMillis());
	}

	/**
	 * This methods loads strings from a text file into a list
	 * 
	 * @param list
	 *            the list to store strings into
	 * @param filename
	 *            the file to load strings from
	 * @param delimiter
	 *            the field delimiter to use for the file
	 */
	private void load(List<String> list, String filename, String delimiter) {
		Scanner sc = null;
		try {
			sc = new Scanner(new FileReader(filename));
			sc.useDelimiter(delimiter);
			while (sc.hasNext()) {
				String line = sc.nextLine();
				String name = line.split(delimiter)[0];
				list.add(name);
			}
			sc.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		System.out.println("Loaded " + list.size() + " strings from "
				+ filename);
	}

	
	/**
	 * This method builds the a log line.
	 * 
	 * @param delimiter
	 *            the field delimiter to use in the output string
	 * @return a complete line of text data
	 */
	private String buildLine(String delimiter) {
		StringBuffer sb = new StringBuffer();
		float multiplier = 1f;
		
		// Add last name
		sb.append(getLastNames().get(rand.nextInt(getLastNames().size())));
		sb.append(delimiter);
		
		// Add first name and gender
		int gender = rand.nextInt(2);
		if (gender == 0) {
			// Ladies first ;)
			sb.append(getFirstNamesFemale().get(
					rand.nextInt(getFirstNamesFemale().size())));
			sb.append(delimiter).append("F").append(delimiter);
		} else {
			sb.append(getFirstNamesMale().get(
					rand.nextInt(getFirstNamesMale().size())));
			sb.append(delimiter).append("M").append(delimiter);
		}
		
		if (gender == 0) {
			multiplier *= 1.25f;
		}
				
		// Add state
		String state = getStates().get(rand.nextInt(getStates().size()));
		sb.append(state);
		sb.append(delimiter);
		
		switch (state) {
		case "California":
		case "Florida":
			multiplier *= 1.50f;
			break;
		case "New York":
			multiplier *= 1.75f;
			break;
		case "District of Columbia":
			multiplier *= 2f;
			break;
		}
		
		// Add age
		int age = 18 + rand.nextInt(70);
		sb.append(age).append(delimiter);
		
		if ((age > 25) && (age < 45)) {
			multiplier *= 1.2f;
		}
		
		// Add day of year
		int day = rand.nextInt(365);
		sb.append(1 + day).append(delimiter);
		
		if ( (day > 305) && (day <=335)) {
			multiplier *= 1.5f;
		}
		else if ( (day > 335) && (day <=350)) {
			multiplier *= 2f;
		} else if (day > 350) {
			multiplier *= 2.5f;
		}
		
		// Add hour and minutes
		sb.append(1 + rand.nextInt(24)).append(delimiter);
		sb.append(1 + rand.nextInt(60)).append(delimiter);
		
		// Add number of items purchased
		int items = 1 + rand.nextInt(10);
		sb.append(items).append(delimiter);
		
		// Add basket price
				
		sb.append(Math.abs((int)(rand.nextGaussian()*50+(100*multiplier))));
		sb.append("\n");
		return sb.toString();
	}

	/**
	 * This method builds a randomly-generated CSV file
	 * @param filename the file to store data in
	 * @param lines the number of lines to store
	 */
	private void save(String filename, int lines) {
		try {
			PrintWriter writer = new PrintWriter(filename, "UTF-8");
			for (int i = 0; i < lines; i++) {
				writer.write(buildLine(","));
			}
			writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}

	public static void main(String args[]) {
		Generator g = new Generator();

		g.load(g.getLastNames(), "dist.all.last", " ");
		g.load(g.getFirstNamesMale(), "dist.male.first", " ");
		g.load(g.getFirstNamesFemale(), "dist.female.first", " ");
		g.load(g.getStates(), "US_States.txt", ",");

		g.save("data-batch-predicton.txt", 1000);
		System.out.println("Done.");
	}
}
