import requests
from bs4 import BeautifulSoup
import json
import csv

# Base URL for Kansas Legislature Statutes
BASE_URL = "https://www.ksrevisor.org"

# URL for Chapter 44 (Workers' Compensation)
CHAPTER_44_URL = f"{BASE_URL}/statutes/chapters/ch44/"

# Headers to mimic a real browser request
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# Function to get all section links from Chapter 44 page
def get_section_links():
    response = requests.get(CHAPTER_44_URL, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")
    
    section_links = []
    for link in soup.select("a[href^='/statutes/chapters/view/']"):  # Find section links
        section_url = BASE_URL + link['href']
        section_title = link.text.strip()
        section_links.append((section_title, section_url))

    return section_links

# Function to extract statute text from each section
def extract_statute_text(section_url):
    response = requests.get(section_url, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")

    # Extract the statute text
    statute_text = " ".join([p.text.strip() for p in soup.select("p")])

    return statute_text

# Scrape all sections and save in JSON, CSV, and TXT
def scrape_chapter_44():
    sections = get_section_links()
    data = []

    for section_title, section_url in sections:
        print(f"Scraping: {section_title} -> {section_url}")
        statute_text = extract_statute_text(section_url)

        # Store data
        data.append({
            "Section": section_title,
            "URL": section_url,
            "Text": statute_text
        })

    # Save as JSON
    with open("kansas_ch44.json", "w", encoding="utf-8") as json_file:
        json.dump(data, json_file, indent=4)

    # Save as CSV
    with open("kansas_ch44.csv", "w", encoding="utf-8", newline="") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["Section", "URL", "Text"])
        for entry in data:
            writer.writerow([entry["Section"], entry["URL"], entry["Text"]])

    # Save as TXT
    with open("kansas_ch44.txt", "w", encoding="utf-8") as txt_file:
        for entry in data:
            txt_file.write(f"Section: {entry['Section']}\n")
            txt_file.write(f"URL: {entry['URL']}\n")
            txt_file.write(f"Text:\n{entry['Text']}\n")
            txt_file.write("="*80 + "\n\n")

    print("Scraping complete! Data saved as JSON, CSV, and TXT.")

# Run the scraper
scrape_chapter_44()
