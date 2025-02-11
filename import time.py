import time
import os
import csv
import re
import certifi
from urllib.parse import urljoin
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- Configuration ---
BASE_URL = "https://appealsdecisions.dol.ks.gov/DocumentRetriever.aspx"
DOWNLOAD_FOLDER = r"G:\My Drive\LexiMentis scraped data"  # update as needed
CSV_OUTPUT = "appeals_data.csv"
EXCEL_OUTPUT = "appeals_data.xlsx"

# Create download folder if it doesn't exist
if not os.path.exists(DOWNLOAD_FOLDER):
    os.makedirs(DOWNLOAD_FOLDER)

# --- Helper function to clean file names for Windows ---
def clean_file_name(file_name):
    # Replace invalid characters for Windows: < > : " / \ | ? *
    return re.sub(r'[<>:"/\\|?*]', '_', file_name)

# --- Setup Selenium WebDriver (using Chrome) ---
driver = webdriver.Chrome()  # Make sure chromedriver is in your PATH
driver.get(BASE_URL)

# Wait for the main table to load
wait = WebDriverWait(driver, 10)
wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.table")))

# --- Create a requests session and copy Selenium cookies into it ---
pdf_session = requests.Session()
for cookie in driver.get_cookies():
    pdf_session.cookies.set(cookie['name'], cookie['value'])

# --- Function to process all records on the current page ---
def process_current_page():
    page_records = []
    tbody = driver.find_element(By.CSS_SELECTOR, "table.table tbody")
    rows = tbody.find_elements(By.TAG_NAME, "tr")
    
    # We'll iterate through rows and look for record rows (which contain a PDF link)
    i = 0
    while i < len(rows):
        row = rows[i]
        try:
            # Check for PDF link cell
            pdf_link_elem = row.find_element(By.CSS_SELECTOR, "td.shorter-width-column a")
            # If found, assume this row contains the record info
            appeals_number = row.find_element(By.CSS_SELECTOR, "td.appealsId-column b").text.strip()
            file_name_text = pdf_link_elem.text.strip()
            order_date = row.find_elements(By.TAG_NAME, "td")[2].text.strip()  # third cell
            rel_pdf_url = pdf_link_elem.get_attribute("href")
            pdf_url = urljoin(BASE_URL, rel_pdf_url)
        except Exception as e:
            # Skip rows that do not match the record structure
            i += 1
            continue

        # Default summary info
        issue_text = ""
        holding_text = ""
        # Check if there is a following row with the summary
        if i + 1 < len(rows):
            summary_row = rows[i + 1]
            try:
                toggle_link = summary_row.find_element(By.CSS_SELECTOR, "a.toggle-summary")
                # Click the toggle link to reveal summary content
                driver.execute_script("arguments[0].click();", toggle_link)
                time.sleep(0.5)  # Allow time for collapse to expand
                data_target = toggle_link.get_attribute("data-target")  # e.g., "#row0details"
                summary_div = driver.find_element(By.CSS_SELECTOR, data_target)
                issue_text = summary_div.find_element(By.CSS_SELECTOR, ".col-sm-4 p").text.strip()
                holding_text = summary_div.find_element(By.CSS_SELECTOR, ".col-sm-6 p").text.strip()
            except Exception as e:
                print("Error processing summary for record", pdf_url, ":", e)
        else:
            print("No summary row found for record", pdf_url)
        
        # Download the PDF file
        try:
            pdf_response = pdf_session.get(pdf_url, verify=False)
            if pdf_response.status_code == 200:
                safe_file_name = clean_file_name(file_name_text)
                file_path = os.path.join(DOWNLOAD_FOLDER, safe_file_name)
                with open(file_path, "wb") as f:
                    f.write(pdf_response.content)
                print("Downloaded PDF:", safe_file_name)
            else:
                print("Failed to download PDF from", pdf_url, "Status:", pdf_response.status_code)
        except Exception as e:
            print("Exception downloading PDF:", pdf_url, "Error:", e)
        
        # Append record data
        page_records.append({
            "Appeals Number": appeals_number,
            "File Name": file_name_text,
            "Order Date": order_date,
            "Issue": issue_text,
            "Holding": holding_text,
            "PDF URL": pdf_url
        })
        i += 2  # Skip the summary row and move to the next record row
    return page_records

# --- Main Loop: Process pages ---
all_records = []
# Get total pages and current page number from the pagination elements
total_pages_elem = driver.find_element(By.ID, "lblTotalPagesTop")
total_pages = int(total_pages_elem.text)
current_page_elem = driver.find_element(By.ID, "lblPageNumberTop")
current_page = int(current_page_elem.text)

while current_page <= total_pages:
    print(f"Processing page {current_page} of {total_pages}...")
    records = process_current_page()
    all_records.extend(records)
    
    if current_page < total_pages:
        # Click the Next button
        next_btn = driver.find_element(By.ID, "btnNextTop")
        driver.execute_script("arguments[0].click();", next_btn)
        # Wait until the page number updates to current_page + 1
        WebDriverWait(driver, 10).until(
            lambda d: int(d.find_element(By.ID, "lblPageNumberTop").text) == current_page + 1
        )
        current_page += 1
        time.sleep(1)  # Additional delay to let content settle
    else:
        break

driver.quit()

# --- Save data to CSV ---
fieldnames = ["Appeals Number", "File Name", "Order Date", "Issue", "Holding", "PDF URL"]
with open(CSV_OUTPUT, "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for record in all_records:
        writer.writerow(record)
print(f"Data saved to CSV file: {CSV_OUTPUT}")

# --- Save data to Excel using pandas (optional) ---
try:
    import pandas as pd
    df = pd.DataFrame(all_records, columns=fieldnames)
    df.to_excel(EXCEL_OUTPUT, index=False)
    print(f"Data saved to Excel file: {EXCEL_OUTPUT}")
except ImportError:
    print("Pandas or openpyxl not installed; Excel file not created.")
