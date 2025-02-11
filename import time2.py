import time
import os
import re
from urllib.parse import urljoin
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- Configuration ---
BASE_URL = "https://appealsdecisions.dol.ks.gov/DocumentRetriever.aspx"
# If you want to save the Excel file in a specific folder, update the following:
OUTPUT_EXCEL = r"G:\My Drive\LexiMentis scraped data\appeals_summary.xlsx"

# --- Setup Selenium WebDriver (using Chrome) ---
driver = webdriver.Chrome()  # Or specify the full path with executable_path if needed
driver.get(BASE_URL)

# Wait until the table is present
wait = WebDriverWait(driver, 15)
wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.table")))

# --- Helper function to sanitize file names (if needed) ---
def clean_file_name(file_name):
    # Replace characters not allowed in Windows filenames
    return re.sub(r'[<>:"/\\|?*]', '_', file_name)

# --- Function to process records on the current page ---
def process_current_page():
    page_records = []
    tbody = driver.find_element(By.CSS_SELECTOR, "table.table tbody")
    # Get all <tr> elements; filter out empty rows
    rows = [row for row in tbody.find_elements(By.TAG_NAME, "tr") if row.get_attribute("innerText").strip() != ""]
    
    i = 0
    while i < len(rows):
        # The record row (with basic info)
        try:
            # PDF link row is expected to contain a <td> with class "shorter-width-column"
            pdf_link_elem = rows[i].find_element(By.CSS_SELECTOR, "td.shorter-width-column a")
            appeals_number = rows[i].find_element(By.CSS_SELECTOR, "td.appealsId-column b").text.strip()
            file_name_text = pdf_link_elem.text.strip()
            # Assuming the order date is in the third <td> cell
            order_date = rows[i].find_elements(By.TAG_NAME, "td")[2].text.strip()
            rel_pdf_url = pdf_link_elem.get_attribute("href")
            pdf_url = urljoin(BASE_URL, rel_pdf_url)
        except Exception as e:
            # If this row is not in the expected format, skip it.
            i += 1
            continue

        # Default summary values
        issue_text = ""
        holding_text = ""
        # The following row should be the summary row
        if i + 1 < len(rows):
            try:
                summary_row = rows[i + 1]
                toggle_link = summary_row.find_element(By.CSS_SELECTOR, "a.toggle-summary")
                # Click the toggle to reveal the summary details
                driver.execute_script("arguments[0].click();", toggle_link)
                # Get the target selector from data-target attribute (e.g., "#row0details")
                data_target = toggle_link.get_attribute("data-target")
                # Wait until the summary div is visible
                wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, data_target)))
                summary_div = driver.find_element(By.CSS_SELECTOR, data_target)
                issue_elem = summary_div.find_element(By.CSS_SELECTOR, ".col-sm-4 p")
                holding_elem = summary_div.find_element(By.CSS_SELECTOR, ".col-sm-6 p")
                issue_text = issue_elem.text.strip()
                holding_text = holding_elem.text.strip()
            except Exception as e:
                print("Error processing summary for record", pdf_url, ":", e)
        else:
            print("No summary row for record", pdf_url)
        
        page_records.append({
            "Appeals Number": appeals_number,
            "File Name": file_name_text,
            "Order Date": order_date,
            "Issue": issue_text,
            "Holding": holding_text,
            "PDF URL": pdf_url
        })
        i += 2  # Skip the summary row
    return page_records

# --- Main Loop: Process all pages ---
all_records = []
total_pages = int(driver.find_element(By.ID, "lblTotalPagesTop").text)
current_page = int(driver.find_element(By.ID, "lblPageNumberTop").text)

while current_page <= total_pages:
    print(f"Processing page {current_page} of {total_pages}...")
    records = process_current_page()
    all_records.extend(records)
    
    if current_page < total_pages:
        next_btn = driver.find_element(By.ID, "btnNextTop")
        driver.execute_script("arguments[0].click();", next_btn)
        WebDriverWait(driver, 15).until(
            lambda d: int(d.find_element(By.ID, "lblPageNumberTop").text) == current_page + 1
        )
        current_page += 1
        time.sleep(1)  # Allow additional time for the new page to settle
    else:
        break

driver.quit()

# --- Save data to Excel using pandas ---
df = pd.DataFrame(all_records)
df.to_excel(OUTPUT_EXCEL, index=False)
print(f"Summary data saved to Excel file: {OUTPUT_EXCEL}")
