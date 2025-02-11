import os
import re
import time
import logging
import requests
import pandas as pd
import spacy
from spacy import matcher
from spacy.matcher import Matcher
from typing import List, Dict, Any
from requests.exceptions import RequestException
import random
import openai
import json

# ----------------------------
# Configuration & Setup
# ----------------------------

# Configure logging to include timestamps and severity level.
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s')

SERPER_API_KEY = os.getenv("SERPER_API_KEY")
if not SERPER_API_KEY:
    logging.error("SERPER_API_KEY is not set in the environment variables.")
    raise ValueError("Must set SERPER_API_KEY environment variable.")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    logging.error("OPENAI_API_KEY is not set in the environment variables.")
    raise ValueError("Must set OPENAI_API_KEY environment variable.")
openai.api_key = OPENAI_API_KEY

# Rate limiting delay (in seconds).
# Increase if you find you are hitting the API rate limit.
API_DELAY_SECONDS = 1

# Number of times to retry the API call on failure before giving up.
MAX_RETRIES = 3

# Exponential backoff factor. Wait time grows each retry attempt.
BACKOFF_FACTOR = 2

# List of companies to search for.
companies = [
    "ADVANCED RADIOLOGY CONSULTANTS OF KC PA",
    "AEM ENGINEERING CONSULTANTS LLC",
    "AIR POWER CONSULTANTS INC",
    "ALBERTSON CONSULTING INC.",
    "ALLIED ENVIRONMENTAL CONSULTANTS INC",
    "ANESTHESIA CONSULTING SERVICES",
    "AON CONSULTING INC",
    "ASBESTOS CONSULTING & TESTING INC",
    "ATLAS TECHNICAL CONSULTANTS LLC",
    "ATMOSPHERIC ANALYSIS & CONSULTING INC",
    "AVIATION CONSULTANT INC",
    "BAJ CONSULTING LLC",
    "BG CONSULTANTS INC",
    "BLADES AEROSPACE CONSULTING LLC",
    "BLUE RIVER CONSULTing LLC",
    "CARDIOVASCULAR CONSULTANTS OF KS",
    "CARE CONSULTANTS BETTER SOLUTIONS INC",
    "CAVANAUGH MACDONALD CONSULTING LLC",
    "CCF MENTAL HEALTH CONSULTATION",
    "CONEFLOWER CONSULTING LLC",
    "CORRECTIONS CONSULTING SERVICES LLC",
    "CR CONSULTANTS",
    "CREO CONSULTING LLC",
    "DAVE BURGESS CONSULTING INC",
    "DC MUNICIPAL CONSULTING",
    "DK AEROSPACE & AVIATION CONSULTING LLC",
    "DUE NORTH CONSULTING INC",
    "DULAC CONSULTING LLC",
    "DUTCH SONS DESIGN AND CONSULTING INC",
    "ELLIS AEROSPACE CONSULTING",
    "ENVISAGE CONSULTING INC",
    "ES DISABILITY EXAMINATION CONSULTANTS LL",
    "FINELINE HR CONSULTING LLC",
    "GLOBAL ENVIRONMENTAL CONSULTING INC",
    "GRAYS PEAK CONSULTING LLC",
    "HAGERTY CONSULTING INC",
    "HEARING CONSULTANTS",
    "HEART OF AMERICA CONSULTING LLC",
    "HG CONSULT INC",
    "HIGH STREET CONSULTING GROUP LLC",
    "HORNER DAVIS CONSULTING LLC",
    "HURON CONSULTING GROUP INC",
    "I SIGN CONSULTING LLC",
    "IE CONSULTANTS INC",
    "IKASO CONSULTING  LLC",
    "IN PATIENT CONSULTANTS OF KANSAS PA",
    "INCLUSIVE THERAPY & CONSULTING SERVICES",
    "INFECTIOUS DISEASE CONSULTANTS PA",
    "INPATIENT CONSULTANTS OF KANSAS PA",
    "J RUNYAN CONSULTING LLC",
    "JB CONSULTANTS LLC",
    "JEO CONSULTING GROUP INC",
    "JLC CONSULTING LLC",
    "JOPLIN NEPHROLOGY CONSULTANTS",
    "JULIA ORLANDO CONSULTING LLC",
    "KANSAS CITY KIDNEY CONSULTANTS  PA",
    "KANSAS IMAGING CONSULTANTS PA",
    "KANSAS PATHOLOGY CONSULTANTS PA",
    "KANSAS SURGICAL CONSULTANTS LLP",
    "KC INFECTIOUS DISEASE CONSULTANTS LLC",
    "KENNEDY JENKS CONSULTANTS INC",
    "KMS CONSULTING GROUP LLC",
    "LAWRENCE GI CONSULTANTS",
    "LIMELIGHT CONSULTING LLC",
    "LMH PSYCHIATRIC CONSULTATION SERVICES",
    "LOCUM TENENS CONSULTING INC",
    "MANGATA CONSULTING LLC",
    "MEGAN STRAUSS CONSULTING LLC",
    "METRO INFECTIOUS DISEASE CONSULTANTS LLC",
    "MIDWEST BEEF CATTLE CONSULTANTS LLC",
    "MIDWEST MEDICAL CONSULTANT LLC",
    "MIDWEST MEDICAL CONSULTANTS INC",
    "MIDWEST TRAINING AND CONSULTING SERVICES",
    "MINGENBACK BUSINESS CONSULTing",
    "MKEC ENGINEERING CONSULTANTS INC",
    "MS AEROSPACE CONSULTING INC",
    "NEUROLOGY CONSULTANTS OF KANSAS LLC",
    "NOBLE CONSULTING SERVICES INC",
    "NORTHEAST KS GI CONSULTANTS PA",
    "PAVE COMMUNICATIONS AND CONSULTING",
    "PBM CONSULTING INC",
    "PCG CONSULTING",
    "PROFESSIONAL ENGINEERING CONSULTANTS PA",
    "PROFESSIONAL PELVIC HEALTH CONSULTANTS",
    "PUBLIC CONSULTING GROUP LLC",
    "REHAB CONSULTANTS PA",
    "RENAISSANCE INFRASTRUCTURE CONSULTING IN",
    "RISK & REGULATORY CONSULTING LLC",
    "RLC CONSULTING INC",
    "RUSTY JONES AVIATION CONSULTANT LLC",
    "SCHILLING CONSULTING LLC",
    "SCHMUCKER TRAINING & CONSULTING",
    "SCL CONSULTING LLC",
    "SCOTT CONSULTING  LLC",
    "SEATEC CONSULTING INC",
    "SENSE TECH CONSULTING LLC",
    "SIDE 6 CONSULTING LLC",
    "SMH CONSULTANTS",
    "SPECIAL MARKETS INSURANCE CONSULTANTS",
    "SRF CONSULTING GROUP",
    "STAMEY STREET CONSULTING GROUP LLC",
    "STANDARD CONSULTING LLC",
    "STANTEC CONSULTING SERVICES INC",
    "STEARNS CONRAD & SCHMIDT CONSULTING",
    "STILES GLAUCOMA CONSULTANTS",
    "SURGICAL CONSULTANTS OF KANSAS CITY PC",
    "TATA CONSULTANCY SERVICES LIMITED",
    "TERRACON CONSULTANTS INC",
    "THE BOSTON CONSULTING GROUP INC",
    "THE CENTER FOR COUNSELING & CONSULTATION",
    "THE CO-OP CONSULTANT LLC",
    "TIANJIN XUESHI EDUCATION CONSULTING CO",
    "TK AEROSPACE CONSULTING",
    "TLC MARKETING CONSULTANTS LLC",
    "TRANSPORT CONSULTANTS INTERNATIONAL INC",
    "UNITED IMAGING CONSULTANTS LLC",
    "UTILITY CONSULTANTS INC",
    "VALLEY CITIES COUNSELING AND CONSULTATIO",
    "VANTAGE TECHNOLOGY CONSULTING GROUP LLC",
    "VISION SURGERY CONSULTANTS PA",
    "VITREO RETINAL CONSULTANTS & SURGEONS",
    "VM CONSULTANT LLC",
    "WATER FLUORIDATION CONSULTING LLC",
    "WAYFINDER CONSULTING LLC",
    "WEST GLEN GASTROINTESTINAL CONSULTANTS",
    "WESTGLEN GI CONSULTANTS",
    "WHATSAMATHER CONSULTING INC"
]

# ----------------------------
# SpaCy Initialization
# ----------------------------
# Ensure you have installed SpaCy and the model with:
#   pip install spacy
#   python -m spacy download en_core_web_sm
try:
    nlp = spacy.load("en_core_web_sm")
except OSError:
    logging.error(
        "SpaCy model 'en_core_web_sm' not found. "
        "Install it using: python -m spacy download en_core_web_sm"
    )
    raise

matcher = Matcher(nlp.vocab)

# Define common executive titles to look for.
# Add synonyms or new roles here as needed.
titles_list = ["owner", "ceo", "cfo", "coo", "founder", "president"]

# Create patterns to match typical usage of Title + Name or Name + Title.
patterns = []
for title in titles_list:
    # Pattern: <title> [optional punctuation] <PERSON>
    patterns.append(
        [{"LOWER": title}, {"IS_PUNCT": True, "OP": "?"}, {"ENT_TYPE": "PERSON"}]
    )
    # Pattern: <PERSON> [optional punctuation] <title>
    patterns.append(
        [{"ENT_TYPE": "PERSON"}, {"IS_PUNCT": True, "OP": "?"}, {"LOWER": title}]
    )
matcher.add("EXECUTIVE", patterns)

def extract_executives_spacy(snippet: str) -> str:
    """
    Uses SpaCy's matcher to extract executive names with associated titles.
    If no matches are found via the matcher, a fallback regex-based extraction is applied.
    
    Returns a string with "Title: Name" pairs.
    """
    doc = nlp(snippet)
    matches = matcher(doc)
    extracted = set()
    for match_id, start, end in matches:
        span = doc[start:end]
        tokens = span.text.split()
        title_found = [token for token in tokens if token.lower() in titles_list]
        if title_found:
            title = title_found[0].capitalize()
            # If a PERSON entity in the span is found, use that as the name.
            found_person = False
            for ent in doc.ents:
                if ent.label_ == "PERSON" and ent.start >= start and ent.end <= end:
                    extracted.add(f"{title}: {ent.text}")
                    found_person = True
            # If no PERSON entity was found in this span, try to extract a name pattern from the raw text.
            if not found_person:
                regex_pattern = rf"{title_found[0]}\s*(?:[:,-]\s*)?([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)"
                regex_match = re.search(regex_pattern, span.text)
                if regex_match:
                    name = regex_match.group(1)
                    extracted.add(f"{title}: {name}")
    # Fallback regex over the whole snippet if no SpaCy matches were made.
    if not extracted:
        for title in titles_list:
            fallback_pattern = rf"{title}\s*(?:[:,-]\s*)?([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)"
            for regex_match in re.finditer(fallback_pattern, snippet, re.IGNORECASE):
                extracted.add(f"{title.capitalize()}: {regex_match.group(1)}")
    return "; ".join(sorted(extracted)) if extracted else "Not Found"

# ----------------------------
# LLM Extraction Function
# ----------------------------
def extract_info_with_llm(snippet: str) -> Dict[str, Any]:
    """
    Uses OpenAI's LLM to extract structured information.
    Expects the snippet (combined text) and returns a dict with keys like:
      - owner
      - company_description
      - other_executives (if any)
    """
    prompt = f"""
    You are an expert business analyst.
    Given the following text harvested from online sources regarding a company,
    extract the following fields in JSON format:
    
      "owner": The name(s) of the owner(s) or the principal executive.
      "company_description": A concise description of what the company does.
      "other_executives": A list of any additional executive role and name pairs (e.g., "CEO: John Doe").
    
    If a field cannot be determined, return null.
    
    Text:
    {snippet}
    
    Provide only the JSON output.
    """
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",  # Updated the model to GPT-4
            messages=[
                {"role": "system", "content": "You extract structured business data from text."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.0,
            max_tokens=250,
        )
        answer_text = response.choices[0].message.content.strip()
        # Expect a JSON string in answer_text
        structured_data = json.loads(answer_text)
        return structured_data
    except Exception as e:
        logging.error(f"LLM extraction failed: {e}")
        return {}

# ----------------------------
# Helper Functions
# ----------------------------

def call_serper_api(query: str) -> Dict[str, Any]:
    """
    Calls the SERPer API with a specified query.
    Returns a JSON response if successful, or raises an exception on failure.

    We'll implement a simple retry with exponential backoff to handle
    intermittent network or server errors.
    """
    url = "https://google.serper.dev/search"
    headers = {"X-API-KEY": SERPER_API_KEY}
    payload = {"q": query}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=10)
            resp.raise_for_status()  # Raise an HTTPError for bad responses
            return resp.json()
        except RequestException as e:
            logging.warning(
                f"Request failed (attempt {attempt}/{MAX_RETRIES}): {e}"
            )
            if attempt == MAX_RETRIES:
                raise  # Re-raise exception if we've exhausted retries
            sleep_time = BACKOFF_FACTOR ** (attempt - 1)
            logging.info(f"Retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)

    # In theory, we'll never reach here because of the raise in the loop.
    return {}

def extract_snippets(data: Dict[str, Any], max_snippets: int = 3) -> List[str]:
    """
    Extracts up to `max_snippets` snippet strings from the SERPer API response.

    If the API response is not in the expected format or empty, returns ["Not Found"].
    """
    if "organic" not in data or not isinstance(data["organic"], list) or len(data["organic"]) == 0:
        return ["Not Found"]

    # Take the top N organic results. Adjust as needed.
    results = data["organic"][:max_snippets]
    snippets = []
    for result in results:
        snippet_text = result.get("snippet", "").strip()
        if snippet_text:
            snippets.append(snippet_text)
        else:
            snippets.append("Not Found")

    return snippets

def process_company(company: str) -> Dict[str, Any]:
    """
    Orchestrates the process of:
      1) Building the query and calling the SERPer API
      2) Extracting multiple snippets
      3) Using SpaCy to find "executive" info in those snippets

    Returns a dictionary with the relevant info.
    """
    # Use a query that might produce relevant ownership/executive info.
    query = f"{company} consulting owner description"

    try:
        # Hit the SERPer API
        data = call_serper_api(query)
        # Extract up to 3 snippets for better coverage
        snippet_list = extract_snippets(data, max_snippets=3)
        # Combine all snippets into one large text for matching
        combined_snippets = " | ".join(snippet_list)
        # First, try rule-based extraction.
        executive_info = extract_executives_spacy(combined_snippets)
        # Then, refine and enrich with LLM extraction.
        llm_info = extract_info_with_llm(combined_snippets)

        return {
            "Company Name": company,
            "Executive(s)_rule_based": executive_info,
            "LLM_extraction": llm_info,
            "Snippets": snippet_list
        }

    except Exception as e:
        logging.error(f"Final failure fetching data for {company}: {e}")
        return {
            "Company Name": company,
            "Executive(s)_rule_based": "Error",
            "LLM_extraction": {},
            "Snippets": ["Error fetching data"]
        }

# ----------------------------
# Main Processing Logic
# ----------------------------

def main():
    results = []

    for company in companies:
        logging.info(f"Processing: {company}")
        result = process_company(company)
        results.append(result)
        # Respect rate limiting
        time.sleep(API_DELAY_SECONDS)

    # Save results to CSV
    csv_filename = r"G:\My Drive\Kansas Finance Deep Dive\consulting_companies.csv"
    df = pd.DataFrame(results)
    df.to_csv(csv_filename, index=False)
    logging.info(f"CSV file saved as {csv_filename}")

if __name__ == "__main__":
    main()
