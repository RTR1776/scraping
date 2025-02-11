import os
import re
import time
import logging
import requests
import pandas as pd

# ----------------------------
# Configuration & Setup
# ----------------------------

# Set up logging for better monitoring
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s')

# Get the Serper API key from an environment variable for security
SERPER_API_KEY = os.getenv("SERPER_API_KEY")
if not SERPER_API_KEY:
    logging.error("SERPER_API_KEY is not set in the environment variables.")
    exit(1)

# List of companies
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
    "PULMONARY AND SLEEP CONSULTANTS",
    "PULMONARY AND SLEEP CONSULTANTS OF",
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
    "UNITED IMAGING CONSULTANT LLC",
    "UNITED IMAGING CONSULTANTS LLC",
    "UTILITY CONSULTANTS INC",
    "VALLEY CITIES COUNSELING AND CONSULTATIO",
    "VANTAGE TECHNOLOGY CONSULTING GROUP LLC",
    "VISION SURGERY CONSULTANTS PA",
    "VITREO RETINAL CONSULTANTS & SURGEONS",
    "VITREO RETINAL CONSULTANTS & SURGEONS PA",
    "VM CONSULTANT LLC",
    "WATER FLUORIDATION CONSULTING LLC",
    "WAYFINDER CONSULTING LLC",
    "WEST GLEN GASTROINTESTINAL CONSULTANTS",
    "WESTGLEN GI CONSULTANTS",
    "WHATSAMATHER CONSULTING INC"
]

# ----------------------------
# Helper Functions
# ----------------------------

def fetch_company_data(company: str) -> dict:
    """
    Query the Serper API for a given company and return the JSON response.
    Returns an empty dictionary on error.
    """
    query = f"{company} consulting owner description"
    payload = {"q": query}
    headers = {"X-API-KEY": SERPER_API_KEY}

    try:
        response = requests.post("https://google.serper.dev/search", json=payload, headers=headers)
        response.raise_for_status()  # Raise error for bad responses (e.g., 4xx or 5xx)
        data = response.json()
        logging.info(f"Successfully retrieved data for {company}")
        return data
    except requests.RequestException as e:
        logging.error(f"Error fetching data for {company}: {e}")
        return {}

def extract_snippet(data: dict) -> str:
    """
    Extracts the snippet from the API response.
    """
    if "organic" in data and isinstance(data["organic"], list) and len(data["organic"]) > 0:
        return data["organic"][0].get("snippet", "Not Found")
    return "Not Found"

def extract_owner_from_snippet(snippet: str) -> str:
    """
    Extracts a potential owner name from the snippet using a regex.
    The regex looks for patterns following the word 'owner' or 'owners'.
    """
    # This regex attempts to capture one or more capitalized words (as a potential name) that follow
    # the word 'owner' or 'owners'. Adjust the pattern as needed.
    pattern = r"(?:owner(?:s)?[:\-]?\s*)([A-Z][a-zA-Z]*(?:\s+[A-Z][a-zA-Z]*)+)"
    match = re.search(pattern, snippet, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return "Not Found"

# ----------------------------
# Main Processing Loop
# ----------------------------

results = []

for company in companies:
    # Fetch data from the API
    data = fetch_company_data(company)
    
    # Extract snippet and owner information
    snippet = extract_snippet(data)
    owner = extract_owner_from_snippet(snippet)
    
    # Append result to the list
    results.append({
        "Company Name": company,
        "Owner(s)": owner,
        "Brief Description": snippet
    })
    
    # Respect the API rate limits
    time.sleep(1)

# ----------------------------
# Save Results to CSV
# ----------------------------

df = pd.DataFrame(results)
csv_filename = "consulting_companies.csv"
df.to_csv(csv_filename, index=False)
logging.info(f"CSV file saved as {csv_filename}")
