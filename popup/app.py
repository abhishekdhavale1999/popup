from flask import Flask, request, jsonify, render_template, redirect, url_for
import pandas as pd
import re
from fuzzywuzzy import fuzz
import requests
import sqlite3
import os
import threading
import time
import json
import logging
import smtplib
import dns.resolver
import dns.exception
import csv
from datetime import datetime, date
from collections import Counter
import ssl # For secure SMTP connection
import traceback # Import traceback for detailed error logging in notifications
from email.mime.text import MIMEText # For proper email encoding

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration for APIs and Email Notifications ---
API_KEY = os.environ.get("GOOGLE_API_KEY", "AIzaSyAcZE3Qc_59mo2fkB0flcH0NXlb0Bspr34")
# Abstract API Key for email validation
ABSTRACT_API_KEY = os.environ.get("ABSTRACT_API_KEY", "d8ccd64e8aba4afea81f2dbaddc90f15")

# SMTP Configuration for Error Notifications (IMPORTANT: Use environment variables in production)
SMTP_SERVER = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
SMTP_USERNAME = os.environ.get("SMTP_USERNAME", "affilatemarketingintentamplify@gmail.com") # Your sender email
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "qcki fnhn lmxu cddj") # Your email password/app password
ADMIN_EMAIL = os.environ.get("ADMIN_EMAIL", "developer@intentamplify.com") # Recipient for error notifications

# --- Global DataFrames and Constants ---

FUZZY_MATCH_THRESHOLD = 80

TEMP_EMAIL_DOMAINS = [
    "mailinator.com", "tempmail.io", "10minutemail.com", "guerrillamail.com",
    "yopmail.com", "temp-mail.org", "disposablemail.com", "trashmail.com",
    "sharklasers.com", "anonbox.net", "dropmail.me", "fakemail.net",
    "getnada.com", "maildrop.cc", "moakt.com", "nada.online",
    "throwawaymail.com", "unspam.email", "wegwerfmail.de", "mail.tm"
]

DELIVERABILITY_DOMAIN_CACHE_TTL = 7 * 24 * 60 * 60 # 7 days

# --- Helper Function: Normalize Company Name ---
def normalize_company_name(name):
    """
    Normalizes a company name by removing common legal suffixes, extra spaces,
    and converting to lowercase for better matching.
    """
    if not name:
        return ""

    name = str(name).lower()
    # List of common legal suffixes and terms to remove (case-insensitive)
    suffixes = [
        r'\binc\b', r'\bltd\b', r'\bllc\b', r'\bcorp\b', r'\bcorporation\b',
        r'\bco\b', r'\bcompany\b', r'\bgroup\b', r'\bholding\b',
        r'\b(s\.?a\.?)\b', r'\b(g\.?m\.?b\.?h)\b', r'\b(a\.?g)\b',
        r'\bsdn bhd\b', r'\bpt\b', r'\bapc\b', r'\blp\b', r'\bltd\.?\b',
        r'\bpllc\b', r'\bna\b' # for 'N.A.' style suffix
    ]
    for suffix in suffixes:
        name = re.sub(suffix, '', name)

    # Remove punctuation and extra spaces
    name = re.sub(r'[^\w\s]', '', name) # Remove non-alphanumeric and non-space characters
    name = re.sub(r'\s+', ' ', name).strip() # Replace multiple spaces with single, strip leading/trailing

    return name

# Load company data from Excel (for fuzzy matching) - Now uses normalize_company_name
try:
    df = pd.read_excel("company_data.xlsx")
    df['company_name'] = df['company_name'].astype(str)
    # Ensure 'auto_alias' creation uses the now-defined normalize_company_name
    df['auto_alias'] = df['company_name'].apply(
        lambda name: normalize_company_name(name).split()[0] # Get the first word of the normalized name
        if str(name) and normalize_company_name(name) else None
    )
    app.logger.info("company_data.xlsx loaded successfully.")
except FileNotFoundError:
    app.logger.warning("company_data.xlsx not found. Starting with an empty DataFrame. All lookups will rely on APIs.")
    df = pd.DataFrame(columns=['company_name', 'industry', 'employee_size', 'country', 'auto_alias'])
except Exception as e:
    app.logger.error(f"Error loading company_data.xlsx: {e}. Starting with an empty DataFrame.", exc_info=True)
    df = pd.DataFrame(columns=['company_name', 'industry', 'employee_size', 'country', 'auto_alias'])


# --- Database Initialization ---
def init_cache_db():
    """Initializes the SQLite database for caching company information and email deliverability,
       and a master table for de-duplicated company data."""
    conn = sqlite3.connect("company_cache.db")
    cur = conn.cursor()

    # Table for company info cache (for external API results)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS company_cache (
            domain TEXT PRIMARY KEY,
            company_name TEXT,
            industry TEXT,
            employee_size TEXT,
            country TEXT,
            logo TEXT,
            linkedin TEXT,
            source TEXT,
            headquarter TEXT,
            hq_number TEXT,
            ai_confidence_score TEXT,
            last_updated INTEGER
        )
    """)

    # Table for email domain deliverability cache
    cur.execute("""
        CREATE TABLE IF NOT EXISTS email_domain_deliverability_cache (
            email_domain TEXT PRIMARY KEY,
            is_domain_reachable INTEGER,
            message TEXT,
            last_checked INTEGER
        )
    """)

    # New table for de-duplicated, master company data
    cur.execute("""
        CREATE TABLE IF NOT EXISTS master_companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_name TEXT UNIQUE,
            domain TEXT UNIQUE, -- Often derived from email, useful for lookup
            industry TEXT,
            employee_size TEXT,
            country TEXT,
            headquarter TEXT,
            hq_number TEXT,
            ai_confidence_score TEXT,
            logo TEXT,
            linkedin_url TEXT,
            first_submission_date TEXT,
            last_updated_date TEXT,
            source_info TEXT -- Combined source info
        )
    """)

    # New table for event logging (pixel tracking data)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS event_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            event_name TEXT NOT NULL,
            user_ip TEXT,
            user_agent TEXT,
            screen_width INTEGER,
            screen_height INTEGER,
            user_ip_country TEXT, -- New field for IP-based country
            user_ip_region TEXT,  -- New field for IP-based region
            user_ip_city TEXT,    -- New field for IP-based city
            event_details TEXT    -- JSON string for additional event-specific data (e.g., time_spent, business_interest)
        )
    """)

    conn.commit()
    conn.close()
    app.logger.info("Company cache, Email Domain Deliverability, and Master Companies databases initialized.")

init_cache_db()

# --- Utility Functions for Cache & Master DB ---
def save_to_cache(domain, data):
    """Saves company data to the SQLite company cache (for external API results)."""
    conn = sqlite3.connect("company_cache.db")
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT OR REPLACE INTO company_cache
            (domain, company_name, industry, employee_size, country, logo, linkedin, source, headquarter, hq_number, ai_confidence_score, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, strftime('%s','now'))
        """, (
            data.get("domain", domain),
            data.get("company_name", ""),
            data.get("industry", ""),
            data.get("employee_size", ""),
            data.get("country", ""),
            data.get("logo", ""),
            data.get("linkedin", ""),
            data.get("source", ""),
            data.get("headquarter", ""),
            data.get("hq_number", ""),
            data.get("ai_confidence_score", "")
        ))
        conn.commit()
        app.logger.info(f"Data for domain '{domain}' saved to company cache.")
    except Exception as e:
        app.logger.error(f"Error saving data to company cache for domain '{domain}': {e}", exc_info=True)
        send_error_notification("Cache Save Error", f"Failed to save data to company cache for domain '{domain}': {e}")
    finally:
        conn.close()

def get_from_cache(domain):
    """Retrieves company data from the SQLite company cache."""
    conn = sqlite3.connect("company_cache.db")
    cur = conn.cursor()
    row = None
    try:
        cur.execute("SELECT company_name, industry, employee_size, country, logo, linkedin, source, headquarter, hq_number, ai_confidence_score FROM company_cache WHERE domain=?", (domain,))
        row = cur.fetchone()
    except Exception as e:
        app.logger.error(f"Error retrieving from company cache for domain '{domain}': {e}", exc_info=True)
        send_error_notification("Cache Retrieval Error", f"Failed to retrieve data from company cache for domain '{domain}': {e}")
    finally:
        conn.close()

    if row:
        app.logger.info(f"Data for domain '{domain}' found in company cache.")
        return {
            "company_name": row[0],
            "industry": row[1],
            "employee_size": row[2],
            "country": row[3],
            "logo": row[4],
            "linkedin": row[5],
            "source": row[6] + " (cache)",
            "headquarter": row[7],
            "hq_number": row[8],
            "ai_confidence_score": row[9]
        }
    app.logger.info(f"Data for domain '{domain}' not found in company cache.")
    return None

def get_domain_deliverability_from_cache(domain):
    """Retrieves email domain deliverability status from the SQLite cache."""
    conn = sqlite3.connect("company_cache.db")
    cur = conn.cursor()
    row = None
    try:
        cur.execute("SELECT is_domain_reachable, message, last_checked FROM email_domain_deliverability_cache WHERE email_domain=?", (domain,))
        row = cur.fetchone()
    except Exception as e:
        app.logger.error(f"Error retrieving domain deliverability from cache for domain '{domain}': {e}", exc_info=True)
        send_error_notification("Domain Deliverability Cache Error", f"Failed to retrieve domain deliverability for domain '{domain}': {e}")
    finally:
        conn.close()

    if row:
        is_domain_reachable, message, last_checked = row
        current_time = int(time.time())
        if (current_time - last_checked) < DELIVERABILITY_DOMAIN_CACHE_TTL:
            app.logger.info(f"Domain deliverability for '{domain}' found in cache and is fresh.")
            return bool(is_domain_reachable), message
        else:
            app.logger.info(f"Domain deliverability for '{domain}' found in cache but is stale. Will re-verify.")
    app.logger.info(f"Domain deliverability for '{domain}' not in cache or is stale.")
    return None, None

def save_domain_deliverability_to_cache(domain, is_domain_reachable, message):
    """Saves email domain deliverability status to the SQLite cache."""
    conn = sqlite3.connect("company_cache.db")
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT OR REPLACE INTO email_domain_deliverability_cache
            (email_domain, is_domain_reachable, message, last_checked)
            VALUES (?, ?, ?, strftime('%s','now'))
        """, (domain, int(is_domain_reachable), message))
        conn.commit()
        app.logger.info(f"Domain deliverability for '{domain}' saved to cache.")
    except Exception as e:
        app.logger.error(f"Error saving domain deliverability to cache for domain '{domain}': {e}", exc_info=True)
        send_error_notification("Save Domain Deliverability Error", f"Failed to save domain deliverability for domain '{domain}': {e}")
    finally:
        conn.close()

def clear_domain_deliverability_cache(domain):
    """Removes a specific domain from the email_domain_deliverability_cache."""
    conn = None
    try:
        conn = sqlite3.connect("company_cache.db", timeout=10, check_same_thread=False)
        cur = conn.cursor()
        cur.execute("DELETE FROM email_domain_deliverability_cache WHERE email_domain=?", (domain,))
        conn.commit()
        cur.close()
        app.logger.info(f"Cleared deliverability cache for domain: {domain}")
    except sqlite3.OperationalError as e:
        app.logger.error(f"Database is locked while clearing cache for domain '{domain}': {e}", exc_info=True)
        send_error_notification("DB Lock Error", f"Database locked during clear_domain_deliverability_cache for '{domain}': {e}")
    except Exception as e:
        app.logger.error(f"Unexpected error while clearing cache for domain '{domain}': {e}", exc_info=True)
        send_error_notification("Clear Cache Error", f"Unexpected error clearing deliverability cache for '{domain}': {e}")
    finally:
        if conn:
            conn.close()

def get_from_master_companies(company_name=None, domain=None):
    """Retrieves company data from the master_companies table."""
    conn = sqlite3.connect("company_cache.db")
    cur = conn.cursor()
    row = None
    try:
        if company_name and company_name != "N/A":
            cur.execute("SELECT * FROM master_companies WHERE company_name=? COLLATE NOCASE", (company_name,))
            row = cur.fetchone()

        if not row and domain and domain != "N/A": # Only try domain if company_name didn't yield a result
            cur.execute("SELECT * FROM master_companies WHERE domain=? COLLATE NOCASE", (domain,))
            row = cur.fetchone()

        if not row:
            return None # No lookup criteria or no result

    except Exception as e:
        app.logger.error(f"Error retrieving from master_companies for company_name='{company_name}', domain='{domain}': {e}", exc_info=True)
        send_error_notification("Master DB Retrieval Error", f"Failed to retrieve from master_companies for company_name='{company_name}', domain='{domain}': {e}")
    finally:
        conn.close()

    if row:
        columns = [description[0] for description in cur.description] # Get column names
        app.logger.info(f"Data for company '{company_name or domain}' found in master_companies.")
        return dict(zip(columns, row))
    app.logger.info(f"Data for company '{company_name or domain}' not found in master_companies.")
    return None


def update_master_companies(data):
    """Updates an existing record in the master_companies table."""
    conn = sqlite3.connect("company_cache.db")
    cur = conn.cursor()
    try:
        # Find existing record by company_name or domain
        existing_id = None
        if data.get("company_name") and data.get("company_name") != "N/A":
            cur.execute("SELECT id FROM master_companies WHERE company_name=? COLLATE NOCASE", (data["company_name"],))
            result = cur.fetchone()
            if result:
                existing_id = result[0]

        if not existing_id and data.get("domain") and data.get("domain") != "N/A":
            cur.execute("SELECT id FROM master_companies WHERE domain=? COLLATE NOCASE", (data["domain"],))
            result = cur.fetchone()
            if result:
                existing_id = result[0]

        if existing_id:
            # Fetch existing data to merge
            existing_data = get_from_master_companies(company_name=data.get("company_name"), domain=data.get("domain"))

            # Update only if the new data is more complete or different
            update_fields = {
                "industry": data.get("industry") if data.get("industry") and data.get("industry") != "N/A" else existing_data.get("industry", "N/A"),
                "employee_size": data.get("employee_size") if data.get("employee_size") and data.get("employee_size") != "N/A" else existing_data.get("employee_size", "N/A"),
                "country": data.get("country") if data.get("country") and data.get("country") != "N/A" else existing_data.get("country", "N/A"),
                "headquarter": data.get("headquarter") if data.get("headquarter") and data.get("headquarter") != "N/A" else existing_data.get("headquarter", "N/A"),
                "hq_number": data.get("hq_number") if data.get("hq_number") and data.get("hq_number") != "N/A" else existing_data.get("hq_number", "N/A"),
                "ai_confidence_score": data.get("ai_confidence_score") if data.get("ai_confidence_score") and data.get("ai_confidence_score") != "N/A" else existing_data.get("ai_confidence_score", "N/A"),
                "logo": data.get("logo") or existing_data.get("logo", ""),
                "linkedin_url": data.get("linkedin") or existing_data.get("linkedin_url", ""),
                "source_info": ", ".join(sorted(list(set(f"{existing_data.get('source_info', '')}, {data.get('source', '')}".strip(', ').split(', ')))))
            }
            # Ensure company_name and domain are updated if they were missing or improved
            if not existing_data.get("company_name") or existing_data.get("company_name") == "N/A" and data.get("company_name") and data.get("company_name") != "N/A":
                update_fields["company_name"] = data["company_name"]
            if not existing_data.get("domain") or existing_data.get("domain") == "N/A" and data.get("domain") and data.get("domain") != "N/A":
                update_fields["domain"] = data["domain"]

            # Update last_updated_date
            update_fields["last_updated_date"] = datetime.now().isoformat()

            set_clause = ", ".join([f"{key}=?" for key in update_fields.keys()])
            values = list(update_fields.values())
            values.append(existing_id)

            cur.execute(f"UPDATE master_companies SET {set_clause} WHERE id=?", values)
            conn.commit()
            app.logger.info(f"Updated master_companies for company '{data.get('company_name', data.get('domain'))}' (ID: {existing_id}).")
            return True
        else:
            app.logger.warning(f"Attempted to update non-existent record in master_companies for {data.get('company_name')}/{data.get('domain')}.")
            return False
    except Exception as e:
        app.logger.error(f"Error updating master_companies for company '{data.get('company_name', data.get('domain'))}': {e}", exc_info=True)
        send_error_notification("Master DB Update Error", f"Failed to update master_companies for company '{data.get('company_name', data.get('domain'))}': {e}\nTraceback: {traceback.format_exc()}")
        return False
    finally:
        conn.close()

def insert_master_companies(data):
    """Inserts a new record into the master_companies table."""
    conn = sqlite3.connect("company_cache.db")
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO master_companies
            (company_name, domain, industry, employee_size, country, headquarter, hq_number, ai_confidence_score, logo, linkedin_url, first_submission_date, last_updated_date, source_info)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data.get("company_name", "N/A"),
            data.get("domain", "N/A"),
            data.get("industry", "N/A"),
            data.get("employee_size", "N/A"),
            data.get("country", "N/A"),
            data.get("headquarter", "N/A"),
            data.get("hq_number", "N/A"),
            data.get("ai_confidence_score", "N/A"),
            data.get("logo", ""),
            data.get("linkedin", ""), # Use 'linkedin' from input data
            datetime.now().isoformat(),
            datetime.now().isoformat(),
            data.get("source", "N/A")
        ))
        conn.commit()
        app.logger.info(f"Inserted new record into master_companies for company '{data.get('company_name', data.get('domain'))}'.")
        return True
    except sqlite3.IntegrityError as e:
        app.logger.warning(f"Integrity error inserting into master_companies (likely duplicate unique field): {e}. Attempting update instead.")
        return update_master_companies(data) # Try updating if it's a unique constraint violation
    except Exception as e:
        app.logger.error(f"Error inserting into master_companies for company '{data.get('company_name', data.get('domain'))}': {e}", exc_info=True)
        send_error_notification("Master DB Insert Error", f"Failed to insert into master_companies for company '{data.get('company_name', data.get('domain'))}': {e}\nTraceback: {traceback.format_exc()}")
        return False
    finally:
        conn.close()


# --- Email Notification Functions ---
def send_email(to_email, subject, body, is_html=False):
    """
    Sends an email using the configured SMTP server.
    Handles UTF-8 encoding for message body and supports HTML.
    """
    if not all([SMTP_SERVER, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD, to_email]):
        app.logger.error(f"SMTP configuration or recipient missing for email to {to_email}. Cannot send email.")
        return False

    try:
        # Create a MIMEText object for proper email formatting and encoding
        # Use 'html' subtype if is_html is True, otherwise 'plain'
        msg = MIMEText(body, 'html' if is_html else 'plain', 'utf-8')
        msg['Subject'] = subject
        msg['From'] = SMTP_USERNAME
        msg['To'] = to_email

        context = ssl.create_default_context()
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.ehlo()
            server.starttls(context=context)
            server.ehlo()
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.sendmail(SMTP_USERNAME, to_email, msg.as_string())
        app.logger.info(f"Email sent successfully to {to_email} with subject: '{subject}'")
        return True
    except Exception as e:
        app.logger.error(f"Failed to send email to {to_email} with subject '{subject}': {e}", exc_info=True)
        return False

def send_error_notification(subject, message):
    """Sends an email notification to the admin email for backend system errors."""
    success = send_email(ADMIN_EMAIL, subject, message, is_html=False)
    if not success:
        app.logger.error("Attempted to send error notification but failed. Check SMTP configuration and network.")

def send_alert_notification(subject, message, is_html=False):
    """Sends an email notification to the admin email for user-related alerts."""
    success = send_email(ADMIN_EMAIL, subject, message, is_html)
    if not success:
        app.logger.error("Attempted to send alert notification but failed. Check SMTP configuration and network.")


# --- Core Logic Functions (remain similar, but with error reporting) ---
def check_email_deliverability_abstract(email):
    """
    Checks email deliverability using Abstract Email Verification API.
    Returns a tuple: (is_deliverable: bool, reason: str)
    """
    try:
        api_key = ABSTRACT_API_KEY  # Abstract API Key
        if not api_key:
            app.logger.warning("Abstract API key not set for email deliverability check.")
            # We don't send an email notification here because it's a configuration issue,
            # not a runtime error during an API call *attempt*.
            return False, "Abstract API key not set"

        url = "https://emailvalidation.abstractapi.com/v1/"
        params = {
            "api_key": api_key,
            "email": email
        }

        response = requests.get(url, params=params, timeout=5)
        response.raise_for_status() # This will raise an HTTPError for 4xx/5xx responses
        result = response.json()

        if result.get("deliverability") == "DELIVERABLE":
            return True, "Email is deliverable"
        else:
            reason = result.get("deliverability") or "Undeliverable"
            return False, f"Email is {reason}"

    except requests.RequestException as e:
        app.logger.error(f"Abstract Email API network error for email '{email}': {e}", exc_info=True)
        # Send notification for actual network/API errors
        send_error_notification("Email API Error (Abstract)", f"Abstract Email API network error for '{email}': {e}\nTraceback: {traceback.format_exc()}")
        return False, f"Network error during email validation: {e}"
    except json.JSONDecodeError:
        app.logger.error(f"Abstract Email API JSON decode error for email '{email}'. Response: {response.text}", exc_info=True)
        # Send notification for API response parsing errors
        send_error_notification("Email API Error (Abstract)", f"Abstract Email API JSON decode error for email '{email}'. Response: {response.text}\nTraceback: {traceback.format_exc()}")
        return False, f"Unexpected API response during email validation."
    except Exception as e:
        app.logger.error(f"Abstract Email API unexpected error for email '{email}': {str(e)}", exc_info=True)
        # Send notification for any other unexpected errors
        send_error_notification("Email API Error (Abstract)", f"Abstract Email API unexpected error for '{email}': {str(e)}\nTraceback: {traceback.format_exc()}")
        return False, f"Unexpected error during email validation: {str(e)}"

def fetch_from_clearbit_autocomplete(query):
    """Fetches company information from Clearbit Autocomplete API."""
    app.logger.info(f"Attempting to fetch from Clearbit Autocomplete for query: {query}")
    try:
        response = requests.get(
            "https://autocomplete.clearbit.com/v1/companies/suggest",
            params={"query": query},
            timeout=5
        )
        response.raise_for_status()
        results = response.json()
        if results:
            company = results[0]
            app.logger.info(f"Clearbit Autocomplete found company: {company.get('name')}")
            return {
                "company_name": company.get("name", ""),
                "industry": "",
                "employee_size": "",
                "country": "",
                "logo": f"https://logo.clearbit.com/{company.get('domain', '')}" if company.get('domain') else "",
                "linkedin": company.get("linkedin_url", "") or company.get("linkedin", ""),
                "headquarter": "",
                "hq_number": "",
                "source": "Clearbit Autocomplete API"
            }
        app.logger.info(f"Clearbit Autocomplete found no results for query: {query}")
    except requests.exceptions.RequestException as e:
        app.logger.error(f"Clearbit Autocomplete network error for query '{query}': {e}", exc_info=True)
        send_error_notification("Clearbit API Error", f"Clearbit Autocomplete network error for query '{query}': {e}\nTraceback: {traceback.format_exc()}")
    except json.JSONDecodeError:
        app.logger.error(f"Clearbit Autocomplete JSON decode error for query '{query}'. Response: {response.text}", exc_info=True)
        send_error_notification("Clearbit API Error", f"Clearbit Autocomplete JSON decode error for query '{query}'. Response: {response.text}\nTraceback: {traceback.format_exc()}")
    except Exception as e:
        app.logger.error(f"Clearbit Autocomplete unexpected error for query '{query}': {e}", exc_info=True)
        send_error_notification("Clearbit API Error", f"Clearbit Autocomplete unexpected error for query '{query}': {e}\nTraceback: {traceback.format_exc()}")
    return None

def fetch_from_wikidata_advanced(company_name):
    """Fetches company information from Wikidata using SPARQL queries."""
    if not company_name:
        app.logger.warning("No company name provided to fetch from Wikidata.")
        return None

    app.logger.info(f"Attempting to fetch from Wikidata (advanced) for company: {company_name}")
    try:
        search_url = "https://www.wikidata.org/w/api.php"
        search_params = {
            "action": "wbsearchentities",
            "format": "json",
            "language": "en",
            "search": company_name,
            "type": "item",
            "limit": 5
        }
        search_response = requests.get(search_url, params=search_params, timeout=5)
        search_response.raise_for_status()
        search_results = search_response.json().get("search", [])

        company_type_qids = {"Q4830453", "Q783794", "Q891723", "Q43128", "Q6881511"}

        qid = None
        for result in search_results:
            potential_qid = result.get("id")
            if potential_qid:
                details_url = "https://www.wikidata.org/w/api.php"
                details_params = {
                    "action": "wbgetentities",
                    "ids": potential_qid,
                    "props": "claims",
                    "format": "json",
                    "language": "en"
                }
                details_response = requests.get(details_url, params=details_params, timeout=5)
                details_response.raise_for_status()
                details_data = details_response.json()

                entity_claims = details_data.get("entities", {}).get(potential_qid, {}).get("claims", {})
                p31_claims = entity_claims.get("P31", [])

                for claim in p31_claims:
                    if 'mainsnak' in claim and 'datavalue' in claim['mainsnak']:
                        claim_value_type = claim['mainsnak']['datavalue'].get('type')
                        if claim_value_type == 'wikibase-entityid':
                            claim_qid = claim['mainsnak']['datavalue']['value'].get('id')
                            if claim_qid in company_type_qids:
                                qid = potential_qid
                                app.logger.info(f"Found strong company QID '{qid}' for '{company_name}' via P31 claim: {claim_qid}.")
                                break
                    if qid: break
            if qid: break

        if not qid and search_results:
            qid = search_results[0].get("id")
            app.logger.info(f"Falling back to first QID '{qid}' for '{company_name}' (no strong P31 match found).")

        if not qid:
            app.logger.info(f"No suitable QID found on Wikidata for company: {company_name}")
            return None

        sparql_query = f"""
        SELECT ?industryLabel ?countryLabel ?employees ?linkedinId ?logoUrl ?headquarterLabel ?hqNumber
        WHERE {{
          OPTIONAL {{ wd:{qid} wdt:P452 ?industry. }}
          OPTIONAL {{ wd:{qid} wdt:P17 ?country. }}
          OPTIONAL {{ wd:{qid} wdt:P1128 ?employees. }}
          OPTIONAL {{ wd:{qid} wdt:P6262 ?linkedinId. }}
          OPTIONAL {{ wd:{qid} wdt:P154 ?logoUrl. }}
          OPTIONAL {{ wd:{qid} wdt:P159 ?headquarter. }} # P159: headquarters
          OPTIONAL {{ wd:{qid} wdt:P281 ?hqNumber. }}    # P281: postal code/HQ number (common usage, might need refinement)

          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
        }}
        LIMIT 1
        """
        query_url = "https://query.wikidata.org/sparql"
        headers = {
            "Accept": "application/sparql-results+json"
        }
        params = {
            "query": sparql_query
        }
        query_response = requests.get(query_url, headers=headers, params=params, timeout=5)
        query_response.raise_for_status()
        query_data = query_response.json()

        bindings = query_data.get("results", {}).get("bindings", [])
        if bindings:
            binding = bindings[0]
            industry = binding.get("industryLabel", {}).get("value", "")
            country = binding.get("countryLabel", {}).get("value", "")
            employee_size_str = binding.get("employees", {}).get("value", "")
            employee_size = int(employee_size_str) if employee_size_str and employee_size_str.isdigit() else ""

            linkedin_id = binding.get("linkedinId", {}).get("value", "")
            linkedin_url = f"https://www.linkedin.com/company/{linkedin_id}/" if linkedin_id else ""

            logo_url = binding.get("logoUrl", {}).get("value", "")
            headquarter = binding.get("headquarterLabel", {}).get("value", "")
            hq_number = binding.get("hqNumber", {}).get("value", "")

            app.logger.info(f"Wikidata data for '{company_name}': Industry='{industry}', Country='{country}', Employees='{employee_size}', LinkedIn='{linkedin_url}', Logo='{logo_url}', Headquarter='{headquarter}', HQ_Number='{hq_number}'")
            return {
                "industry": industry,
                "country": country,
                "employee_size": employee_size,
                "linkedin": linkedin_url,
                "logo": logo_url,
                "headquarter": headquarter,
                "hq_number": hq_number,
                "source": "Wikidata (SPARQL)"
            }
        app.logger.info(f"No specific property data found on Wikidata for QID '{qid}' (company: {company_name}).")
    except requests.exceptions.RequestException as e:
        app.logger.error(f"Wikidata network error for company '{company_name}': {e}", exc_info=True)
        send_error_notification("Wikidata API Error", f"Wikidata network error for company '{company_name}': {e}\nTraceback: {traceback.format_exc()}")
    except json.JSONDecodeError:
        app.logger.error(f"Wikidata JSON decode error for company '{company_name}'. Response: {query_response.text if 'query_response' in locals() else 'N/A'}", exc_info=True)
        send_error_notification("Wikidata API Error", f"Wikidata JSON decode error for company '{company_name}'. Response: {query_response.text if 'query_response' in locals() else 'N/A'}\nTraceback: {traceback.format_exc()}")
    except Exception as e:
        app.logger.error(f"Wikidata advanced unexpected error for company '{company_name}': {e}", exc_info=True)
        send_error_notification("Wikidata API Error", f"Wikidata advanced unexpected error for company '{company_name}': {e}\nTraceback: {traceback.format_exc()}")
    return None

def infer_company_info_with_ai(company_name):
    """Infers company information (industry, employee size, country, headquarter, hq number) and a confidence score using a generative AI model."""
    if not company_name:
        app.logger.warning("No company name provided for AI inference.")
        return None

    app.logger.info(f"Attempting AI inference for company: {company_name}")
    try:
        prompt_text = f"""
        Given the company name "{company_name}", infer the following information:
        1.  **Industry**: What industry does this company most likely belong to? Provide a common industry name.
        2.  **Employee Size**: Estimate the employee count or size range (e.g., "1-10", "11-50", "51-200", "201-500", "501-1000", "1001-5000", "10000+").
        3.  **Country**: What country is this company most likely headquartered in or primarily operates from?
        4.  **Headquarter**: What is the primary city and state/region of its headquarters?
        5.  **HQ Number**: What is the main phone number for its headquarters, if publicly available?

        Additionally, provide a **Confidence Score** for the inferred data as a whole, indicating how certain you are about the accuracy of the information provided (e.g., "High", "Medium", "Low").

        Return the answer in a JSON format. If any specific information cannot be reasonably inferred, use "N/A" for that field. If the entire inference is highly uncertain, use "Low" for the confidence score.

        Example JSON format:
        {{
            "industry": "Software",
            "employee_size": "501-1000",
            "country": "USA",
            "headquarter": "Mountain View, CA",
            "hq_number": "+1-650-253-0000",
            "confidence_score": "High"
        }}
        """

        chat_history = [{"role": "user", "parts": [{"text": prompt_text}]}]

        payload = {
            "contents": chat_history,
            "generationConfig": {
                "responseMimeType": "application/json",
                "responseSchema": {
                    "type": "OBJECT",
                    "properties": {
                        "industry": {"type": "STRING"},
                        "employee_size": {"type": "STRING"},
                        "country": {"type": "STRING"},
                        "headquarter": {"type": "STRING"},
                        "hq_number": {"type": "STRING"},
                        "confidence_score": {"type": "STRING"}
                    },
                    "propertyOrdering": ["industry", "employee_size", "country", "headquarter", "hq_number", "confidence_score"]
                }
            }
        }

        api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={API_KEY}"

        response = requests.post(api_url, headers={'Content-Type': 'application/json'}, json=payload, timeout=10)
        response.raise_for_status()

        result = response.json()

        if result.get("candidates") and result["candidates"][0].get("content") and result["candidates"][0]["content"].get("parts"):
            json_str = result["candidates"][0]["content"]["parts"][0].get("text")
            if json_str:
                try:
                    inferred_data = json.loads(json_str)
                    app.logger.info(f"AI inferred data for '{company_name}': {inferred_data}")
                    return {
                        "industry": inferred_data.get("industry", "N/A"),
                        "employee_size": inferred_data.get("employee_size", "N/A"),
                        "country": inferred_data.get("country", "N/A"),
                        "headquarter": inferred_data.get("headquarter", "N/A"),
                        "hq_number": inferred_data.get("hq_number", "N/A"),
                        "ai_confidence_score": inferred_data.get("confidence_score", "N/A"),
                        "source": "AI-inferred (Gemini)"
                    }
                except json.JSONDecodeError:
                    app.logger.error(f"AI response JSON decode error for '{company_name}'. Raw response: {json_str}", exc_info=True)
                    send_error_notification("AI Inference Error", f"AI response JSON decode error for '{company_name}'. Raw response: {json_str}\nTraceback: {traceback.format_exc()}")
                    return None
            else:
                app.logger.warning(f"AI response parts did not contain text for '{company_name}'. Full result: {result}")
                return None
        else:
            app.logger.warning(f"AI response structure unexpected for '{company_name}'. Full result: {result}")
            return None

    except requests.exceptions.RequestException as e:
        app.logger.error(f"AI inference network error for '{company_name}': {e}", exc_info=True)
        send_error_notification("AI Inference Error", f"AI inference network error for '{company_name}': {e}\nTraceback: {traceback.format_exc()}")
    except Exception as e:
        app.logger.error(f"AI inference unexpected error for '{company_name}': {e}", exc_info=True)
        send_error_notification("AI Inference Error", f"AI inference unexpected error for '{company_name}': {e}\nTraceback: {traceback.format_exc()}")
    return None

def fetch_company_info_from_ip(ip_address):
    """Fetches company/ISP information from an IP address using ip-api.com."""
    if not ip_address or ip_address in ["127.0.0.1", "localhost", "::1"]:
        app.logger.warning(f"Skipping IP lookup for local address: {ip_address}")
        return None, None, None, None # Return None for all geo fields

    app.logger.info(f"Attempting to fetch geo info from IP: {ip_address}")
    try:
        api_url = f"http://ip-api.com/json/{ip_address}?fields=status,message,country,regionName,city,isp,org,as,query"
        response = requests.get(api_url, timeout=2)
        response.raise_for_status()
        data = response.json()

        if data.get("status") == "success":
            country = data.get("country", "Unknown")
            region = data.get("regionName", "Unknown")
            city = data.get("city", "Unknown")
            company_name_from_ip = data.get("org") or data.get("isp", "")

            app.logger.info(f"IP info for {ip_address}: Company='{company_name_from_ip}', Country='{country}', Region='{region}', City='{city}'")
            return company_name_from_ip, country, region, city
        else:
            app.logger.warning(f"IP-API.com failed for {ip_address}: {data.get('message', 'Unknown error')}")
            return None, None, None, None
    except requests.exceptions.RequestException as e:
        app.logger.error(f"IP-API.com network error for IP '{ip_address}': {e}", exc_info=True)
        send_error_notification("IP API Error", f"IP-API.com network error for IP '{ip_address}': {e}\nTraceback: {traceback.format_exc()}")
        return None, None, None, None
    except json.JSONDecodeError:
        app.logger.error(f"IP-API.com JSON decode error for IP '{ip_address}'. Response: {response.text if 'response' in locals() else 'N/A'}", exc_info=True)
        send_error_notification("IP API Error", f"IP-API.com JSON decode error for IP '{ip_address}'. Response: {response.text if 'response' in locals() else 'N/A'}\nTraceback: {traceback.format_exc()}")
        return None, None, None, None
    except Exception as e:
        app.logger.error(f"IP-API.com unexpected error for IP '{ip_address}': {e}", exc_info=True)
        send_error_notification("IP API Error", f"IP-API.com unexpected error for IP '{ip_address}': {e}\nTraceback: {traceback.format_exc()}")
        return None, None, None, None

def _lookup_company_details(domain, company_name_hint=None):
    """
    Helper function to perform company information lookup from various sources.
    Prioritizes master_companies DB, then Excel, then Clearbit, Wikidata, AI.
    """
    base_domain = domain.split(".")[0]
    app.logger.info(f"Starting company details lookup for domain: {domain}, hint: {company_name_hint}")

    company_data_result = {
        "company_name": company_name_hint if company_name_hint else "",
        "domain": domain, # Add domain to the result for master_companies
        "industry": "",
        "employee_size": "",
        "country": "",
        "logo": "",
        "linkedin": "",
        "source": "",
        "headquarter": "",
        "hq_number": "",
        "ai_confidence_score": ""
    }

    # 1. Try to get from master_companies DB first (most reliable, de-duplicated source)
    master_data = get_from_master_companies(company_name=company_name_hint, domain=domain)
    if master_data:
        # Map master_data fields to the expected output format
        company_data_result.update({
            "company_name": master_data.get("company_name", ""),
            "domain": master_data.get("domain", domain),
            "industry": master_data.get("industry", ""),
            "employee_size": master_data.get("employee_size", ""),
            "country": master_data.get("country", ""),
            "logo": master_data.get("logo", ""),
            "linkedin": master_data.get("linkedin_url", ""), # master_companies stores as linkedin_url
            "source": master_data.get("source_info", "") + " (Master DB)",
            "headquarter": master_data.get("headquarter", ""),
            "hq_number": master_data.get("hq_number", ""),
            "ai_confidence_score": master_data.get("ai_confidence_score", "")
        })
        app.logger.info(f"Data for domain '{domain}' found in master_companies.")
        return company_data_result

    # 2. Try to get from company_cache (for external API call cache)
    cached = get_from_cache(domain)
    if cached:
        company_data_result.update(cached)
        app.logger.info(f"Data for domain '{domain}' found in company_cache.")
        # If found in cache, also try to update/insert into master_companies
        if get_from_master_companies(company_name=company_data_result.get("company_name"), domain=domain):
            update_master_companies(company_data_result)
        else:
            insert_master_companies(company_data_result)
        return company_data_result

    # Normalize company name hint for better lookup in Excel/external APIs
    normalized_company_name_hint = normalize_company_name(company_name_hint or base_domain)

    # 3. Try Excel DB (if available)
    if not df.empty:
        # Adjusted for single word alias (df['auto_alias'] is the first word of normalized name)
        exact_match = df[df['auto_alias'] == (normalized_company_name_hint.split()[0] if normalized_company_name_hint else None)]
        if not exact_match.empty:
            row = exact_match.iloc[0].astype(object)
            company_data_result.update({
                "company_name": str(row.get("company_name", "")),
                "industry": str(row.get("industry", "")),
                "employee_size": int(row.get("employee_size", 0)) if pd.notnull(row.get("employee_size")) else "",
                "country": str(row.get("country", "")),
                "source": "Excel DB (exact auto-alias match)"
            })
            app.logger.info(f"Found exact match in Excel for domain: {domain}")
            # Cache and add/update master_companies
            save_to_cache(domain, company_data_result)
            if get_from_master_companies(company_name=company_data_result.get("company_name"), domain=domain):
                update_master_companies(company_data_result)
            else:
                insert_master_companies(company_data_result)
            return company_data_result

        best_match = None
        highest_score = -1
        for _, row in df.iterrows():
            alias_to_compare = row['auto_alias']
            if alias_to_compare:
                score = fuzz.ratio(normalized_company_name_hint, alias_to_compare)
                if score > highest_score and score >= FUZZY_MATCH_THRESHOLD:
                    highest_score = score
                    best_match = row
        if best_match is not None:
            row = best_match.astype(object)
            company_data_result.update({
                "company_name": str(row.get("company_name", "")),
                "industry": str(row.get("industry", "")),
                "employee_size": int(row.get("employee_size", 0)) if pd.notnull(row.get("employee_size")) else "",
                "country": str(row.get("country", "")),
                "source": f"Excel DB (fuzzy match: {highest_score}%)"
            })
            app.logger.info(f"Found fuzzy match in Excel for domain: {domain} with score {highest_score}")
            # Cache and add/update master_companies
            save_to_cache(domain, company_data_result)
            if get_from_master_companies(company_name=company_data_result.get("company_name"), domain=domain):
                update_master_companies(company_data_result)
            else:
                insert_master_companies(company_data_result)
            return company_data_result
    else:
        app.logger.info("Excel DataFrame is empty, skipping Excel lookup.")

    # 4. Try Clearbit Autocomplete
    clearbit_query = company_data_result.get("company_name") or base_domain
    normalized_clearbit_query = normalize_company_name(clearbit_query) if clearbit_query else ""
    clearbit_data = fetch_from_clearbit_autocomplete(normalized_clearbit_query)
    if clearbit_data:
        company_data_result.update(clearbit_data)
        if "Clearbit Autocomplete API" not in company_data_result["source"]:
            company_data_result["source"] += ", + Clearbit Autocomplete API" if company_data_result["source"] else "Clearbit Autocomplete API"
        app.logger.info(f"Data enriched with Clearbit for domain: {domain}")
    else:
        app.logger.info(f"No initial data from Clearbit for domain: {domain}")

    # 5. Try Wikidata (after potentially getting a company name from Clearbit)
    company_name_for_wiki = company_data_result.get("company_name") or base_domain
    normalized_wiki_query = normalize_company_name(company_name_for_wiki) if company_name_for_wiki else ""
    if normalized_wiki_query:
        wiki_data = fetch_from_wikidata_advanced(normalized_wiki_query)
        if wiki_data:
            # Merge Wikidata info, prioritizing existing valid data
            for key, value in wiki_data.items():
                if key not in ["source"] and value and value != "N/A":
                    # Only update if current value is empty or 'N/A'
                    if not company_data_result.get(key) or company_data_result.get(key) == 'N/A':
                        company_data_result[key] = value

            if "Wikidata" not in company_data_result["source"]:
                company_data_result["source"] += ", + Wikidata (SPARQL)" if company_data_result["source"] else "Wikidata (SPARQL)"
            app.logger.info(f"Data enriched with Wikidata for domain: {domain}")
        else:
            app.logger.info(f"Wikidata lookup failed for '{normalized_wiki_query}' for domain: {domain}")

    # 6. Try AI inference for remaining missing fields (after all other sources)
    if company_data_result.get("company_name"):
        needs_ai_inference = (
            not company_data_result.get("industry") or company_data_result.get("industry") == "N/A" or
            not company_data_result.get("employee_size") or company_data_result.get("employee_size") == "N/A" or
            not company_data_result.get("country") or company_data_result.get("country") == "N/A" or
            not company_data_result.get("headquarter") or company_data_result.get("headquarter") == "N/A" or
            not company_data_result.get("hq_number") or company_data_result.get("hq_number") == "N/A" or
            not company_data_result.get("ai_confidence_score") or company_data_result.get("ai_confidence_score") == "N/A"
        )

        if needs_ai_inference:
            ai_inferred_data = infer_company_info_with_ai(company_data_result["company_name"])
            if ai_inferred_data:
                # Merge AI inferred info, prioritizing existing valid data
                for key, value in ai_inferred_data.items():
                    if key not in ["source"] and value and value != "N/A":
                        if not company_data_result.get(key) or company_data_result.get(key) == 'N/A':
                            company_data_result[key] = value

                if "AI-inferred" not in company_data_result["source"]:
                    company_data_result["source"] += ", + AI-inferred (Gemini)" if company_data_result["source"] else "AI-inferred (Gemini)"
                app.logger.info(f"Data enriched with AI inference for domain: {domain}")
            else:
                log_company_name = company_data_result.get('company_name') or domain or 'Unknown Company'
                app.logger.info(f"AI inference failed for company: {log_company_name}")
    else:
        app.logger.info(f"No company name to use for AI inference for domain: {domain}")

    if not company_data_result.get("company_name") or company_data_result.get("company_name") == 'N/A':
        app.logger.info(f"No matching company name found after all sources for domain: {domain}")
        return None

    # Save/Update to cache and master_companies after all enrichment attempts
    save_to_cache(domain, company_data_result)
    master_existing = get_from_master_companies(company_name=company_data_result.get("company_name"), domain=company_data_result.get("domain"))
    if master_existing:
        update_master_companies(company_data_result)
    else:
        insert_master_companies(company_data_result)

    return company_data_result

# --- Flask Routes ---
@app.route("/")
def form():
    """Renders the HTML form for adding company emails and for IP lookup."""
    return render_template("add_company.html")

@app.route("/dashboard")
def dashboard():
    all_data = []
    event_log_data = []
    conn = None

    # Ensure data_exports directory exists and create a dummy CSV if none exist
    data_exports_dir = "data_exports"
    if not os.path.exists(data_exports_dir):
        os.makedirs(data_exports_dir)
        app.logger.info(f"Created data directory: {data_exports_dir}")

    csv_files = [f for f in os.listdir(data_exports_dir) if f.endswith(".csv")]
    if not csv_files:
        app.logger.warning(f"No CSV files found in '{data_exports_dir}'. Creating a dummy CSV.")
        dummy_csv_path = os.path.join(data_exports_dir, f"{date.today().strftime('%Y-%m-%d')}_dummy_submissions.csv")
        with open(dummy_csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                "timestamp", "email", "company_name", "industry", "employee_size",
                "country", "headquarter", "hq_number", "ai_confidence_score",
                "linkedin_url", "source", "personal_phone_number"
            ])
            writer.writerow([
                datetime.now().isoformat(), "dummy1@example.com", "Dummy Corp", "Tech", "101-500",
                "USA", "San Francisco, CA", "N/A", "High",
                "https://linkedin.com/dummycorp", "Dummy Data", "N/A"
            ])
            writer.writerow([
                datetime.now().isoformat(), "dummy2@example.com", "Acme Inc", "Manufacturing", "51-200",
                "Canada", "Toronto, ON", "N/A", "Medium",
                "https://linkedin.com/acmeinc", "Dummy Data", "N/A"
            ])
        app.logger.info(f"Created dummy CSV: {dummy_csv_path}")

    # Load data from CSV files for the main "Company Submissions" table
    # This ensures fields like timestamp, email, personal_phone_number are present
    app.logger.info(f"Attempting to load data from: {os.path.abspath(data_exports_dir)}")
    for filename in sorted(csv_files):
        filepath = os.path.join(data_exports_dir, filename)
        try:
            with open(filepath, "r", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                expected_headers = [
                    "timestamp", "email", "company_name", "industry", "employee_size",
                    "country", "headquarter", "hq_number", "ai_confidence_score",
                    "linkedin_url", "source", "personal_phone_number"
                ]
                if not reader.fieldnames:
                    app.logger.warning(f"CSV file '{filename}' has no headers. Skipping.")
                    continue

                # Ensure all expected headers are present, fill missing with empty string
                file_data = []
                for row in reader:
                    processed_row = {key: row.get(key, '') for key in expected_headers}
                    processed_row["date"] = filename.split("_")[0] # Add date from filename if needed for filtering
                    file_data.append(processed_row)
                app.logger.info(f"Successfully loaded {len(file_data)} rows from '{filename}'.")
                all_data.extend(file_data)
        except Exception as e:
            app.logger.error(f"Error reading CSV file '{filepath}': {e}", exc_info=True)
            send_error_notification("Dashboard CSV Read Error", f"Error reading CSV '{filepath}' for dashboard: {e}\nTraceback: {traceback.format_exc()}")
    
    # Fetch event log data (this part remains the same as it was already correct)
    try:
        conn = sqlite3.connect("company_cache.db")
        conn.row_factory = sqlite3.Row # Allows accessing columns by name
        cur = conn.cursor()

        # Fetch event log data
        cur.execute("SELECT * FROM event_log ORDER BY timestamp DESC LIMIT 500") # Limit to recent events for performance
        event_log_rows = cur.fetchall()

        # Insert dummy event log data if table is empty
        if not event_log_rows:
            app.logger.warning("Event log table is empty. Inserting dummy event log data.")
            dummy_events = [
                (datetime.now().isoformat(), "Page_View", "127.0.0.1", "Mozilla/5.0", 1920, 1080, "USA", "California", "San Francisco", json.dumps({"page": "/", "referrer": ""})),
                (datetime.now().isoformat(), "Modal_Opened", "127.0.0.1", "Mozilla/5.0", 1920, 1080, "USA", "California", "San Francisco", json.dumps({"modalName": "Email_Modal"})),
                (datetime.now().isoformat(), "Form_Submission_Success", "127.0.0.1", "Mozilla/5.0", 1920, 1080, "USA", "California", "San Francisco", json.dumps({"email": "test@example.com", "company_name": "TestCo", "phone_provided": "Yes"}))
            ]
            cur.executemany("""
                INSERT INTO event_log (timestamp, event_name, user_ip, user_agent, screen_width, screen_height, user_ip_country, user_ip_region, user_ip_city, event_details)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, dummy_events)
            conn.commit()
            cur.execute("SELECT * FROM event_log ORDER BY timestamp DESC LIMIT 500") # Re-fetch after inserting
            event_log_rows = cur.fetchall()

        processed_event_log = []
        for row in event_log_rows:
            event_dict = dict(row) # Use dict(row) since row_factory is set
            # Process event_details to ensure it's a Python object (dictionary)
            if 'event_details' in event_dict and event_dict['event_details']:
                try:
                    # Attempt to parse the event_details string into a Python object
                    details_obj = json.loads(event_dict['event_details'])
                    event_dict['event_details'] = details_obj # Store as object, not string
                except (json.JSONDecodeError, TypeError):
                    # If it's not valid JSON, or already a simple string, keep it as is
                    event_dict['event_details'] = str(event_dict['event_details']) # Ensure it's a string for fallback display
            processed_event_log.append(event_dict)
        event_log_data = processed_event_log

    except Exception as e:
        app.logger.error(f"Error fetching event_log data for dashboard: {e}", exc_info=True)
        send_error_notification("Dashboard Event Log Data Fetch Error", f"Error fetching event_log data: {e}\nTraceback: {traceback.format_exc()}")
        event_log_data = [] # Fallback to empty list if there's an error
    finally:
        if conn:
            conn.close()

    app.logger.info(f"Total company submissions prepared for dashboard: {len(all_data)}")
    app.logger.info(f"Total event log entries prepared for dashboard: {len(event_log_data)}")
    return render_template("dashboard.html", data=all_data, event_log=event_log_data)


@app.route("/traffic-source-data")
def traffic_source_data():
    conn = None
    try:
        conn = sqlite3.connect('company_cache.db')
        cursor = conn.cursor()
        # Query event_log table for traffic sources (e.g., Page_View events)
        cursor.execute("SELECT json_extract(event_details, '$.referrer') as referrer, COUNT(*) FROM event_log WHERE event_name = 'Page_View' GROUP BY referrer")
        data = cursor.fetchall()

        source_counts = Counter()
        for row in data:
            referrer = row[0]
            count = row[1]
            if referrer:
                if "google" in referrer.lower():
                    source_counts["Organic"] += count
                elif any(s in referrer.lower() for s in ["facebook", "instagram", "linkedin", "twitter", "x.com"]):
                    source_counts["Social"] += count
                elif referrer == "Direct" or not referrer: # Treat empty referrer as Direct too
                    source_counts["Direct"] += count
                else:
                    source_counts["Referral"] += count
            else:
                source_counts["Direct"] += count # Default to Direct if referrer is None/empty

        return jsonify(dict(source_counts))
    except sqlite3.Error as e:
        app.logger.error(f"Database error in traffic_source_data: {e}", exc_info=True)
        return jsonify({'error': 'Database error'}), 500
    finally:
        if conn:
            conn.close()

@app.route("/traffic-country-data")
def traffic_country_data():
    conn = None
    try:
        conn = sqlite3.connect('company_cache.db')
        cursor = conn.cursor()
        cursor.execute("SELECT user_ip_country, COUNT(*) FROM event_log GROUP BY user_ip_country")
        data = cursor.fetchall()

        country_counts = Counter()
        for row in data:
            country = row[0] if row[0] else "Unknown"
            count = row[1]
            country_counts[country] += count

        return jsonify(dict(country_counts))
    except sqlite3.Error as e:
        app.logger.error(f"Database error in traffic_country_data: {e}", exc_info=True)
        return jsonify({'error': 'Database error'}), 500
    finally:
        if conn:
            conn.close()

@app.route("/traffic-source-data-today")
def traffic_source_data_today():
    conn = None
    try:
        conn = sqlite3.connect('company_cache.db')
        cursor = conn.cursor()
        cursor.execute("SELECT json_extract(event_details, '$.referrer') as referrer, COUNT(*) FROM event_log WHERE date(timestamp) = date('now') AND event_name = 'Page_View' GROUP BY referrer")
        data = cursor.fetchall()

        source_counts = Counter()
        for row in data:
            referrer = row[0]
            count = row[1]
            if referrer:
                if "google" in referrer.lower():
                    source_counts["Organic"] += count
                elif any(s in referrer.lower() for s in ["facebook", "instagram", "linkedin", "twitter", "x.com"]):
                    source_counts["Social"] += count
                elif referrer == "Direct" or not referrer:
                    source_counts["Direct"] += count
                else:
                    source_counts["Referral"] += count
            else:
                source_counts["Direct"] += count

        return jsonify(dict(source_counts))
    except sqlite3.Error as e:
        app.logger.error(f"Database error in traffic_source_data_today: {e}", exc_info=True)
        return jsonify({'error': 'Database error'}), 500
    finally:
        if conn:
            conn.close()

@app.route("/traffic-country-data-today")
def traffic_country_data_today():
    conn = None
    try:
        conn = sqlite3.connect('company_cache.db')
        cursor = conn.cursor()
        cursor.execute("SELECT user_ip_country, COUNT(*) FROM event_log WHERE date(timestamp) = date('now') GROUP BY user_ip_country")
        data = cursor.fetchall()

        country_counts = Counter()
        for row in data:
            country = row[0] if row[0] else "Unknown"
            count = row[1]
            country_counts[country] += count

        return jsonify(dict(country_counts))
    except sqlite3.Error as e:
        app.logger.error(f"Database error in traffic_country_data_today: {e}", exc_info=True)
        return jsonify({'error': 'Database error'}), 500
    finally:
        if conn:
            conn.close()

@app.route("/track-event", methods=["POST"])
def track_event():
    """
    API endpoint to receive and log client-side events (pixel tracking data).
    Now includes user's location (country, region, city) and more detailed event_details.
    """
    try:
        data = request.get_json()
        event_name = data.get("eventName")
        event_details = data.get("eventDetails", {})
        user_ip = request.headers.get('X-Forwarded-For', request.remote_addr)
        user_agent = data.get("userAgent", request.headers.get('User-Agent', 'Unknown')) # Use client-sent UA if available
        screen_width = data.get("screenWidth")
        screen_height = data.get("screenHeight")

        # Fetch detailed IP geolocation for the event
        # Ensure fetch_company_info_from_ip is robust for local IPs
        company_name_from_ip, user_ip_country, user_ip_region, user_ip_city = fetch_company_info_from_ip(user_ip)
        user_ip_country = user_ip_country if user_ip_country else "Unknown"
        user_ip_region = user_ip_region if user_ip_region else "Unknown"
        user_ip_city = user_ip_city if user_ip_city else "Unknown"

        conn = sqlite3.connect("company_cache.db")
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO event_log (timestamp, event_name, user_ip, user_agent, screen_width, screen_height, user_ip_country, user_ip_region, user_ip_city, event_details)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            datetime.now().isoformat(),
            event_name,
            user_ip,
            user_agent,
            screen_width,
            screen_height,
            user_ip_country,
            user_ip_region,
            user_ip_city,
            json.dumps(event_details) # event_details can contain time_spent, business_interest etc.
        ))
        conn.commit()
        conn.close()
        app.logger.info(f"Event logged: {event_name} from IP {user_ip} (Country: {user_ip_country})")
        return jsonify({"status": "success"}), 200
    except Exception as e:
        app.logger.error(f"Error logging event: {e}", exc_info=True)
        send_error_notification("Event Tracking Error", f"Error logging event: {e}\nTraceback: {traceback.format_exc()}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/get-company-info", methods=["POST"])
def get_company_info():
    """
    API endpoint to get company information based on an email domain.
    It first checks temporary email, then email deliverability (with cache), then proceeds with company info lookup.
    """
    app.logger.info("Received request for email-based company info.")
    email = ""
    domain = ""
    try:
        data = request.get_json()
        email = data.get("email", "").strip()

        if not email or "@" not in email:
            app.logger.warning(f"Invalid email format received: '{email}'")
            # HTML content for invalid email alert
            invalid_email_html_content = f"""
            <html>
            <head>
                <style>
                    body {{ font-family: Arial, sans-serif; background-color: #f4f4f4; margin: 0; padding: 0; }}
                    .email-container {{ max-width: 600px; margin: 20px auto; background-color: #ffffff; border-radius: 8px; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1); overflow: hidden; border-left: 5px solid #ffc107; }}
                    .header {{ background-color: #ffc107; color: #ffffff; padding: 20px; text-align: center; font-size: 20px; font-weight: bold; }}
                    .content {{ padding: 20px 30px; color: #333333; line-height: 1.6; }}
                    .data-item {{ margin-bottom: 5px; }}
                    .data-label {{ font-weight: bold; color: #555555; display: inline-block; width: 120px; }}
                    .footer {{ background-color: #f0f0f0; color: #777777; padding: 15px; text-align: center; font-size: 12px; }}
                </style>
            </head>
            <body>
                <div class="email-container">
                    <div class="header">Alert: Invalid Email Attempt!</div>
                    <div class="content">
                        <p>Hello Team,</p>
                        <p>An attempt was made to enter an email with an invalid format.</p>
                        <div class="data-item"><span class="data-label">Attempted Email:</span> {email}</div>
                        <div class="data-item"><span class="data-label">Reason:</span> Invalid Email Format</div>
                        <div class="data-item"><span class="data-label">Timestamp:</span> {datetime.now().isoformat()}</div>
                        <p>Please review if necessary.</p>
                        <p>Best regards,<br>Your System Team</p>
                    </div>
                    <div class="footer">This is an automated notification.</div>
                </div>
            </body>
            </html>
            """
            send_alert_notification("Invalid Email Attempt", invalid_email_html_content, is_html=True)
            return jsonify({
                "error": "Invalid email format. Please provide a valid email address.",
                "is_deliverable": False,
                "company_name": "", "industry": "", "employee_size": "", "country": "", "logo": "", "linkedin": "", "source": "",
                "headquarter": "", "hq_number": "", "ai_confidence_score": ""
            }), 200

        domain = email.split("@")[1].lower()

        if domain in TEMP_EMAIL_DOMAINS:
            app.logger.warning(f"Temporary/disposable email detected: {email}")
            # HTML content for temporary email alert
            temp_email_html_content = f"""
            <html>
            <head>
                <style>
                    body {{ font-family: Arial, sans-serif; background-color: #f4f4f4; margin: 0; padding: 0; }}
                    .email-container {{ max-width: 600px; margin: 20px auto; background-color: #ffffff; border-radius: 8px; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1); overflow: hidden; border-left: 5px solid #ffc107; }}
                    .header {{ background-color: #ffc107; color: #ffffff; padding: 20px; text-align: center; font-size: 20px; font-weight: bold; }}
                    .content {{ padding: 20px 30px; color: #333333; line-height: 1.6; }}
                    .data-item {{ margin-bottom: 5px; }}
                    .data-label {{ font-weight: bold; color: #555555; display: inline-block; width: 120px; }}
                    .footer {{ background-color: #f0f0f0; color: #777777; padding: 15px; text-align: center; font-size: 12px; }}
                </style>
            </head>
            <body>
                <div class="email-container">
                    <div class="header">Alert: Temporary Email Attempt!</div>
                    <div class="content">
                        <p>Hello Team,</p>
                        <p>An attempt was made to enter a temporary/disposable email address.</p>
                        <div class="data-item"><span class="data-label">Attempted Email:</span> {email}</div>
                        <div class="data-item"><span class="data-label">Reason:</span> Temporary/Disposable Email</div>
                        <div class="data-item"><span class="data-label">Timestamp:</span> {datetime.now().isoformat()}</div>
                        <p>Please review if necessary.</p>
                        <p>Best regards,<br>Your System Team</p>
                    </div>
                    <div class="footer">This is an automated notification.</div>
                </div>
            </body>
            </html>
            """
            send_alert_notification("Temporary Email Attempt", temp_email_html_content, is_html=True)
            return jsonify({
                "error": "Temporary/disposable email addresses are not allowed.",
                "is_deliverable": False,
                "company_name": "", "industry": "", "employee_size": "", "country": "", "logo": "", "linkedin": "", "source": "Temp Email",
                "headquarter": "", "hq_number": "", "ai_confidence_score": ""
            }), 200

        is_deliverable, deliverability_message = check_email_deliverability_abstract(email)

        if not is_deliverable:
            app.logger.info(f"Email '{email}' is not deliverable: {deliverability_message}")
            # HTML content for undeliverable email alert
            undeliverable_email_html_content = f"""
            <html>
            <head>
                <style>
                    body {{ font-family: Arial, sans-serif; background-color: #f4f4f4; margin: 0; padding: 0; }}
                    .email-container {{ max-width: 600px; margin: 20px auto; background-color: #ffffff; border-radius: 8px; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1); overflow: hidden; border-left: 5px solid #dc3545; }}
                    .header {{ background-color: #dc3545; color: #ffffff; padding: 20px; text-align: center; font-size: 20px; font-weight: bold; }}
                    .content {{ padding: 20px 30px; color: #333333; line-height: 1.6; }}
                    .data-item {{ margin-bottom: 5px; }}
                    .data-label {{ font-weight: bold; color: #555555; display: inline-block; width: 120px; }}
                    .footer {{ background-color: #f0f0f0; color: #777777; padding: 15px; text-align: center; font-size: 12px; }}
                </style>
            </head>
            <body>
                <div class="email-container">
                    <div class="header">Alert: Undeliverable Email Attempt!</div>
                    <div class="content">
                        <p>Hello Team,</p>
                        <p>An attempt was made to enter an email address that was flagged as undeliverable.</p>
                        <div class="data-item"><span class="data-label">Attempted Email:</span> {email}</div>
                        <div class="data-item"><span class="data-label">Reason:</span> {deliverability_message}</div>
                        <div class="data-item"><span class="data-label">Timestamp:</span> {datetime.now().isoformat()}</div>
                        <p>Please review if necessary.</p>
                        <p>Best regards,<br>Your System Team</p>
                    </div>
                    <div class="footer">This is an automated notification.</div>
                </div>
            </body>
            </html>
            """
            send_alert_notification("Email Undeliverable Attempt", undeliverable_email_html_content, is_html=True)
            return jsonify({
                "error": f"Email is not deliverable: {deliverability_message}",
                "is_deliverable": False,
                "company_name": "", "industry": "", "employee_size": "", "country": "", "logo": "", "linkedin": "", "source": "Email Not Deliverable",
                "headquarter": "", "hq_number": "", "ai_confidence_score": ""
            }), 200

        app.logger.info(f"Processing email: {email}, domain: {domain}")

        company_data_result = _lookup_company_details(domain, company_name_hint=domain.split('.')[0])

        if company_data_result:
            company_data_result["is_deliverable"] = True
            return jsonify(company_data_result)
        else:
            app.logger.info(f"No company info found after all sources for domain: {domain} (email '{email}')")
            return jsonify({"error": "No matching company found in any source", "is_deliverable": True}), 404

    except Exception as e:
        app.logger.error(f"An unexpected error occurred in get_company_info for email '{email}': {e}", exc_info=True)
        # Send an email notification for this unexpected server-side error
        send_error_notification("Get Company Info Runtime Error", f"An unexpected error occurred while processing email '{email}': {e}\nTraceback: {traceback.format_exc()}")
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        if domain:
            # Clear domain deliverability cache, but not company cache as it's useful for master_companies population
            clear_domain_deliverability_cache(domain)


@app.route("/get-company-info-by-domain", methods=["POST"])
def get_company_info_by_domain():
    """
    API endpoint to get company information based on a domain name directly.
    It skips email deliverability and temporary email checks.
    """
    app.logger.info("Received request for domain-based company info.")
    domain = ""
    try:
        data = request.get_json()
        domain = data.get("domain", "").strip().lower()

        if not domain or "." not in domain:
            app.logger.warning(f"Invalid domain format received: '{domain}'")
            return jsonify({"error": "Invalid domain format. Please provide a valid domain (e.g., example.com)."}), 400

        domain = re.sub(r'^(https?://)?(www\.)?', '', domain).rstrip('/')
        app.logger.info(f"Processing domain: {domain}")

        company_data_result = _lookup_company_details(domain, company_name_hint=domain.split('.')[0])

        if company_data_result:
            company_data_result["is_deliverable"] = None
            return jsonify(company_data_result)
        else:
            app.logger.info(f"No company info found after all sources for domain: {domain}")
            return jsonify({"error": "No matching company found in any source"}), 404

    except Exception as e:
        app.logger.error(f"An unexpected error occurred in get_company_info_by_domain for domain '{domain}': {e}", exc_info=True)
        send_error_notification("Get Company Info By Domain Error", f"Unexpected error in get_company_info_by_domain for domain '{domain}': {e}\nTraceback: {traceback.format_exc()}")
        return jsonify({"error": "An internal server error occurred."}), 500


@app.route("/get-ip-company-info", methods=["GET"])
def get_ip_company_info():
    """
    API endpoint to get company information based on the client's IP address.
    It fetches company/ISP from IP, then tries to enrich using Clearbit, Wikidata, and AI.
    """
    app.logger.info("Received request for IP-based company info.")
    ip_address = request.remote_addr

    if not ip_address:
        app.logger.warning("Could not determine client IP address.")
        return jsonify({"error": "Could not determine client IP address."}), 400

    company_data_result = {
        "company_name": "",
        "industry": "",
        "employee_size": "",
        "country": "",
        "logo": "",
        "linkedin": "",
        "source": "",
        "headquarter": "",
        "hq_number": "",
        "ai_confidence_score": "",
        "is_deliverable": None
    }

    try:
        # Fetch company name from IP, and also get location details
        company_name_from_ip, ip_country, ip_region, ip_city = fetch_company_info_from_ip(ip_address)

        if company_name_from_ip:
            company_data_result.update({
                "company_name": company_name_from_ip,
                "country": ip_country,
                "headquarter": f"{ip_city}, {ip_region}" if ip_city and ip_region else ip_city, # Combine city and region for headquarter
                "source": f"IP-API.com (IP: {ip_address})"
            })
            app.logger.info(f"Initial company info from IP: {company_data_result}")

            company_name_for_enrichment = company_data_result.get("company_name")
            if company_name_for_enrichment:
                normalized_enrichment_name = normalize_company_name(company_name_for_enrichment)

                clearbit_data = fetch_from_clearbit_autocomplete(normalized_enrichment_name)
                if clearbit_data:
                    # Merge Clearbit info, prioritizing existing valid data
                    for key, value in clearbit_data.items():
                        if key not in ["source"] and value and value != "N/A":
                            if not company_data_result.get(key) or company_data_result.get(key) == 'N/A':
                                company_data_result[key] = value

                    if "Clearbit Autocomplete API" not in company_data_result["source"]:
                        company_data_result["source"] += ", + Clearbit Autocomplete API" if company_data_result["source"] else "Clearbit Autocomplete API"
                    app.logger.info(f"IP data enriched with Clearbit for company: {company_name_for_enrichment}")

                wiki_data = fetch_from_wikidata_advanced(normalized_enrichment_name)
                if wiki_data:
                    # Merge Wikidata info, prioritizing existing valid data
                    for key, value in wiki_data.items():
                        if key not in ["source"] and value and value != "N/A":
                            if not company_data_result.get(key) or company_data_result.get(key) == 'N/A':
                                company_data_result[key] = value
                    if "Wikidata" not in company_data_result["source"]:
                        company_data_result["source"] += ", + Wikidata (SPARQL)" if company_data_result["source"] else "Wikidata (SPARQL)"
                    app.logger.info(f"IP data enriched with Wikidata for company: {company_name_for_enrichment}")

                needs_ai_inference = (
                    not company_data_result.get("industry") or company_data_result.get("industry") == "N/A" or
                    not company_data_result.get("employee_size") or company_data_result.get("employee_size") == "N/A" or
                    not company_data_result.get("country") or company_data_result.get("country") == "N/A" or
                    not company_data_result.get("headquarter") or company_data_result.get("headquarter") == "N/A" or
                    not company_data_result.get("hq_number") or company_data_result.get("hq_number") == "N/A" or
                    not company_data_result.get("ai_confidence_score") or company_data_result.get("ai_confidence_score") == "N/A"
                )
                if needs_ai_inference:
                    ai_inferred_data = infer_company_info_with_ai(normalized_enrichment_name)
                    if ai_inferred_data:
                        # Merge AI inferred info, prioritizing existing valid data
                        for key, value in ai_inferred_data.items():
                            if key not in ["source"] and value and value != "N/A":
                                if not company_data_result.get(key) or company_data_result.get(key) == 'N/A':
                                    company_data_result[key] = value

                        if "AI-inferred" not in company_data_result["source"]:
                            company_data_result["source"] += ", + AI-inferred (Gemini)" if company_data_result["source"] else "AI-inferred (Gemini)"
                        app.logger.info(f"IP data enriched with AI inference for company: {company_name_for_enrichment}")
                    else:
                        domain_for_logging = company_data_result.get('domain') or 'unknown.com'
                        log_company_name = company_data_result.get('company_name') or domain_for_logging or 'Unknown Company'
                        app.logger.info(f"AI inference failed for company: {log_company_name}")
            else:
                app.logger.info(f"No company name to use for AI inference for IP: {ip_address}")

            # Once enriched, save/update to master_companies if a company name exists
            if company_data_result.get("company_name") and company_data_result.get("company_name") != 'N/A':
                master_existing = get_from_master_companies(company_name=company_data_result.get("company_name"), domain=company_data_result.get("domain"))
                if master_existing:
                    update_master_companies(company_data_result)
                else:
                    insert_master_companies(company_data_result)

            return jsonify(company_data_result)
        else:
            app.logger.info(f"No initial company info found from IP-API.com for IP: {ip_address}")
            return jsonify({"error": "No company information found for your IP address."}), 404

    except Exception as e:
        app.logger.error(f"An unexpected error occurred in get_ip_company_info for IP '{ip_address}': {e}", exc_info=True)
        send_error_notification("Get IP Company Info Error", f"Unexpected error in get_ip_company_info for IP '{ip_address}': {e}\nTraceback: {traceback.format_exc()}")
        return jsonify({"error": "An internal server error occurred."}), 500

@app.route("/save-company-data", methods=["POST"])
def save_company_data():
    """
    API endpoint to save company data received from the frontend to a CSV file.
    Also updates/inserts into the master_companies SQLite table for deduplication.
    Now includes a personal phone number field.
    """
    app.logger.info("Received request to save company data.")
    try:
        data = request.get_json()
        app.logger.info(f"Data received for saving: {data}")

        data_exports_dir = "data_exports"
        today_date_str = datetime.now().strftime("%Y-%m-%d")
        csv_filename = os.path.join(data_exports_dir, f"{today_date_str}_company_submissions.csv")

        if not os.path.exists(data_exports_dir):
            os.makedirs(data_exports_dir)
            app.logger.info(f"Created directory: {data_exports_dir}")

        # Add 'personal_phone_number' to the fieldnames
        fieldnames = [
            "timestamp", "email", "company_name", "industry", "employee_size",
            "country", "headquarter", "hq_number", "ai_confidence_score",
            "linkedin_url", "source", "personal_phone_number" # New field
        ]

        row_data = {
            "timestamp": datetime.now().isoformat(),
            "email": data.get("email", ""),
            "company_name": data.get("company_name", ""),
            "industry": data.get("industry", ""),
            "employee_size": data.get("employee_size", ""),
            "country": data.get("country", ""),
            "headquarter": data.get("headquarter", ""),
            "hq_number": data.get("hq_number", ""),
            "ai_confidence_score": data.get("ai_confidence_score", ""),
            "linkedin_url": data.get("linkedin_url", ""),
            "source": data.get("source", "Manual Submission"),
            "personal_phone_number": data.get("personal_phone_number", "") # Get new field
        }

        # --- Deduplication logic for master_companies table ---
        # Note: personal_phone_number is *not* added to master_companies table
        # as it's considered personal contact, not company HQ info.
        company_name_for_dedupe = row_data.get("company_name")
        email_domain_for_dedupe = row_data.get("email").split('@')[1] if '@' in row_data.get("email", "") else None

        master_record = None
        if company_name_for_dedupe and company_name_for_dedupe != "N/A":
            master_record = get_from_master_companies(company_name=company_name_for_dedupe)
        if not master_record and email_domain_for_dedupe:
            master_record = get_from_master_companies(domain=email_domain_for_dedupe)

        # Prepare data for master_companies (ensure 'domain' and 'linkedin' keys are aligned)
        master_data_for_db = {
            "company_name": row_data.get("company_name"),
            "domain": email_domain_for_dedupe,
            "industry": row_data.get("industry"),
            "employee_size": row_data.get("employee_size"),
            "country": row_data.get("country"),
            "headquarter": row_data.get("headquarter"),
            "hq_number": row_data.get("hq_number"),
            "ai_confidence_score": row_data.get("ai_confidence_score"),
            "logo": data.get("logo", ""), # 'logo' comes from the initial fetched data, not row_data
            "linkedin": row_data.get("linkedin_url"), # Master DB expects 'linkedin_url' but enrichment uses 'linkedin'
            "source": row_data.get("source")
        }

        if master_record:
            update_master_companies(master_data_for_db)
        else:
            insert_master_companies(master_data_for_db)

        # --- Append to daily CSV log ---
        file_exists = os.path.exists(csv_filename)
        with open(csv_filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
                app.logger.info(f"Wrote header to new CSV file: {csv_filename}")
            writer.writerow(row_data)
            app.logger.info(f"Successfully wrote data to {csv_filename}")

        # Send notification for form submission
        # Example of a rich HTML template for submission alert:
        html_content = f"""
        <html>
        <head>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    background-color: #f4f4f4;
                    margin: 0;
                    padding: 0;
                }}
                .email-container {{
                    max-width: 600px;
                    margin: 20px auto;
                    background-color: #ffffff;
                    border-radius: 8px;
                    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
                    overflow: hidden;
                }}
                .header {{
                    background-color: #007bff;
                    color: #ffffff;
                    padding: 20px;
                    text-align: center;
                    font-size: 24px;
                    font-weight: bold;
                }}
                .content {{
                    padding: 20px 30px;
                    color: #333333;
                    line-height: 1.6;
                }}
                .content p {{
                    margin-bottom: 10px;
                }}
                .data-item {{
                    margin-bottom: 5px;
                }}
                .data-label {{
                    font-weight: bold;
                    color: #555555;
                    display: inline-block;
                    width: 150px; /* Adjust as needed */
                }}
                .footer {{
                    background-color: #f0f0f0;
                    color: #777777;
                    padding: 15px;
                    text-align: center;
                    font-size: 12px;
                }}
                .button {{
                    display: inline-block;
                    background-color: #007bff;
                    color: #ffffff !important;
                    padding: 10px 20px;
                    border-radius: 5px;
                    text-decoration: none;
                    margin-top: 20px;
                }}
            </style>
        </head>
        <body>
            <div class="email-container">
                <div class="header">
                    New Form Submission Alert!
                </div>
                <div class="content">
                    <p>Hello Team,</p>
                    <p>A new company data submission has been received through the form. Here are the details:</p>
                    <div class="data-item"><span class="data-label">Email:</span> {row_data.get('email', 'N/A')}</div>
                    <div class="data-item"><span class="data-label">Company Name:</span> {row_data.get('company_name', 'N/A')}</div>
                    <div class="data-item"><span class="data-label">Industry:</span> {row_data.get('industry', 'N/A')}</div>
                    <div class="data-item"><span class="data-label">Employee Size:</span> {row_data.get('employee_size', 'N/A')}</div>
                    <div class="data-item"><span class="data-label">Country:</span> {row_data.get('country', 'N/A')}</div>
                    <div class="data-item"><span class="data-label">Headquarter:</span> {row_data.get('headquarter', 'N/A')}</div>
                    <div class="data-item"><span class="data-label">HQ Number:</span> {row_data.get('hq_number', 'N/A')}</div>
                    <div class="data-item"><span class="data-label">AI Confidence Score:</span> {row_data.get('ai_confidence_score', 'N/A')}</div>
                    <div class="data-item"><span class="data-label">LinkedIn URL:</span> <a href="{row_data.get('linkedin_url', '#')}" target="_blank">{row_data.get('linkedin_url', 'N/A')}</a></div>
                    <div class="data-item"><span class="data-label">Personal Phone:</span> {row_data.get('personal_phone_number', 'N/A')}</div> <!-- New field in email -->
                    <div class="data-item"><span class="data-label">Source:</span> {row_data.get('source', 'N/A')}</div>
                    <div class="data-item"><span class="data-label">Timestamp:</span> {row_data.get('timestamp', 'N/A')}</div>

                    <p>You can view all submissions in the <a href="http://your-app-domain.com/dashboard" class="button">Analytics Dashboard</a>.</p>
                    <p>Best regards,<br>Your System Team</p>
                </div>
                <div class="footer">
                    This is an automated notification. Please do not reply.
                </div>
            </div>
        </body>
        </html>
        """
        send_alert_notification("New Form Submission Received", html_content, is_html=True)

        # Redirect to the thank you page
        return redirect(url_for('thank_you_page'))

    except Exception as e:
        app.logger.error(f"Error saving company data: {e}", exc_info=True)
        send_error_notification("Save Company Data Error", f"Failed to save company data: {e}\nTraceback: {traceback.format_exc()}")
        return jsonify({"error": f"Failed to save company data: {str(e)}"}), 500

# New route for the thank you page
@app.route("/thank-you")
def thank_you_page():
    """Renders the thank you page after successful form submission."""
    return render_template("thank_you.html")

# --- Background Jobs ---
# Dummy cache refresh job (can be expanded for other periodic tasks)
def refresh_cache_job():
    while True:
        app.logger.info("Background cache refresh job running (dummy).")
        # Add any other small periodic tasks here if needed
        time.sleep(3600) # Run every hour

def enrich_existing_data_job():
    """
    Background job to continuously enrich existing company data in master_companies.
    """
    app.logger.info("Starting background data enrichment job.")
    while True:
        conn = None
        try:
            conn = sqlite3.connect("company_cache.db")
            cur = conn.cursor()
            cur.execute("SELECT id, company_name, domain, industry, employee_size, country, headquarter, hq_number, ai_confidence_score, logo, linkedin_url FROM master_companies")
            companies_to_enrich = cur.fetchall()

            columns = [description[0] for description in cur.description] # Get column names
            app.logger.info(f"Found {len(companies_to_enrich)} companies to potentially enrich.")

            for company_row in companies_to_enrich:
                company_dict = dict(zip(columns, company_row))

                needs_enrichment = (
                    company_dict.get("industry", "N/A") in ["", "N/A"] or
                    company_dict.get("employee_size", "N/A") in ["", "N/A"] or
                    company_dict.get("country", "N/A") in ["", "N/A"] or
                    company_dict.get("headquarter", "N/A") in ["", "N/A"] or
                    company_dict.get("hq_number", "N/A") in ["", "N/A"] or
                    company_dict.get("ai_confidence_score", "N/A") in ["", "N/A"] or
                    company_dict.get("logo", "") == "" or
                    company_dict.get("linkedin_url", "") == ""
                )

                if needs_enrichment:
                    app.logger.info(f"Enriching company: {company_dict.get('company_name', company_dict.get('domain'))}")
                    # Use _lookup_company_details, which will use external APIs if data is not in cache/master
                    enriched_data = _lookup_company_details(
                        domain=company_dict.get("domain", ""),
                        company_name_hint=company_dict.get("company_name", "")
                    )

                    if enriched_data:
                        # Update the master_companies table with the newly enriched data
                        # Note: update_master_companies will merge fields, not overwrite
                        update_master_companies(enriched_data)
                        app.logger.info(f"Enriched and updated master_companies for {company_dict.get('company_name', company_dict.get('domain'))}")
                    else:
                        app.logger.info(f"No further enrichment found for {company_dict.get('company_name', company_dict.get('domain'))}")
                else:
                    app.logger.debug(f"Company {company_dict.get('company_name', company_dict.get('domain'))} already enriched.")

                time.sleep(1) # Small delay to respect API limits if _lookup_company_details makes external calls

        except Exception as e:
            app.logger.error(f"Error in background data enrichment job: {e}", exc_info=True)
            send_error_notification("Background Enrichment Error", f"Error in background enrichment job: {e}\nTraceback: {traceback.format_exc()}")
        finally:
            if conn:
                conn.close()

        time.sleep(6 * 3600) # Run every 6 hours

# --- App Startup ---
if __name__ == "__main__":
    # Ensure data_exports directory exists on startup
    data_exports_dir = "data_exports"
    if not os.path.exists(data_exports_dir):
        os.makedirs(data_exports_dir)
        app.logger.info(f"Created data directory: {data_exports_dir}")

    # Start background jobs in separate threads
    threading.Thread(target=refresh_cache_job, daemon=True).start()
    app.logger.info("Background cache refresh job started.")

    threading.Thread(target=enrich_existing_data_job, daemon=True).start()
    app.logger.info("Background data enrichment job started.")

    host = '0.0.0.0'
    port = 5000
    app.run(debug=True, host=host, port=port)
    print(f"Flask app running on http://{host}:{port}/")
