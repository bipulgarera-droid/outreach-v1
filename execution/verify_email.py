import re
import smtplib
import time
import sys
import os
import socket
import uuid

# Ensure the local vendor folder is in the path for dnspython
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.vendor')))
import dns.resolver

EMAIL_REGEX = re.compile(r"[^@]+@[^@]+\.[^@]+")

# Using a standard set, can be expanded later
DISPOSABLE_DOMAINS = {"mailinator.com", "10minutemail.com", "guerrillamail.com", "yopmail.com", "tempmail.com", "mail.com"}
ROLE_BASED_PREFIXES = {"info", "support", "admin", "sales", "contact", "marketing", "billing", "hello"}

def check_email(email: str) -> tuple[str, str]:
    """
    Verifies an email using Regex, Domain checks, MX lookup, and SMTP probes.
    Returns:
        (status, reason)
        status can be: "valid", "risky", "invalid"
    """
    # Broad trailing punctuation strip (preserving middle dots like .com.mx)
    email = str(email).strip().rstrip('.,;:)!% ]').strip().lower()

    if not EMAIL_REGEX.match(email):
        return "invalid", "bad_syntax"

    try:
        local, domain = email.split('@')
    except ValueError:
        return "invalid", "bad_syntax"

    if domain in DISPOSABLE_DOMAINS:
        return "invalid", "disposable_domain"
    
    if local in ROLE_BASED_PREFIXES:
        # We allow role-based emails (info@, contact@) as long as the domain isn't a catch-all.
        # This will be refined in the SMTP check below.
        pass

    # MX Record Lookup
    try:
        records = dns.resolver.resolve(domain, 'MX')
        # Sort MX records by preference (lowest first)
        records = sorted(records, key=lambda r: r.preference)
        mx_record = str(records[0].exchange).rstrip('.')
    except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.resolver.NoNameservers, Exception):
        return "invalid", "no_mx"

    # Check specific email
    def smtp_check(email_to_check):
        try:
            server = smtplib.SMTP(timeout=10)
            server.connect(mx_record)
            server.helo("example.com")
            server.mail("verifier@example.com")
            code, msg = server.rcpt(email_to_check)
            server.quit()
            return code, msg.decode('utf-8', 'ignore') if msg else ""
        except socket.gaierror:
            # DNS lookup for the MX host failed (e.g. clevelandseoguy)
            return -1, "dns_gaierror"
        except socket.timeout:
            return None, "timeout"
        except Exception as e:
            return None, str(e)

    # Test 1: Catch-all Check (Liar Detector)
    import uuid
    code_stupid, msg_stupid = smtp_check(f"probe-{uuid.uuid4().hex[:8]}@{domain}")
    
    # Test 2: Real Email
    code_real, msg_real = smtp_check(email)
    
    # LOGIC:
    # 1. Catch-all (Liar) -> Always ALLOW (VALID). Because redchillies (liar) is good.
    if code_stupid == 250:
        return "valid", "domain_catch_all"

    # 2. Honest Server (it rejected the stupid email)
    if code_real == 250:
        return "valid", "smtp_ok"
    elif code_real == 550:
        msg_lower = msg_real.lower()
        if any(keyword in msg_lower for keyword in ["no such user", "does not exist", "nosuchuser", "recipient address rejected", "user unknown", "invalid recipient"]):
            # HARD REJECTION: This is definitely one of the 'assholes'
            return "invalid", "hard_reject_nosuchuser"
        else:
            # SOFT REJECTION: Likely throttling or sender blacklist
            return "risky", "smtp_reject_550_safe_shield"
    elif code_real == -1 and msg_real == "dns_gaierror":
        # DNS Error for MX Host (e.g. clevelandseoguy)
        return "invalid", "dns_gaierror"
    elif code_real is None:
        # TIMEOUT: We now treat ALL timeouts as risky to preserve leads on Yandex/Zoho/etc.
        return "risky", f"smtp_timeout_{msg_real}"
    else:
        # Default to risky for temporary connection issues
        return "risky", f"smtp_error_{code_real}_{msg_real[:50]}"

if __name__ == "__main__":
    # Simple test cases if executed directly
    print("Testing doesnotexist123@google.com:", check_email("doesnotexist123@google.com"))
    print("Testing info@google.com:", check_email("info@google.com"))
