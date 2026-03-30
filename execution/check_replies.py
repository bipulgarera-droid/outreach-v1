import imaplib
import email
import os
import re
import socket
import ssl
from datetime import datetime, timedelta
import logging
from email.header import decode_header

# Constants
IMAP_HOST = "imap.gmail.com"
IMAP_PORT = 993

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def _load_accounts_from_env() -> list[dict]:
    """Load Gmail accounts from .env using GMAIL_N_EMAIL format."""
    accounts = []
    for i in range(1, 25): # Increased range
        acct_email = os.getenv(f"GMAIL_{i}_EMAIL")
        acct_password = os.getenv(f"GMAIL_{i}_PASSWORD")
        if not acct_email or not acct_password:
            continue
        accounts.append({"email": acct_email.strip(), "app_password": acct_password.strip()})
    return accounts

def _decode_header_value(raw):
    """Safely decode an email header value."""
    if raw is None:
        return ""
    try:
        decoded_parts = decode_header(raw)
        result = ""
        for part, charset in decoded_parts:
            if isinstance(part, bytes):
                result += part.decode(charset or "utf-8", errors="replace")
            else:
                result += part
        return result
    except:
        return str(raw)

def _extract_sender_email(from_header: str) -> str:
    """Extract just the email address from a From: header, handling encoded strings."""
    decoded_from = _decode_header_value(from_header)
    
    # Use regex for better precision if possible
    emails = re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', decoded_from)
    if emails:
        return emails[0].strip().lower()
        
    if "<" in decoded_from and ">" in decoded_from:
        return decoded_from.split("<")[1].split(">")[0].strip().lower()
    return decoded_from.strip().lower()

def _get_imap_connection(acct_email: str, acct_password: str) -> imaplib.IMAP4_SSL:
    """Helper to establish a fresh IMAP connection with timeout."""
    mail = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT, timeout=30)
    mail.login(acct_email, acct_password)
    
    # Try [Gmail]/All Mail if available, it's more robust
    try:
        mail.select('"[Gmail]/All Mail"')
    except:
        mail.select("INBOX")
    return mail

def _extract_body(msg) -> str:
    """Recursively extract the plain text body from an email message."""
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            content_disposition = str(part.get("Content-Disposition"))
            if content_type == "text/plain" and "attachment" not in content_disposition:
                try:
                    payload = part.get_payload(decode=True)
                    if payload:
                        body += payload.decode('utf-8', errors='ignore')
                except: pass
    else:
        try:
            payload = msg.get_payload(decode=True)
            if payload:
                body = payload.decode('utf-8', errors='ignore')
        except: pass
    return body.strip()

def is_bounce(from_addr: str, subject: str) -> bool:
    """Determine if a message is a NDR (Non-Delivery Receipt)."""
    from_addr = (from_addr or "").lower()
    subject = (subject or "").lower()
    
    bounce_senders = ['mailer-daemon', 'postmaster', 'no-reply@accounts.google.com']
    if any(s in from_addr for s in bounce_senders):
        return True
        
    bounce_subjects = ['undeliverable', 'delivery status notification', 'failure', 'returned mail']
    if any(s in subject for s in bounce_subjects):
        return True
        
    return False

def analyze_sentiment(text: str):
    """Simple sentiment analysis (Placeholder for more robust LLM check)."""
    text = text.lower()
    positive_keywords = ['interested', 'let\'s talk', 'call', 'meeting', 'demo', 'tell me more', 'sounds good']
    negative_keywords = ['not interested', 'remove', 'unsubscribe', 'stop', 'wrong person', 'fuck off']
    
    if any(k in text for k in negative_keywords):
        return 'Negative', 0.1
    if any(k in text for k in positive_keywords):
        return 'Positive', 0.9
    return 'Neutral', 0.5

def check_replies_for_account(acct_email: str, acct_password: str, prospect_emails: set, days: int = 7, logger_callback=None) -> tuple[list[dict], list[str]]:
    """Connect and scan for replies/bounces with full-spectrum matching."""
    replied = []
    bounced = []
    mail = None

    def _log(msg):
        logger.info(msg)
        if logger_callback: logger_callback(msg)

    try:
        _log(f"Checking {acct_email}...")
        mail = _get_imap_connection(acct_email, acct_password)

        since_date = (datetime.now() - timedelta(days=days)).strftime("%d-%b-%Y")
        status, message_ids = mail.search(None, f'(SINCE {since_date})')

        if status != "OK" or not message_ids[0]:
            _log(f"[{acct_email}] No recent emails found.")
            mail.logout()
            return [], []

        ids = message_ids[0].split()
        _log(f"[{acct_email}] Scanning {len(ids)} emails from last {days} days...")

        for msg_id in ids:
            try:
                # Fetch full message for robust body scanning
                status, msg_data = mail.fetch(msg_id, "(RFC822 X-GM-THRID X-GM-MSGID)")
                if status != "OK" or not msg_data: continue

                raw = msg_data[0][1]
                msg = email.message_from_bytes(raw)
                
                from_hdr = _decode_header_value(msg.get("From", ""))
                subject_hdr = _decode_header_value(msg.get("Subject", ""))
                sender = _extract_sender_email(from_hdr)
                body_text = _extract_body(msg)
                body_lower = body_text.lower()
                
                # Metadata
                metadata_raw = msg_data[0][0].decode()
                thread_id = re.search(r"X-GM-THRID (\d+)", metadata_raw)
                thread_id = thread_id.group(1) if thread_id else None
                message_id_gmail = re.search(r"X-GM-MSGID (\d+)", metadata_raw)
                message_id_gmail = message_id_gmail.group(1) if message_id_gmail else None

                is_b = is_bounce(from_hdr, subject_hdr)
                found_prospect = None
                
                # --- MATCHING LOGIC ---
                # 1. Direct Sender Match
                if sender in prospect_emails:
                    found_prospect = sender
                    _log(f"  {'❌ Bounce' if is_b else '✅ Reply'} Match (Sender): {sender}")
                
                # 2. Redundant Body Match (The Marcelo Protection)
                if not found_prospect:
                    for p_email in prospect_emails:
                        if p_email in body_lower:
                            found_prospect = p_email
                            _log(f"  {'❌ Bounce' if is_b else '✅ Reply'} Match (Body): {found_prospect}")
                            break
                
                if found_prospect:
                    if is_b:
                        bounced.append(found_prospect)
                    else:
                        sentiment, score = analyze_sentiment(body_text)
                        replied.append({
                            'email': found_prospect,
                            'subject': subject_hdr,
                            'body': body_text,
                            'sentiment': sentiment,
                            'sentiment_score': score,
                            'thread_id': thread_id,
                            'message_id': message_id_gmail,
                            'recipient_email': acct_email
                        })
            except Exception as e:
                logger.error(f"Error processing message {msg_id}: {e}")

        mail.logout()
    except Exception as e:
        _log(f"FAILED for {acct_email}: {e}")
        if mail:
            try: mail.logout()
            except: pass

    return replied, bounced

def check_replies():
    """Main entry point for reply and bounce synchronization."""
    from dotenv import load_dotenv
    load_dotenv()
    
    # Use Service Role Key to bypass RLS for automated jobs
    url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
    
    if not url or not key:
        return {"error": "Supabase credentials missing"}
    
    from supabase import create_client
    supabase = create_client(url, key)
    
    # Get all contacts to monitor (status-agnostic for safety)
    res = supabase.table('contacts').select('id, email').not_.is_('email', 'null').execute()
    contact_map = {r['email'].strip().lower(): r['id'] for r in res.data}
    prospect_emails = set(contact_map.keys())
    
    accounts = _load_accounts_from_env()
    all_replies = []
    all_bounces = set()
    
    for acct in accounts:
        replies, bounces = check_replies_for_account(acct['email'], acct['app_password'], prospect_emails)
        all_replies.extend(replies)
        all_bounces.update(bounces)
    
    # Process Bounces
    for bounce_email in all_bounces:
        cid = contact_map.get(bounce_email.lower())
        if cid:
            supabase.table('contacts').update({'status': 'bounced'}).eq('id', cid).execute()
            logger.info(f"Updated status to BOUNCED for {bounce_email}")

    # Process Replies
    for reply in all_replies:
        cid = contact_map.get(reply['email'].lower())
        if cid:
            # 1. Update contact status
            supabase.table('contacts').update({'status': 'replied'}).eq('id', cid).execute()
            
            # 2. Record reply (deduplicated by message_id)
            if reply['message_id']:
                existing = supabase.table('replies').select('id').eq('message_id', reply['message_id']).execute()
                if existing.data: continue
                
            supabase.table('replies').insert({
                'contact_id': cid,
                'sender_email': reply['email'],
                'recipient_email': reply['recipient_email'],
                'subject': reply['subject'],
                'body': reply['body'],
                'sentiment': reply['sentiment'],
                'sentiment_score': reply['sentiment_score'],
                'message_id': reply['message_id'],
                'thread_id': reply['thread_id']
            }).execute()
            logger.info(f"Recorded REPLY from {reply['email']}")

    return {
        "status": "completed",
        "replies_found": len(all_replies),
        "bounces_found": len(all_bounces)
    }

if __name__ == "__main__":
    check_replies()
