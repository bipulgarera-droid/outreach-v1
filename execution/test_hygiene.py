import sys
import os

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from execution.verify_email import check_email
from execution.enrich_contacts import _is_valid_email

def test_hygiene():
    print("--- Testing Email Verification hygiene ---")
    
    # Test trailing dot stripping
    # If it didn't strip the dot, it might still pass regex but it's good to see it hits SMTP
    status, reason = check_email("nonexistent-test-12345@gmail.com.")
    print(f"nonexistent-test-12345@gmail.com. -> {status} ({reason})")
    # If it's smtp_reject (550), it means it reached Google's servers as "nonexistent-test-12345@gmail.com"
    # If it was still "com.", it might have failed MX lookup or been rejected earlier.
    assert reason in ["smtp_reject", "smtp_ok", "risky"], f"Expected it to reach SMTP, got {status}/{reason}"

    # Test mail.com rejection
    status, reason = check_email("bad@mail.com")
    print(f"bad@mail.com -> {status} ({reason})")
    assert status == "invalid" and reason == "disposable_domain", f"Expected invalid/disposable for mail.com, got {status}/{reason}"

    # Test .com.mx preservation
    status, reason = check_email("test@ambulante.com.mx")
    print(f"test@ambulante.com.mx -> {status} ({reason})")
    assert "ambulante.com.mx" in status or reason in ["smtp_reject", "smtp_ok", "risky", "no_mx"], "Should preserve .com.mx"

    # Test gmail.com acceptance (not blacklisted)
    status, reason = check_email("good@gmail.com")
    print(f"good@gmail.com -> {status} ({reason})")
    assert status in ["valid", "risky"], "gmail.com should be allowed"

    print("\n--- Testing Enrichment Filter hygiene ---")
    
    # Test valid filter
    assert _is_valid_email("good@gmail.com") == True, "gmail.com should be valid in filter"
    assert _is_valid_email("bad@mail.com") == False, "mail.com should be invalid in filter"
    assert _is_valid_email("test@example.com") == False, "example.com should be invalid in filter"

    print("\n✅ All hygiene tests passed!")

if __name__ == "__main__":
    try:
        test_hygiene()
    except AssertionError as e:
        print(f"\n❌ Test FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)
