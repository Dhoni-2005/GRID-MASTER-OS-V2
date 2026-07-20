"""
interface/auth.py — Grid Master OS Phase 5/6
Authentication hook. Phase 5 returned True unconditionally.
Phase 6 delegates to security.auth.verify().
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from security.auth import verify as _verify
    _SECURITY_AVAILABLE = True
except ImportError:
    _SECURITY_AVAILABLE = False


def check_auth(request=None) -> bool:
    """
    Return True if the request is authenticated.
    Falls back to True if security package is absent.
    Phase 6: delegates to security.auth.verify().
    """
    if not _SECURITY_AVAILABLE or request is None:
        return True
    return _verify(request) is not None
