"""
Fix for kafka-python compatibility with Python 3.13.

The problem: kafka-python bundles its own version of six in kafka.vendor.six,
but this bundled version doesn't work properly with Python 3.13.

The solution: Intercept the import of kafka.vendor.six and replace it with
the system-installed six library. We do this by pre-registering our patched
version in sys.modules BEFORE kafka tries to import its own bundled version.

IMPORTANT: This module must be imported BEFORE importing anything from kafka.
"""

import sys
import six
from types import ModuleType


def patch_kafka_vendor_six():
    """
    Patch kafka.vendor.six to use the system-installed six library.
    
    Strategy:
    1. Pre-register kafka.vendor.six in sys.modules pointing to system six
    2. Pre-register kafka.vendor.six.moves in sys.modules
    3. Do NOT pre-register kafka or kafka.vendor - let them import normally
    
    When kafka tries to import kafka.vendor.six, Python will find our
    pre-registered version in sys.modules and use it instead of loading
    kafka's broken bundled version.
    """
    
    print("Applying Python 3.13 compatibility patch for kafka-python...")
    
    # CRITICAL: Do NOT create kafka or kafka.vendor modules in sys.modules!
    # Let Python import them normally from the real kafka-python package.
    
    # Only pre-register kafka.vendor.six and kafka.vendor.six.moves
    # These will be found by Python when kafka tries to import its bundled six
    
    # Use the system-installed six library for kafka.vendor.six
    sys.modules['kafka.vendor.six'] = six
    
    # Also make sure kafka.vendor.six.moves is available
    # six.moves is a special lazy-loading module
    if hasattr(six, 'moves'):
        sys.modules['kafka.vendor.six.moves'] = six.moves
        print(f"  ✓ Using system six.moves from: {type(six.moves).__name__}")
    else:
        # If six.moves doesn't exist (unlikely), create a minimal version
        print("  ⚠ six.moves not found, creating minimal replacement")
        
        class _MinimalMoves:
            range = range
            map = map
            filter = filter
            zip = zip
            
            @property
            def reduce(self):
                from functools import reduce
                return reduce
            
            @property
            def queue(self):
                import queue
                return queue
            
            @property
            def configparser(self):
                import configparser
                return configparser
            
            @property
            def socketserver(self):
                import socketserver
                return socketserver
            
            @property
            def _thread(self):
                import _thread
                return _thread
            
            @property
            def _dummy_thread(self):
                import _dummy_thread
                return _dummy_thread
            
            @property
            def http_client(self):
                import http.client
                return http.client
            
            @property
            def html_entities(self):
                import html.entities
                return html.entities
            
            @property
            def html_parser(self):
                import html.parser
                return html.parser
            
            @property
            def urllib_parse(self):
                import urllib.parse
                return urllib.parse
            
            @property
            def urllib_error(self):
                import urllib.error
                return urllib.error
            
            @property
            def urllib_request(self):
                import urllib.request
                return urllib.request
        
        minimal_moves = _MinimalMoves()
        sys.modules['kafka.vendor.six.moves'] = minimal_moves
        
        # Also attach to six.moves if needed
        if not hasattr(six, 'moves'):
            six.moves = minimal_moves
    
    print("✓ kafka.vendor.six patch applied (will use system six library)")
    print("\nPatch is ready. Now you can import from kafka.")
    
    # Note: We don't test the import here because that would trigger
    # the import of kafka, which might fail if our patch isn't complete.
    # Let the user's code test the import.


# Apply the patch when this module is imported
patch_kafka_vendor_six()
