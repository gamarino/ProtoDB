"""Task 7 fix round fixture: the switches the import-time code of ``reg7_importcommit`` and
``reg7_peek`` reads.  With ``db`` (or ``tr``) left ``None`` (as in a separate installer process)
importing those modules does nothing but declare them."""
import threading

importing = threading.Event()
go = threading.Event()
db = None
tr = None
peeked = []
#: ``reg7_peekcommit``: the transaction being committed by another thread, and what reading it gave.
committing = None
seen = []
#: ``reg7_selfinstall``: a transaction its import-time code installs it into, and what that raised.
selfinstall = None
selfinstall_errors = []
#: ``reg7_halfcommit`` and ``reg7_halfresolve``: the database their import-time code uses, and what it saw.
half = None
half_seen = []
