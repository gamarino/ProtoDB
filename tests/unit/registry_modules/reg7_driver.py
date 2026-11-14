"""Task 7 fix round fixture: the switches the import-time code of ``reg7_importcommit`` and
``reg7_peek`` reads.  With ``db`` (or ``tr``) left ``None`` (as in a separate installer process)
importing those modules does nothing but declare them."""
import threading

importing = threading.Event()
go = threading.Event()
db = None
tr = None
peeked = []
