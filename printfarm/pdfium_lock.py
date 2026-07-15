from __future__ import annotations

import threading


# PDFium is a process-wide native library and is not safe to call concurrently.
# Keep one lock shared by background rendering and GUI-side PDF inspection.
PDFIUM_LOCK = threading.Lock()
