"""File parser for extracting text from uploaded context files."""

import csv
import io
import logging
from typing import Optional

logger = logging.getLogger(__name__)

MAX_CONTENT_LENGTH = 8000  # ~2000 tokens


def parse_uploaded_file(filename: str, content_bytes: bytes) -> Optional[str]:
    """Parse an uploaded file and return extracted text content.

    Supports CSV, PDF, MD, TXT, and JSON files.
    Returns None if the file cannot be parsed.
    """
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

    try:
        if ext == "csv":
            return _parse_csv(content_bytes)
        elif ext == "pdf":
            return _parse_pdf(content_bytes)
        elif ext in ("md", "txt", "json"):
            return _parse_text(content_bytes)
        else:
            logger.warning(f"Unsupported file type: .{ext}")
            return None
    except Exception as e:
        logger.error(f"Error parsing file '{filename}': {e}")
        return None


def _parse_csv(content_bytes: bytes) -> Optional[str]:
    """Extract headers and first 5 sample rows from CSV."""
    text = content_bytes.decode("utf-8", errors="replace")
    reader = csv.reader(io.StringIO(text))
    rows = []
    for i, row in enumerate(reader):
        if i >= 6:  # header + 5 rows
            break
        rows.append(row)

    if not rows:
        return None

    header = rows[0]
    result = f"CSV Headers: {', '.join(header)}\n"
    if len(rows) > 1:
        result += "Sample Rows:\n"
        for row in rows[1:]:
            result += f"  {', '.join(row)}\n"

    return result[:MAX_CONTENT_LENGTH]


def _parse_pdf(content_bytes: bytes) -> Optional[str]:
    """Extract text from first 10 pages of PDF using pymupdf."""
    try:
        import fitz  # pymupdf
    except ImportError:
        logger.warning("pymupdf not installed; cannot parse PDF files")
        return None

    doc = fitz.open(stream=content_bytes, filetype="pdf")
    text_parts = []
    for page_num in range(min(10, len(doc))):
        page = doc[page_num]
        text_parts.append(page.get_text())
    doc.close()

    text = "\n".join(text_parts).strip()
    if not text:
        return None

    return text[:MAX_CONTENT_LENGTH]


def _parse_text(content_bytes: bytes) -> Optional[str]:
    """Decode raw text content."""
    text = content_bytes.decode("utf-8", errors="replace").strip()
    if not text:
        return None
    return text[:MAX_CONTENT_LENGTH]
