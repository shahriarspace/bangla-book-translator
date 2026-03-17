"""
PDF-to-Book: Convert and translate PDF books using AI.

Converts PDF pages to images, sends each page for OCR and translation,
and combines the results into markdown files ready for Astro or any static site.

Supports two backends:
  - gh-copilot: GitHub Copilot CLI (recommended — supports Claude & GPT models)
  - opencode:   OpenCode CLI (legacy — GPT-4o only, Claude has empty-stdout bug)
"""

import argparse
import difflib
import json
import logging
import os
import shutil
import subprocess
import sys
import time
import unicodedata
from datetime import datetime, timedelta
from pathlib import Path

# Default backend
DEFAULT_BACKEND = "gh-copilot"


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------


def setup_logging(
    output_dir: str | None = None, verbose: bool = False
) -> logging.Logger:
    """Configure logging with both console and file handlers.

    Console gets a compact format; log file gets full timestamps.
    """
    logger = logging.getLogger("pdf_to_book")
    logger.setLevel(logging.DEBUG)

    # Console handler — compact, colorful-ish
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    console.setFormatter(
        logging.Formatter("%(asctime)s | %(message)s", datefmt="%H:%M:%S")
    )
    logger.addHandler(console)

    # File handler — full detail (created when output_dir is known)
    if output_dir:
        log_dir = Path(output_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "pipeline.log"
        fh = logging.FileHandler(str(log_file), encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(
            logging.Formatter(
                "%(asctime)s [%(levelname)-5s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
            )
        )
        logger.addHandler(fh)
        logger.info(f"Log file: {log_file}")

    return logger


# Global logger — initialized lazily
log = logging.getLogger("pdf_to_book")


# ---------------------------------------------------------------------------
# Progress bar helper
# ---------------------------------------------------------------------------


def _progress_bar(
    current: int, total: int, width: int = 30, elapsed: float = 0, label: str = ""
) -> str:
    """Render an ASCII progress bar with ETA.

    Example: [==============>.............] 52% (36/69) ETA 00:42:10
    """
    pct = current / total if total else 0
    filled = int(width * pct)
    arrow = ">" if filled < width else ""
    bar = "=" * filled + arrow + "." * (width - filled - len(arrow))

    # ETA calculation
    eta_str = ""
    if current > 0 and elapsed > 0:
        rate = elapsed / current
        remaining = rate * (total - current)
        eta = timedelta(seconds=int(remaining))
        elapsed_td = timedelta(seconds=int(elapsed))
        eta_str = f" ETA {eta} (elapsed {elapsed_td})"

    return f"[{bar}] {pct:>5.1%} ({current}/{total}){eta_str} {label}"


def _format_duration(seconds: float) -> str:
    """Format seconds into human-readable duration."""
    td = timedelta(seconds=int(seconds))
    return str(td)


# ---------------------------------------------------------------------------
# GitHub Copilot CLI (gh copilot) backend
# ---------------------------------------------------------------------------

# gh CLI search paths (not always on default PATH)
_GH_SEARCH_PATHS = [
    Path("C:/Program Files/GitHub CLI/gh.exe"),
    Path("C:/Program Files (x86)/GitHub CLI/gh.exe"),
    Path.home() / "AppData" / "Local" / "GitHub CLI" / "gh.exe",
]


def find_gh_cli() -> str:
    """Find the GitHub CLI executable.

    Checks:
    1. GH_CLI env var
    2. 'gh' on PATH
    3. Known install locations
    """
    env_path = os.environ.get("GH_CLI")
    if env_path and os.path.isfile(env_path):
        return env_path

    on_path = shutil.which("gh")
    if on_path:
        return on_path

    for p in _GH_SEARCH_PATHS:
        if p.is_file():
            return str(p)

    print("ERROR: GitHub CLI (gh) not found.")
    print("Looked in:")
    print("  - PATH")
    for p in _GH_SEARCH_PATHS:
        print(f"  - {p}")
    print()
    print("Install: winget install --id GitHub.cli")
    print("Then: gh auth login")
    sys.exit(1)


def _map_model_for_gh(model: str) -> str:
    """Map model names to gh copilot --model values.

    Accepts either:
      - Full OpenCode-style names like 'github-copilot/claude-sonnet-4'
      - Short names like 'claude-sonnet-4.6' or 'gpt-4.1'

    Returns the short model name that gh copilot expects.
    """
    # Strip provider prefix if present
    if "/" in model:
        model = model.split("/", 1)[1]

    # Map legacy model names to current ones
    _LEGACY_MAP = {
        "gpt-4o": "gpt-4.1",
        "claude-sonnet-4": "claude-sonnet-4.6",
    }
    return _LEGACY_MAP.get(model, model)


# ---------------------------------------------------------------------------
# OpenCode CLI detection (legacy backend)
# ---------------------------------------------------------------------------

# Common install locations for OpenCode CLI
_OPENCODE_SEARCH_PATHS = [
    # Windows AppData install
    Path.home() / "AppData" / "Local" / "OpenCode" / "opencode-cli.exe",
    # Linux/macOS common locations
    Path.home() / ".local" / "bin" / "opencode",
    Path("/usr/local/bin/opencode"),
    Path("/usr/bin/opencode"),
]


def find_opencode_cli() -> str:
    """Find the OpenCode CLI executable.

    Checks:
    1. OPENCODE_CLI env var
    2. 'opencode' on PATH
    3. Known install locations
    """
    # 1. Environment variable override
    env_path = os.environ.get("OPENCODE_CLI")
    if env_path and os.path.isfile(env_path):
        return env_path

    # 2. Check PATH
    on_path = shutil.which("opencode") or shutil.which("opencode-cli")
    if on_path:
        return on_path

    # 3. Check known install locations
    for p in _OPENCODE_SEARCH_PATHS:
        if p.is_file():
            return str(p)

    print("ERROR: OpenCode CLI not found.")
    print("Looked in:")
    print("  - PATH (opencode / opencode-cli)")
    for p in _OPENCODE_SEARCH_PATHS:
        print(f"  - {p}")
    print()
    print("Fix: set the OPENCODE_CLI environment variable to the full path:")
    print('  set OPENCODE_CLI="C:\\path\\to\\opencode-cli.exe"')
    sys.exit(1)


def find_opencode_server() -> str | None:
    """Try to find a running OpenCode server by checking common ports.

    Returns the server URL if found, None otherwise.
    """
    import socket

    # Try to find the port by checking the OpenCode process's listening port
    try:
        result = subprocess.run(
            ["netstat", "-ano"], capture_output=True, text=True, timeout=10
        )
        # Find PIDs of opencode-cli.exe processes
        pid_result = subprocess.run(
            ["tasklist", "/FI", "IMAGENAME eq opencode-cli.exe", "/FO", "CSV", "/NH"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        pids = set()
        for line in pid_result.stdout.strip().split("\n"):
            parts = line.strip().strip('"').split('","')
            if len(parts) >= 2:
                try:
                    pids.add(parts[1])
                except (ValueError, IndexError):
                    pass

        # Find listening ports for those PIDs
        for line in result.stdout.split("\n"):
            if "LISTENING" in line:
                parts = line.split()
                if len(parts) >= 5 and parts[4] in pids:
                    addr = parts[1]
                    if "127.0.0.1:" in addr:
                        port = addr.split(":")[-1]
                        url = f"http://localhost:{port}"
                        # Quick check if it responds
                        try:
                            sock = socket.create_connection(
                                ("localhost", int(port)), timeout=2
                            )
                            sock.close()
                            return url
                        except (socket.error, OSError):
                            continue
    except Exception:
        pass

    return None


# ---------------------------------------------------------------------------
# PDF to Images
# ---------------------------------------------------------------------------


def pdf_to_images(
    pdf_path: str,
    output_dir: str,
    dpi: int = 300,
    fmt: str = "png",
    start_page: int = 1,
    end_page: int | None = None,
    skip_pages: list[int] | None = None,
) -> list[Path]:
    """Convert each page of a PDF into an image file.

    Also extracts embedded text (if present) and saves to embedded_text/ dir.
    This allows the OCR step to skip AI calls for pages with existing text.

    Returns a sorted list of paths to the generated images.
    """
    try:
        import fitz  # PyMuPDF
    except ImportError:
        log.error("PyMuPDF is not installed. Run: pip install PyMuPDF")
        sys.exit(1)

    images_dir = Path(output_dir) / "pages"
    images_dir.mkdir(parents=True, exist_ok=True)

    # Also create dir for embedded text extraction
    embedded_dir = Path(output_dir) / "embedded_text"
    embedded_dir.mkdir(parents=True, exist_ok=True)

    doc = fitz.open(pdf_path)
    total_pages = len(doc)
    end = end_page if end_page else total_pages
    skip = set(skip_pages or [])

    log.info(f"PDF has {total_pages} pages. Processing pages {start_page}-{end}.")

    generated: list[Path] = []
    pages_with_text = 0
    zoom = dpi / 72  # 72 is the default PDF DPI
    matrix = fitz.Matrix(zoom, zoom)

    for page_num in range(start_page - 1, min(end, total_pages)):
        page_display = page_num + 1
        if page_display in skip:
            log.debug(f"  Skipping page {page_display}")
            continue

        page = doc[page_num]
        pix = page.get_pixmap(matrix=matrix)

        image_path = images_dir / f"page_{page_display:04d}.{fmt}"
        pix.save(str(image_path))
        generated.append(image_path)

        # Extract embedded text if available
        embedded_text = str(page.get_text("text")).strip()
        txt_path = embedded_dir / f"page_{page_display:04d}.txt"
        if embedded_text:
            txt_path.write_text(embedded_text + "\n", encoding="utf-8")
            pages_with_text += 1
            log.info(
                f"  Extracted page {page_display}/{end} (has embedded text: {len(embedded_text)} chars)"
            )
        else:
            # Write empty file to signal "no embedded text"
            txt_path.write_text("", encoding="utf-8")
            log.info(f"  Extracted page {page_display}/{end} (image only)")

    doc.close()
    log.info(f"Extracted {len(generated)} page images to {images_dir}")
    if pages_with_text > 0:
        log.info(
            f"  {pages_with_text}/{len(generated)} pages have embedded text "
            f"(will skip AI OCR for these)"
        )
    return sorted(generated)


# ---------------------------------------------------------------------------
# OpenCode run helper
# ---------------------------------------------------------------------------

import re

# ANSI escape code pattern
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _run_gh_copilot(
    gh_cli: str,
    prompt: str,
    model: str | None = None,
    image_path: Path | None = None,
    timeout: int = 300,
) -> tuple[int, str, str]:
    """Run a single `gh copilot -p` command and return (returncode, stdout, stderr).

    Uses the GitHub Copilot CLI in non-interactive mode with:
    - -s (silent): output only the agent response, no stats
    - --no-ask-user: fully autonomous, no interactive questions
    - --allow-all: allow file read access for image OCR

    For OCR: the prompt tells the model to read the image file at a given path.
    For translation: the prompt contains the text directly.
    """
    cmd = [gh_cli, "copilot", "-s", "--no-ask-user", "--allow-all"]
    if model:
        mapped = _map_model_for_gh(model)
        cmd.extend(["--model", mapped])

    # If we have an image, inject the file path into the prompt
    if image_path:
        abs_path = str(image_path.resolve()).replace("\\", "/")
        full_prompt = (
            f"Read the image file at {abs_path} and perform the following task. "
            f"{prompt}"
        )
    else:
        full_prompt = prompt

    cmd.extend(["-p", full_prompt])

    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        env={
            **os.environ,
            "PATH": os.environ.get("PATH", "")
            + os.pathsep
            + "C:\\Program Files\\GitHub CLI",
        },
    )
    stdout = result.stdout.decode("utf-8", errors="replace").strip()
    stderr = _ANSI_RE.sub("", result.stderr.decode("utf-8", errors="replace").strip())
    return result.returncode, stdout, stderr


def _run_opencode(
    opencode_cli: str,
    prompt: str,
    attach_url: str | None = None,
    model: str | None = None,
    file_path: Path | None = None,
    timeout: int = 300,
) -> tuple[int, str, str]:
    """Run a single `opencode run` command and return (returncode, stdout, stderr).

    Handles:
    - UTF-8 decoding for Bengali text
    - ANSI escape code stripping
    - The --file/-- separator quirk
    """
    cmd = [opencode_cli, "run"]
    if attach_url:
        cmd.extend(["--attach", attach_url])
    if model:
        cmd.extend(["-m", model])
    if file_path:
        cmd.extend(["--file", str(file_path.resolve()), "--", prompt])
    else:
        cmd.append(prompt)

    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
    )
    stdout = result.stdout.decode("utf-8", errors="replace").strip()
    stderr = _ANSI_RE.sub("", result.stderr.decode("utf-8", errors="replace").strip())
    return result.returncode, stdout, stderr


def _run_ai(
    backend: str,
    prompt: str,
    model: str | None = None,
    image_path: Path | None = None,
    timeout: int = 300,
    # OpenCode-specific
    opencode_cli: str | None = None,
    attach_url: str | None = None,
    # gh copilot-specific
    gh_cli: str | None = None,
) -> tuple[int, str, str]:
    """Unified dispatch: run a prompt through the configured AI backend."""
    if backend == "gh-copilot":
        return _run_gh_copilot(
            gh_cli or find_gh_cli(),
            prompt,
            model=model,
            image_path=image_path,
            timeout=timeout,
        )
    elif backend == "opencode":
        return _run_opencode(
            opencode_cli or find_opencode_cli(),
            prompt,
            attach_url=attach_url,
            model=model,
            file_path=image_path,
            timeout=timeout,
        )
    else:
        log.error(f"Unknown backend '{backend}'. Use 'gh-copilot' or 'opencode'.")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Pass 1: OCR — extract Bengali text from page images
# ---------------------------------------------------------------------------

# Bengali Unicode range for detection
_BN_CHAR_DETECT = re.compile(r"[\u0980-\u09FF]")

# Minimum number of Bengali characters to consider embedded text usable
_MIN_EMBEDDED_BN_CHARS = 30


def _has_usable_embedded_text(output_dir: str, page_stem: str) -> str | None:
    """Check if a page has usable embedded text from PDF extraction.

    Returns the embedded text if it contains enough Bengali characters
    to be considered a real text page (not just stray characters from
    headers/page numbers). Returns None otherwise.
    """
    embedded_path = Path(output_dir) / "embedded_text" / f"{page_stem}.txt"
    if not embedded_path.exists():
        return None

    text = embedded_path.read_text(encoding="utf-8").strip()
    if not text:
        return None

    # Count Bengali characters
    bn_chars = len(_BN_CHAR_DETECT.findall(text))
    if bn_chars >= _MIN_EMBEDDED_BN_CHARS:
        return text

    return None


_OCR_PROMPT = (
    "You are an expert OCR engine for Bengali (Bangla) script. "
    "Extract ALL the Bengali text from this image exactly as written. "
    "Preserve paragraph breaks with blank lines. "
    "Do NOT translate — output only the original Bengali text. "
    "Do NOT add any commentary, headers, or explanation. "
    "If the page has no readable text (blank page, only images/decorations), "
    "respond with exactly: NO_TEXT_CONTENT"
)


def ocr_page(
    image_path: Path,
    backend: str = DEFAULT_BACKEND,
    ocr_model: str | None = None,
    max_retries: int = 3,
    # Backend-specific
    opencode_cli: str | None = None,
    attach_url: str | None = None,
    gh_cli: str | None = None,
) -> str:
    """Send a page image for OCR and return the extracted Bengali text."""

    for attempt in range(1, max_retries + 1):
        try:
            t0 = time.time()
            rc, stdout, stderr = _run_ai(
                backend,
                _OCR_PROMPT,
                model=ocr_model,
                image_path=image_path,
                timeout=180,
                opencode_cli=opencode_cli,
                attach_url=attach_url,
                gh_cli=gh_cli,
            )
            elapsed = time.time() - t0

            if rc == 0 and stdout:
                chars = len(stdout)
                log.debug(f"    OCR OK in {elapsed:.1f}s ({chars} chars extracted)")
                return _clean_model_output(stdout)
            else:
                log.warning(
                    f"    OCR attempt {attempt}/{max_retries} FAILED ({elapsed:.1f}s): "
                    f"{stderr[:200] or 'empty output'}"
                )
        except subprocess.TimeoutExpired:
            log.warning(f"    OCR attempt {attempt}/{max_retries} TIMED OUT (>180s)")
        except FileNotFoundError as e:
            log.error(f"CLI not found: {e}")
            sys.exit(1)

        if attempt < max_retries:
            wait = attempt * 5
            log.info(f"    Retrying in {wait}s...")
            time.sleep(wait)

    return f"<!-- OCR FAILED for {image_path.name} after {max_retries} attempts -->"


def ocr_all_pages(
    image_paths: list[Path],
    output_dir: str,
    backend: str = DEFAULT_BACKEND,
    ocr_model: str | None = None,
    delay: float = 2,
    max_retries: int = 3,
    # Backend-specific
    opencode_cli: str | None = None,
    attach_url: str | None = None,
    gh_cli: str | None = None,
) -> list[Path]:
    """OCR all page images and save extracted text as .txt files.

    Uses embedded PDF text when available (instant, free), falling back
    to AI-based OCR only for scanned/image-only pages.

    Returns a sorted list of paths to the generated text files.
    Supports resuming (skips pages with existing OCR output).
    """
    ocr_dir = Path(output_dir) / "ocr"
    ocr_dir.mkdir(parents=True, exist_ok=True)

    total = len(image_paths)
    generated: list[Path] = []
    skipped = 0
    failed = 0
    embedded_used = 0
    phase_start = time.time()

    log.info(f"OCR pass: {total} pages to process (model: {ocr_model})")

    for i, img_path in enumerate(image_paths, 1):
        txt_path = ocr_dir / f"{img_path.stem}.txt"

        # Resume support
        if txt_path.exists() and txt_path.stat().st_size > 0:
            skipped += 1
            generated.append(txt_path)
            elapsed = time.time() - phase_start
            log.info(
                _progress_bar(
                    i, total, elapsed=elapsed, label=f"{img_path.name} SKIP (cached)"
                )
            )
            continue

        # Check for usable embedded text (from PDF text layer)
        embedded = _has_usable_embedded_text(output_dir, img_path.stem)
        if embedded:
            embedded_used += 1
            chars = len(embedded)
            txt_path.write_text(embedded + "\n", encoding="utf-8")
            generated.append(txt_path)
            elapsed = time.time() - phase_start
            log.info(
                _progress_bar(
                    i,
                    total,
                    elapsed=elapsed,
                    label=f"{img_path.name} EMBEDDED ({chars} chars, no AI call)",
                )
            )
            continue

        # No embedded text — use AI OCR
        elapsed = time.time() - phase_start
        log.info(
            _progress_bar(
                i - 1, total, elapsed=elapsed, label=f"{img_path.name} OCR..."
            )
        )

        page_start = time.time()
        text = ocr_page(
            img_path,
            backend=backend,
            ocr_model=ocr_model,
            max_retries=max_retries,
            opencode_cli=opencode_cli,
            attach_url=attach_url,
            gh_cli=gh_cli,
        )
        page_elapsed = time.time() - page_start

        if "OCR FAILED" in text:
            failed += 1
            log.error(f"    FAILED: {img_path.name} after {page_elapsed:.1f}s")
        else:
            chars = len(text)
            log.info(f"    OK: {chars} chars in {page_elapsed:.1f}s -> {txt_path.name}")

        txt_path.write_text(text + "\n", encoding="utf-8")
        generated.append(txt_path)

        if i < total:
            time.sleep(delay)

    total_time = time.time() - phase_start
    log.info(f"")
    log.info(f"OCR COMPLETE: {total} pages in {_format_duration(total_time)}")
    log.info(
        f"  AI OCR: {total - skipped - failed - embedded_used} | "
        f"Embedded text: {embedded_used} | Skipped: {skipped} | Failed: {failed}"
    )
    log.info(f"  Output: {ocr_dir}")
    return sorted(generated)


# ---------------------------------------------------------------------------
# Pass 2: Translation — Bengali text to English
# ---------------------------------------------------------------------------


def _build_translation_prompt(source_lang: str, target_lang: str, text: str) -> str:
    """Build a literary translation prompt with the source text embedded."""
    return (
        f"You are an expert literary translator from {source_lang} to {target_lang}. "
        f"Below is {source_lang} text extracted from a novel. "
        f"Translate it into natural, literary {target_lang}. "
        f"Preserve the tone, style, and paragraph structure of the original. "
        f"Use natural English prose — do not be overly literal.\n\n"
        f"Output the result in markdown with two sections:\n"
        f"1. '## Original ({source_lang})' — the original text exactly as given\n"
        f"2. '## Translation ({target_lang})' — your translation\n\n"
        f"Do NOT add any extra commentary, questions, or notes after the translation.\n\n"
        f"--- BEGIN {source_lang.upper()} TEXT ---\n"
        f"{text}\n"
        f"--- END {source_lang.upper()} TEXT ---"
    )


def translate_text(
    text: str,
    source_lang: str,
    target_lang: str,
    backend: str = DEFAULT_BACKEND,
    translate_model: str | None = None,
    max_retries: int = 3,
    # Backend-specific
    opencode_cli: str | None = None,
    attach_url: str | None = None,
    gh_cli: str | None = None,
) -> str:
    """Send extracted text for translation. Uses text-only mode (no image)."""
    prompt = _build_translation_prompt(source_lang, target_lang, text)

    for attempt in range(1, max_retries + 1):
        try:
            t0 = time.time()
            rc, stdout, stderr = _run_ai(
                backend,
                prompt,
                model=translate_model,
                image_path=None,
                timeout=300,
                opencode_cli=opencode_cli,
                attach_url=attach_url,
                gh_cli=gh_cli,
            )
            elapsed = time.time() - t0

            if rc == 0 and stdout:
                chars = len(stdout)
                log.debug(f"    Translation OK in {elapsed:.1f}s ({chars} chars)")
                return _clean_model_output(stdout)
            else:
                log.warning(
                    f"    Translation attempt {attempt}/{max_retries} FAILED ({elapsed:.1f}s): "
                    f"{stderr[:200] or 'empty output'}"
                )
        except subprocess.TimeoutExpired:
            log.warning(
                f"    Translation attempt {attempt}/{max_retries} TIMED OUT (>300s)"
            )
        except FileNotFoundError as e:
            log.error(f"CLI not found: {e}")
            sys.exit(1)

        if attempt < max_retries:
            wait = attempt * 5
            log.info(f"    Retrying in {wait}s...")
            time.sleep(wait)

    return f"<!-- TRANSLATION FAILED after {max_retries} attempts -->"


def translate_all_pages(
    ocr_paths: list[Path],
    output_dir: str,
    source_lang: str,
    target_lang: str,
    backend: str = DEFAULT_BACKEND,
    translate_model: str | None = None,
    delay: float = 2,
    max_retries: int = 3,
    # Backend-specific
    opencode_cli: str | None = None,
    attach_url: str | None = None,
    gh_cli: str | None = None,
) -> list[Path]:
    """Translate all OCR'd text files and save as individual markdown files.

    Returns a sorted list of paths to the generated markdown files.
    Supports resuming (skips pages with existing translation).
    """
    translations_dir = Path(output_dir) / "translations"
    translations_dir.mkdir(parents=True, exist_ok=True)

    total = len(ocr_paths)
    generated: list[Path] = []
    skipped = 0
    failed = 0
    no_text = 0
    phase_start = time.time()

    log.info(f"Translation pass: {total} pages to process (model: {translate_model})")

    for i, txt_path in enumerate(ocr_paths, 1):
        md_path = translations_dir / f"{txt_path.stem}.md"

        # Resume support
        if md_path.exists() and md_path.stat().st_size > 0:
            skipped += 1
            generated.append(md_path)
            elapsed = time.time() - phase_start
            log.info(
                _progress_bar(
                    i, total, elapsed=elapsed, label=f"{txt_path.name} SKIP (cached)"
                )
            )
            continue

        text = txt_path.read_text(encoding="utf-8").strip()

        # Skip pages with no content or failed OCR
        if not text or "NO_TEXT_CONTENT" in text or "OCR FAILED" in text:
            no_text += 1
            md_path.write_text(
                "<!-- No translatable text on this page -->\n", encoding="utf-8"
            )
            generated.append(md_path)
            elapsed = time.time() - phase_start
            log.info(
                _progress_bar(
                    i, total, elapsed=elapsed, label=f"{txt_path.name} SKIP (no text)"
                )
            )
            continue

        elapsed = time.time() - phase_start
        log.info(
            _progress_bar(
                i - 1, total, elapsed=elapsed, label=f"{txt_path.name} translating..."
            )
        )

        page_start = time.time()
        translation = translate_text(
            text,
            source_lang,
            target_lang,
            backend=backend,
            translate_model=translate_model,
            max_retries=max_retries,
            opencode_cli=opencode_cli,
            attach_url=attach_url,
            gh_cli=gh_cli,
        )
        page_elapsed = time.time() - page_start

        if "TRANSLATION FAILED" in translation:
            failed += 1
            log.error(f"    FAILED: {txt_path.name} after {page_elapsed:.1f}s")
        else:
            chars = len(translation)
            log.info(f"    OK: {chars} chars in {page_elapsed:.1f}s -> {md_path.name}")

        md_path.write_text(translation + "\n", encoding="utf-8")
        generated.append(md_path)

        if i < total:
            time.sleep(delay)

    total_time = time.time() - phase_start
    log.info(f"")
    log.info(f"TRANSLATION COMPLETE: {total} pages in {_format_duration(total_time)}")
    log.info(
        f"  Translated: {total - skipped - failed - no_text} | Skipped: {skipped} | No text: {no_text} | Failed: {failed}"
    )
    log.info(f"  Output: {translations_dir}")
    return sorted(generated)


# ---------------------------------------------------------------------------
# Legacy single-pass translation (kept for backward compatibility)
# ---------------------------------------------------------------------------


def translate_page_single_pass(
    image_path: Path,
    prompt: str,
    backend: str = DEFAULT_BACKEND,
    model: str | None = None,
    max_retries: int = 3,
    # Backend-specific
    opencode_cli: str | None = None,
    attach_url: str | None = None,
    gh_cli: str | None = None,
) -> str:
    """Send a page image for OCR+translation in one step.

    This is the original single-pass approach.
    """
    for attempt in range(1, max_retries + 1):
        try:
            rc, stdout, stderr = _run_ai(
                backend,
                prompt,
                model=model,
                image_path=image_path,
                timeout=300,
                opencode_cli=opencode_cli,
                attach_url=attach_url,
                gh_cli=gh_cli,
            )

            if rc == 0 and stdout:
                return _clean_model_output(stdout)
            else:
                log.warning(
                    f"    Attempt {attempt}/{max_retries} failed: {stderr or 'empty output'}"
                )
        except subprocess.TimeoutExpired:
            log.warning(f"    Attempt {attempt}/{max_retries} timed out")
        except FileNotFoundError as e:
            log.error(f"CLI not found: {e}")
            sys.exit(1)

        if attempt < max_retries:
            wait = attempt * 5
            log.info(f"    Retrying in {wait}s...")
            time.sleep(wait)

    return f"<!-- TRANSLATION FAILED for {image_path.name} after {max_retries} attempts -->"


# ---------------------------------------------------------------------------
# Output cleaning
# ---------------------------------------------------------------------------


def _clean_model_output(text: str) -> str:
    """Remove common model boilerplate from output.

    Strips:
    - Markdown code fences (```markdown ... ```)
    - Trailing lines like 'Let me know if...',  'I hope this helps', etc.
    """
    lines = text.rstrip().split("\n")

    # Strip leading/trailing markdown code fences
    # GPT-4o often wraps output in ```markdown ... ```
    if lines and re.match(r"^```\w*$", lines[0].strip()):
        lines.pop(0)
    if lines and lines[-1].strip() == "```":
        lines.pop()

    # Remove trailing boilerplate lines
    boilerplate_patterns = [
        r"^let me know",
        r"^i hope this",
        r"^feel free to",
        r"^if you need",
        r"^please let me",
        r"^is there anything",
        r"^do you want",
        r"^would you like",
        r"^---\s*$",
    ]

    while lines:
        last = lines[-1].strip().lower()
        if not last:
            lines.pop()
            continue
        if any(re.match(pat, last) for pat in boilerplate_patterns):
            lines.pop()
            continue
        break

    return "\n".join(lines).rstrip()


# ---------------------------------------------------------------------------
# Refinement Step 1: Unicode text normalization (FREE — no API calls)
# ---------------------------------------------------------------------------


# Zero-width and invisible Unicode characters to strip
_INVISIBLE_CHARS = re.compile(
    "[\u200b\u200c\u200d\u200e\u200f"  # zero-width space/joiners/marks
    "\u00ad"  # soft hyphen
    "\ufeff"  # BOM / zero-width no-break space
    "\u2060"  # word joiner
    "\u2061\u2062\u2063\u2064"  # invisible operators
    "\u180e"  # mongolian vowel separator (sometimes appears)
    "]"
)


def normalize_text(text: str) -> str:
    """Apply Unicode NFC normalization and whitespace cleanup to text.

    Operations:
    1. NFC normalization (canonical decomposition + canonical composition)
       — ensures Bengali conjuncts and diacritics are in canonical form
    2. Strip zero-width and invisible characters
    3. Collapse multiple spaces into one
    4. Normalize line endings to \\n
    5. Collapse 3+ consecutive blank lines into 2
    6. Strip trailing whitespace from each line
    """
    if not text:
        return text

    # 1. NFC normalization
    text = unicodedata.normalize("NFC", text)

    # 2. Strip invisible/zero-width characters
    text = _INVISIBLE_CHARS.sub("", text)

    # 3. Normalize line endings
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    # 4. Process line by line: strip trailing whitespace, collapse spaces
    lines = []
    for line in text.split("\n"):
        # Collapse multiple spaces (but not leading indentation)
        line = re.sub(r"  +", " ", line)
        # Strip trailing whitespace
        line = line.rstrip()
        lines.append(line)

    text = "\n".join(lines)

    # 5. Collapse 3+ consecutive blank lines into 2
    text = re.sub(r"\n{3,}", "\n\n", text)

    return text.strip()


def normalize_ocr_files(output_dir: str) -> int:
    """Apply normalize_text() to all OCR .txt files in output_dir/ocr/.

    Modifies files in place. Returns the count of files normalized.
    """
    ocr_dir = Path(output_dir) / "ocr"
    if not ocr_dir.exists():
        log.warning(f"OCR directory not found: {ocr_dir}")
        return 0

    count = 0
    for txt_path in sorted(ocr_dir.glob("page_*.txt")):
        original = txt_path.read_text(encoding="utf-8")
        normalized = normalize_text(original)
        if normalized != original:
            txt_path.write_text(normalized + "\n", encoding="utf-8")
            count += 1

    log.info(
        f"Normalized {count} OCR files (of {len(list(ocr_dir.glob('page_*.txt')))} total)"
    )
    return count


# ---------------------------------------------------------------------------
# Refinement Step 2: AI-based OCR error correction (costs API calls)
# ---------------------------------------------------------------------------

_OCR_CORRECTION_PROMPT = (
    "You are an expert Bengali (Bangla) language proofreader specializing in OCR error correction. "
    "Below is Bengali text extracted via OCR from a scanned book page. "
    "It likely contains OCR errors such as:\n"
    "- Character substitution (similar-looking Bengali characters swapped)\n"
    "- Broken words (spaces inserted in the middle of words)\n"
    "- Merged words (missing spaces between words)\n"
    "- Diacritics/vowel mark errors (hasanta, matra placement)\n"
    "- Line breaks in the middle of sentences\n\n"
    "Fix all OCR errors while preserving the original meaning and paragraph structure. "
    "Output ONLY the corrected Bengali text — no commentary, no explanation, no translation.\n\n"
    "--- BEGIN OCR TEXT ---\n"
    "{text}\n"
    "--- END OCR TEXT ---"
)


def correct_ocr_page(
    text: str,
    backend: str = DEFAULT_BACKEND,
    model: str | None = None,
    max_retries: int = 3,
    # Backend-specific
    opencode_cli: str | None = None,
    attach_url: str | None = None,
    gh_cli: str | None = None,
) -> str:
    """Send Bengali OCR text for AI-based error correction. Returns corrected text."""
    if not text.strip() or "NO_TEXT_CONTENT" in text or "OCR FAILED" in text:
        return text

    prompt = _OCR_CORRECTION_PROMPT.format(text=text)

    for attempt in range(1, max_retries + 1):
        try:
            t0 = time.time()
            rc, stdout, stderr = _run_ai(
                backend,
                prompt,
                model=model,
                image_path=None,
                timeout=180,
                opencode_cli=opencode_cli,
                attach_url=attach_url,
                gh_cli=gh_cli,
            )
            elapsed = time.time() - t0

            if rc == 0 and stdout:
                corrected = _clean_model_output(stdout)
                log.debug(
                    f"    OCR correction OK in {elapsed:.1f}s ({len(corrected)} chars)"
                )
                return corrected
            else:
                log.warning(
                    f"    OCR correction attempt {attempt}/{max_retries} FAILED ({elapsed:.1f}s): "
                    f"{stderr[:200] or 'empty output'}"
                )
        except subprocess.TimeoutExpired:
            log.warning(f"    OCR correction attempt {attempt}/{max_retries} TIMED OUT")
        except FileNotFoundError as e:
            log.error(f"CLI not found: {e}")
            sys.exit(1)

        if attempt < max_retries:
            wait = attempt * 5
            log.info(f"    Retrying in {wait}s...")
            time.sleep(wait)

    log.warning("    OCR correction failed, using original text")
    return text


def correct_ocr_all_pages(
    output_dir: str,
    backend: str = DEFAULT_BACKEND,
    model: str | None = None,
    delay: float = 2,
    max_retries: int = 3,
    # Backend-specific
    opencode_cli: str | None = None,
    attach_url: str | None = None,
    gh_cli: str | None = None,
) -> list[Path]:
    """Run AI-based OCR correction on all pages. Saves to ocr_corrected/ directory.

    Supports resume (skips pages with existing corrected output).
    Returns sorted list of corrected file paths.
    """
    ocr_dir = Path(output_dir) / "ocr"
    corrected_dir = Path(output_dir) / "ocr_corrected"
    corrected_dir.mkdir(parents=True, exist_ok=True)

    if not ocr_dir.exists():
        log.error(f"OCR directory not found: {ocr_dir}")
        return []

    txt_files = sorted(ocr_dir.glob("page_*.txt"))
    total = len(txt_files)
    generated: list[Path] = []
    skipped = 0
    phase_start = time.time()

    log.info(f"OCR correction pass: {total} pages (model: {model})")

    for i, txt_path in enumerate(txt_files, 1):
        corrected_path = corrected_dir / txt_path.name

        # Resume support
        if corrected_path.exists() and corrected_path.stat().st_size > 0:
            skipped += 1
            generated.append(corrected_path)
            elapsed = time.time() - phase_start
            log.info(
                _progress_bar(
                    i, total, elapsed=elapsed, label=f"{txt_path.name} SKIP (cached)"
                )
            )
            continue

        text = txt_path.read_text(encoding="utf-8").strip()

        if not text or "NO_TEXT_CONTENT" in text or "OCR FAILED" in text:
            corrected_path.write_text(text + "\n", encoding="utf-8")
            generated.append(corrected_path)
            elapsed = time.time() - phase_start
            log.info(
                _progress_bar(
                    i, total, elapsed=elapsed, label=f"{txt_path.name} SKIP (no text)"
                )
            )
            continue

        elapsed = time.time() - phase_start
        log.info(
            _progress_bar(
                i - 1, total, elapsed=elapsed, label=f"{txt_path.name} correcting..."
            )
        )

        corrected = correct_ocr_page(
            text,
            backend=backend,
            model=model,
            max_retries=max_retries,
            opencode_cli=opencode_cli,
            attach_url=attach_url,
            gh_cli=gh_cli,
        )

        # Normalize the corrected output too
        corrected = normalize_text(corrected)
        corrected_path.write_text(corrected + "\n", encoding="utf-8")
        generated.append(corrected_path)

        if i < total:
            time.sleep(delay)

    total_time = time.time() - phase_start
    log.info(f"")
    log.info(
        f"OCR CORRECTION COMPLETE: {total} pages in {_format_duration(total_time)}"
    )
    log.info(f"  Processed: {total - skipped} | Skipped: {skipped}")
    log.info(f"  Output: {corrected_dir}")
    return sorted(generated)


# ---------------------------------------------------------------------------
# Refinement Step 3: Post-translation AI refinement (costs API calls)
# ---------------------------------------------------------------------------

_REFINEMENT_PROMPT = (
    "You are an expert Bengali-to-English literary translation reviewer. "
    "Below is a Bengali original text and its English translation from a novel. "
    "Review both carefully and:\n"
    "1. Fix any remaining OCR errors in the Bengali text\n"
    "2. Improve the English translation for accuracy, fluency, and literary quality\n"
    "3. Ensure paragraph structure is preserved\n"
    "4. Ensure the English reads as natural prose, not a literal word-for-word translation\n\n"
    "Output the result in this exact markdown format:\n"
    "## Original (Bengali)\n\n"
    "<corrected Bengali text>\n\n"
    "---\n\n"
    "## Translation (English)\n\n"
    "<improved English translation>\n\n"
    "Do NOT add any commentary, notes, or explanation.\n\n"
    "--- BEGIN BENGALI ORIGINAL ---\n"
    "{bn_text}\n"
    "--- END BENGALI ORIGINAL ---\n\n"
    "--- BEGIN ENGLISH TRANSLATION ---\n"
    "{en_text}\n"
    "--- END ENGLISH TRANSLATION ---"
)


def refine_translation_page(
    bn_text: str,
    en_text: str,
    backend: str = DEFAULT_BACKEND,
    model: str | None = None,
    max_retries: int = 3,
    # Backend-specific
    opencode_cli: str | None = None,
    attach_url: str | None = None,
    gh_cli: str | None = None,
) -> str:
    """Send a Bengali+English pair for AI-based refinement. Returns refined markdown."""
    prompt = _REFINEMENT_PROMPT.format(bn_text=bn_text, en_text=en_text)

    for attempt in range(1, max_retries + 1):
        try:
            t0 = time.time()
            rc, stdout, stderr = _run_ai(
                backend,
                prompt,
                model=model,
                image_path=None,
                timeout=300,
                opencode_cli=opencode_cli,
                attach_url=attach_url,
                gh_cli=gh_cli,
            )
            elapsed = time.time() - t0

            if rc == 0 and stdout:
                refined = _clean_model_output(stdout)
                log.debug(f"    Refinement OK in {elapsed:.1f}s ({len(refined)} chars)")
                return refined
            else:
                log.warning(
                    f"    Refinement attempt {attempt}/{max_retries} FAILED ({elapsed:.1f}s): "
                    f"{stderr[:200] or 'empty output'}"
                )
        except subprocess.TimeoutExpired:
            log.warning(f"    Refinement attempt {attempt}/{max_retries} TIMED OUT")
        except FileNotFoundError as e:
            log.error(f"CLI not found: {e}")
            sys.exit(1)

        if attempt < max_retries:
            wait = attempt * 5
            log.info(f"    Retrying in {wait}s...")
            time.sleep(wait)

    log.warning("    Refinement failed, keeping original translation")
    return ""  # Empty signals "keep original"


def refine_all_translations(
    output_dir: str,
    backend: str = DEFAULT_BACKEND,
    model: str | None = None,
    delay: float = 2,
    max_retries: int = 3,
    # Backend-specific
    opencode_cli: str | None = None,
    attach_url: str | None = None,
    gh_cli: str | None = None,
) -> list[Path]:
    """Run AI-based refinement on all translated pages. Saves to refined/ directory.

    Reads Bengali from ocr/ (or ocr_corrected/ if exists) and English from translations/.
    Supports resume (skips pages with existing refined output).
    Returns sorted list of refined file paths.
    """
    # Prefer corrected OCR if available
    corrected_dir = Path(output_dir) / "ocr_corrected"
    ocr_dir = corrected_dir if corrected_dir.exists() else Path(output_dir) / "ocr"
    translations_dir = Path(output_dir) / "translations"
    refined_dir = Path(output_dir) / "refined"
    refined_dir.mkdir(parents=True, exist_ok=True)

    if not translations_dir.exists():
        log.error(f"Translations directory not found: {translations_dir}")
        return []

    md_files = sorted(translations_dir.glob("page_*.md"))
    total = len(md_files)
    generated: list[Path] = []
    skipped = 0
    phase_start = time.time()

    log.info(f"Translation refinement pass: {total} pages (model: {model})")
    log.info(f"  Bengali source: {ocr_dir}")

    for i, md_path in enumerate(md_files, 1):
        refined_path = refined_dir / md_path.name

        # Resume support
        if refined_path.exists() and refined_path.stat().st_size > 0:
            skipped += 1
            generated.append(refined_path)
            elapsed = time.time() - phase_start
            log.info(
                _progress_bar(
                    i, total, elapsed=elapsed, label=f"{md_path.name} SKIP (cached)"
                )
            )
            continue

        # Read the translation file
        md_content = md_path.read_text(encoding="utf-8").strip()
        if "No translatable text" in md_content:
            refined_path.write_text(
                "<!-- No translatable text on this page -->\n", encoding="utf-8"
            )
            generated.append(refined_path)
            continue

        # Read Bengali source
        bn_path = ocr_dir / f"{md_path.stem}.txt"
        if bn_path.exists():
            bn_text = bn_path.read_text(encoding="utf-8").strip()
        else:
            # Extract from translation file
            bn_paras, _ = _parse_translation_md(md_path)
            bn_text = "\n\n".join(bn_paras)

        # Extract English from translation file
        _, en_paras = _parse_translation_md(md_path)
        en_text = "\n\n".join(en_paras)

        if not bn_text or not en_text:
            refined_path.write_text(md_content + "\n", encoding="utf-8")
            generated.append(refined_path)
            continue

        elapsed = time.time() - phase_start
        log.info(
            _progress_bar(
                i - 1, total, elapsed=elapsed, label=f"{md_path.name} refining..."
            )
        )

        refined = refine_translation_page(
            bn_text,
            en_text,
            backend=backend,
            model=model,
            max_retries=max_retries,
            opencode_cli=opencode_cli,
            attach_url=attach_url,
            gh_cli=gh_cli,
        )

        if refined:
            refined_path.write_text(refined + "\n", encoding="utf-8")
        else:
            # Refinement failed, copy original
            refined_path.write_text(md_content + "\n", encoding="utf-8")

        generated.append(refined_path)

        if i < total:
            time.sleep(delay)

    total_time = time.time() - phase_start
    log.info(f"")
    log.info(f"REFINEMENT COMPLETE: {total} pages in {_format_duration(total_time)}")
    log.info(f"  Refined: {total - skipped} | Skipped: {skipped}")
    log.info(f"  Output: {refined_dir}")
    return sorted(generated)


# ---------------------------------------------------------------------------
# Refinement Step 4: Cross-page continuity stitching (FREE)
# ---------------------------------------------------------------------------

# Bengali sentence-ending punctuation: dari (।), question mark, exclamation
_BN_SENTENCE_END = re.compile(r"[।?!।\?\!]\s*$")
# Starts with a Bengali letter or common paragraph indicator
_BN_PARA_START = re.compile(r"^[\u0980-\u09FF\"'\'\"\u201C\u201D—–-]")


def stitch_pages(ocr_dir: str) -> tuple[int, list[str]]:
    """Detect and fix sentence fragments split across page boundaries.

    Examines the last line of page N and the first line of page N+1.
    If the last line doesn't end with sentence-ending punctuation AND the
    first line of the next page doesn't look like a new paragraph, merge them.

    Modifies OCR files in place. Returns (count_of_stitches, list_of_descriptions).
    """
    ocr_path = Path(ocr_dir)
    txt_files = sorted(ocr_path.glob("page_*.txt"))

    if len(txt_files) < 2:
        return 0, []

    stitches: list[str] = []
    count = 0

    for i in range(len(txt_files) - 1):
        curr_path = txt_files[i]
        next_path = txt_files[i + 1]

        curr_text = curr_path.read_text(encoding="utf-8").rstrip()
        next_text = next_path.read_text(encoding="utf-8").lstrip()

        if not curr_text or not next_text:
            continue
        if "NO_TEXT_CONTENT" in curr_text or "NO_TEXT_CONTENT" in next_text:
            continue
        if "OCR FAILED" in curr_text or "OCR FAILED" in next_text:
            continue

        # Get last non-empty line of current page
        curr_lines = [l for l in curr_text.split("\n") if l.strip()]
        next_lines = [l for l in next_text.split("\n") if l.strip()]

        if not curr_lines or not next_lines:
            continue

        last_line = curr_lines[-1].strip()
        first_line = next_lines[0].strip()

        # Check: does last line end with sentence-ending punctuation?
        ends_sentence = bool(_BN_SENTENCE_END.search(last_line))

        # Check: does first line of next page look like a new paragraph?
        # (starts with indentation, a dash/quote, or follows a blank line gap)
        starts_new_para = (
            next_text.startswith("\n")  # blank line at start
            or next_text.startswith("  ")  # indented
            or next_text.startswith("\t")  # tab indented
        )

        if not ends_sentence and not starts_new_para:
            # Stitch: append first line of next page to end of current page
            # and remove it from next page
            count += 1
            desc = (
                f"Stitched {curr_path.name} -> {next_path.name}: "
                f"...{last_line[-30:]} + {first_line[:30]}..."
            )
            stitches.append(desc)
            log.info(f"  Stitch #{count}: {curr_path.name} <-> {next_path.name}")

            # Merge: append first line of next to current
            new_curr = curr_text + " " + first_line
            curr_path.write_text(new_curr + "\n", encoding="utf-8")

            # Remove first line from next page
            remaining_lines = next_lines[1:]
            new_next = "\n".join(remaining_lines) if remaining_lines else ""
            next_path.write_text(new_next + "\n", encoding="utf-8")

    log.info(f"Cross-page stitching: {count} fragments merged")
    return count, stitches


# ---------------------------------------------------------------------------
# Refinement Step 5: Duplicate/overlap detection at page boundaries (FREE)
# ---------------------------------------------------------------------------


def detect_duplicates(
    ocr_dir: str, similarity_threshold: float = 0.6, window: int = 150
) -> list[dict]:
    """Detect overlapping/duplicate content at page boundaries.

    Compares the last `window` characters of page N with the first `window`
    characters of page N+1 using SequenceMatcher. If similarity exceeds
    threshold, flags it as a potential duplicate.

    Returns a list of dicts: {page_a, page_b, similarity, overlap_a, overlap_b}
    Does NOT modify files — only reports.
    """
    ocr_path = Path(ocr_dir)
    txt_files = sorted(ocr_path.glob("page_*.txt"))

    if len(txt_files) < 2:
        return []

    duplicates: list[dict] = []

    for i in range(len(txt_files) - 1):
        curr_path = txt_files[i]
        next_path = txt_files[i + 1]

        curr_text = curr_path.read_text(encoding="utf-8").strip()
        next_text = next_path.read_text(encoding="utf-8").strip()

        if not curr_text or not next_text:
            continue

        # Get tail of current and head of next
        tail = curr_text[-window:]
        head = next_text[:window]

        # Compare using SequenceMatcher
        ratio = difflib.SequenceMatcher(None, tail, head).ratio()

        if ratio >= similarity_threshold:
            duplicates.append(
                {
                    "page_a": curr_path.name,
                    "page_b": next_path.name,
                    "similarity": round(ratio, 3),
                    "overlap_a_tail": tail[-80:],
                    "overlap_b_head": head[:80],
                }
            )
            log.warning(
                f"  Potential overlap: {curr_path.name} <-> {next_path.name} "
                f"(similarity: {ratio:.1%})"
            )

    log.info(
        f"Duplicate detection: {len(duplicates)} potential overlaps found "
        f"(threshold: {similarity_threshold:.0%})"
    )
    return duplicates


# ---------------------------------------------------------------------------
# Refinement Step 6: Quality scoring (FREE)
# ---------------------------------------------------------------------------

# Bengali Unicode range: U+0980 - U+09FF
_BN_CHAR_RE = re.compile(r"[\u0980-\u09FF]")
# Common English words that shouldn't appear in Bengali text
_EN_WORD_RE = re.compile(r"\b[a-zA-Z]{3,}\b")


def score_page_quality(page_name: str, bn_text: str, en_text: str) -> dict:
    """Score the quality of a single Bengali/English page pair.

    Metrics:
    - length_ratio: ratio of Bengali chars to English chars (expected ~0.5-1.5)
    - bn_non_bengali_pct: percentage of non-Bengali/non-whitespace chars in bn text
    - en_untranslated_pct: percentage of Bengali chars remaining in English text
    - bn_length: total Bengali text length
    - en_length: total English text length
    - is_short: whether the page has very little content (<50 chars)
    - confidence: overall quality score 0-100

    Returns a dict with all metrics.
    """
    bn_len = len(bn_text.strip())
    en_len = len(en_text.strip())

    # Bengali character counts
    bn_bengali_chars = len(_BN_CHAR_RE.findall(bn_text))
    bn_total_non_ws = len(re.sub(r"\s", "", bn_text))

    # Non-Bengali percentage in Bengali text
    bn_non_bengali_pct = 0.0
    if bn_total_non_ws > 0:
        bn_non_bengali = bn_total_non_ws - bn_bengali_chars
        # Don't count Bengali punctuation (dari, etc) as non-Bengali
        bn_punctuation = len(
            re.findall(r"[।,;:\"\'—\-–\(\)\[\]\{\}!?\.\u0964\u0965]", bn_text)
        )
        bn_non_bengali = max(0, bn_non_bengali - bn_punctuation)
        bn_non_bengali_pct = round(bn_non_bengali / bn_total_non_ws * 100, 1)

    # Untranslated Bengali in English text
    en_bengali_chars = len(_BN_CHAR_RE.findall(en_text))
    en_total_chars = len(re.sub(r"\s", "", en_text))
    en_untranslated_pct = 0.0
    if en_total_chars > 0:
        en_untranslated_pct = round(en_bengali_chars / en_total_chars * 100, 1)

    # Length ratio
    length_ratio = 0.0
    if en_len > 0:
        length_ratio = round(bn_len / en_len, 2)

    # Is the page suspiciously short?
    is_short = bn_len < 50 or en_len < 50

    # Overall confidence score (0-100)
    confidence = 100.0
    penalties = []

    # Penalty for extreme length ratios (expect 0.4-2.0)
    if length_ratio < 0.2 or length_ratio > 3.0:
        penalty = 30
        confidence -= penalty
        penalties.append(f"extreme_length_ratio({length_ratio})")
    elif length_ratio < 0.4 or length_ratio > 2.0:
        penalty = 15
        confidence -= penalty
        penalties.append(f"unusual_length_ratio({length_ratio})")

    # Penalty for non-Bengali characters in Bengali text
    if bn_non_bengali_pct > 30:
        penalty = 25
        confidence -= penalty
        penalties.append(f"high_non_bengali({bn_non_bengali_pct}%)")
    elif bn_non_bengali_pct > 15:
        penalty = 10
        confidence -= penalty
        penalties.append(f"moderate_non_bengali({bn_non_bengali_pct}%)")

    # Penalty for untranslated Bengali in English
    if en_untranslated_pct > 20:
        penalty = 30
        confidence -= penalty
        penalties.append(f"high_untranslated({en_untranslated_pct}%)")
    elif en_untranslated_pct > 5:
        penalty = 15
        confidence -= penalty
        penalties.append(f"some_untranslated({en_untranslated_pct}%)")

    # Penalty for very short pages
    if is_short:
        penalty = 10
        confidence -= penalty
        penalties.append("very_short_page")

    confidence = max(0, round(confidence, 1))

    return {
        "page": page_name,
        "bn_length": bn_len,
        "en_length": en_len,
        "length_ratio": length_ratio,
        "bn_non_bengali_pct": bn_non_bengali_pct,
        "en_untranslated_pct": en_untranslated_pct,
        "is_short": is_short,
        "confidence": confidence,
        "penalties": penalties,
    }


def score_all_pages(output_dir: str) -> dict:
    """Score quality of all translated pages and generate quality_report.json.

    Reads Bengali from ocr/ (or ocr_corrected/) and English from translations/ (or refined/).
    Returns the full report dict and saves it to output_dir/quality_report.json.
    """
    # Prefer refined/corrected sources
    refined_dir = Path(output_dir) / "refined"
    translations_dir = (
        refined_dir if refined_dir.exists() else Path(output_dir) / "translations"
    )
    corrected_dir = Path(output_dir) / "ocr_corrected"
    ocr_dir = corrected_dir if corrected_dir.exists() else Path(output_dir) / "ocr"

    if not translations_dir.exists():
        log.error(f"No translations found in {output_dir}")
        return {}

    md_files = sorted(translations_dir.glob("page_*.md"))
    page_scores: list[dict] = []
    low_confidence_pages: list[str] = []

    for md_path in md_files:
        # Read Bengali
        bn_path = ocr_dir / f"{md_path.stem}.txt"
        if bn_path.exists():
            bn_text = bn_path.read_text(encoding="utf-8").strip()
        else:
            bn_text = ""

        # Read English from translation/refined md
        _, en_paras = _parse_translation_md(md_path)
        en_text = "\n\n".join(en_paras)

        if not bn_text and not en_text:
            continue

        score = score_page_quality(md_path.name, bn_text, en_text)
        page_scores.append(score)

        if score["confidence"] < 60:
            low_confidence_pages.append(score["page"])

    # Summary statistics
    if page_scores:
        confidences = [s["confidence"] for s in page_scores]
        avg_confidence = round(sum(confidences) / len(confidences), 1)
        min_confidence = min(confidences)
        max_confidence = max(confidences)
        ratios = [s["length_ratio"] for s in page_scores if s["length_ratio"] > 0]
        avg_ratio = round(sum(ratios) / len(ratios), 2) if ratios else 0
    else:
        avg_confidence = 0
        min_confidence = 0
        max_confidence = 0
        avg_ratio = 0

    report = {
        "generated_at": datetime.now().isoformat(),
        "total_pages": len(page_scores),
        "summary": {
            "avg_confidence": avg_confidence,
            "min_confidence": min_confidence,
            "max_confidence": max_confidence,
            "avg_length_ratio": avg_ratio,
            "low_confidence_count": len(low_confidence_pages),
            "low_confidence_pages": low_confidence_pages,
        },
        "pages": page_scores,
    }

    report_path = Path(output_dir) / "quality_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    log.info(f"Quality report saved to {report_path}")
    log.info(
        f"  Pages scored: {len(page_scores)} | "
        f"Avg confidence: {avg_confidence} | "
        f"Low confidence (<60): {len(low_confidence_pages)}"
    )
    if low_confidence_pages:
        log.warning(f"  Low confidence pages: {', '.join(low_confidence_pages)}")

    return report


def combine_translations(
    translation_paths: list[Path], output_dir: str, title: str, author: str
) -> Path:
    """Merge individual page translations into a single markdown book file."""
    book_path = Path(output_dir) / "full_book.md"

    frontmatter = f"""---
title: "{title}"
author: "{author}"
translated_by: "OpenCode AI"
date: "{time.strftime("%Y-%m-%d")}"
---

# {title}
**by {author}**

*Translated by OpenCode AI on {time.strftime("%B %d, %Y")}*

---

"""

    with open(book_path, "w", encoding="utf-8") as f:
        f.write(frontmatter)

        for md_path in translation_paths:
            content = md_path.read_text(encoding="utf-8").strip()
            if "No translatable text" in content:
                continue

            page_num = md_path.stem.replace("page_", "")
            f.write(f"\n<!-- Page {page_num} -->\n\n")
            f.write(content)
            f.write("\n\n---\n")

    log.info(f"Combined book saved to {book_path}")
    return book_path


# ---------------------------------------------------------------------------
# JSON export (bangla-library format)
# ---------------------------------------------------------------------------


def _parse_translation_md(md_path: Path) -> tuple[list[str], list[str]]:
    """Parse a translation .md file into Bengali and English paragraph lists.

    Expected format:
        ## Original (Bengali)

        <paragraphs separated by blank lines>

        ---

        ## Translation (English)

        <paragraphs separated by blank lines>

    Returns (bengali_paragraphs, english_paragraphs).
    """
    text = md_path.read_text(encoding="utf-8").strip()

    # Skip pages with no translatable text
    if "No translatable text" in text:
        return [], []

    # Split into Bengali and English sections
    # Look for the --- separator between sections
    bn_paragraphs: list[str] = []
    en_paragraphs: list[str] = []

    # Find the Bengali section
    bn_start = text.find("## Original (Bengali)")
    en_start = text.find("## Translation (English)")

    if bn_start == -1 or en_start == -1:
        # Fallback: try to find just the separator
        log.warning(f"  Non-standard format in {md_path.name}, skipping")
        return [], []

    # Extract Bengali text: between "## Original (Bengali)" header and "---"
    bn_section = text[bn_start:en_start]
    # Remove the header line
    bn_section = bn_section.replace("## Original (Bengali)", "").strip()
    # Remove trailing --- separator
    bn_section = bn_section.rstrip("-").strip()

    # Extract English text: after "## Translation (English)" header
    en_section = text[en_start:]
    en_section = en_section.replace("## Translation (English)", "").strip()
    # Remove trailing --- if present
    en_section = en_section.rstrip("-").strip()

    # Split into paragraphs by blank lines
    bn_paragraphs = [p.strip() for p in re.split(r"\n\s*\n", bn_section) if p.strip()]
    en_paragraphs = [p.strip() for p in re.split(r"\n\s*\n", en_section) if p.strip()]

    # The AI sometimes formats Bengali text without blank lines between paragraphs
    # (each paragraph is just a single newline away). If blank-line splitting produces
    # far fewer Bengali paragraphs than English, fall back to single-newline splitting.
    if (
        bn_paragraphs
        and en_paragraphs
        and len(bn_paragraphs) < len(en_paragraphs) * 0.5
    ):
        bn_paragraphs = [p.strip() for p in bn_section.split("\n") if p.strip()]

    return bn_paragraphs, en_paragraphs


def export_to_json(
    translation_paths: list[Path],
    output_dir: str,
    title_en: str,
    title_bn: str = "",
    author_en: str = "",
    author_bn: str = "",
    year: str = "",
    category: str = "Novel",
    description_en: str = "",
    slug: str | None = None,
    # --- bangla-library extended fields ---
    author_slug: str = "",
    status: str = "published",
    published_date: str = "",
    description_bn: str = "",
    copyright_notice: str = "",
    source: str = "",
    original_publisher: str = "",
    edition_note: str = "",
    translation_reviewed: bool = False,
    cover_image: str = "",
    back_image: str = "",
    priority: int = 2,
    publish_date: str = "",
) -> Path:
    """Convert translation .md files to bangla-library JSON format.

    Reads all translation files, extracts Bengali/English paragraph pairs,
    and writes a single JSON file compatible with the bangla-library Astro site.

    Extended fields (all optional in the schema):
      author_slug        - Links to author profile page (e.g. "rabindranath-tagore")
      status             - "published" or "unpublished" (default: "published")
      published_date     - ISO date when book was added (e.g. "2025-06-15")
      description_bn     - Bengali description
      copyright_notice   - For non-public-domain works
      source             - URL or reference to original PDF source
      original_publisher - Original publisher name
      edition_note       - Notes about the edition used
      translation_reviewed - Whether a human reviewed the translation (default: False)
      cover_image        - Path/URL for cover image
      back_image         - Path/URL for back cover image
      priority           - Sort priority (lower = first, default: 2)
      publish_date       - Scheduled publish date (ISO format)
    """
    all_bn: list[str] = []
    all_en: list[str] = []

    for md_path in translation_paths:
        bn_paras, en_paras = _parse_translation_md(md_path)

        if not bn_paras and not en_paras:
            continue

        # If paragraph counts don't match, log a warning but still include them.
        # Pair up as many as we can, then append any extras as unpaired.
        if len(bn_paras) != len(en_paras):
            log.warning(
                f"  Paragraph mismatch in {md_path.name}: "
                f"{len(bn_paras)} bn vs {len(en_paras)} en"
            )

        all_bn.extend(bn_paras)
        all_en.extend(en_paras)

    # Build the paragraphs array with paired bn/en
    # Use the shorter list length for paired entries, then append extras
    paired_count = min(len(all_bn), len(all_en))
    paragraphs = []

    for i in range(paired_count):
        paragraphs.append(
            {
                "id": i + 1,
                "bn": all_bn[i],
                "en": all_en[i],
            }
        )

    # Append any remaining unpaired paragraphs
    for i in range(paired_count, len(all_bn)):
        paragraphs.append(
            {
                "id": len(paragraphs) + 1,
                "bn": all_bn[i],
                "en": "(translation not available)",
            }
        )
    for i in range(paired_count, len(all_en)):
        paragraphs.append(
            {
                "id": len(paragraphs) + 1,
                "bn": "(মূল পাঠ্য পাওয়া যায়নি)",
                "en": all_en[i],
            }
        )

    # Build the book JSON object
    book_data: dict = {
        "title_bn": title_bn,
        "title_en": title_en,
        "author_bn": author_bn,
        "author_en": author_en,
        "year": year,
        "category": category,
    }
    # Optional fields — only include if non-empty to keep JSON clean
    if author_slug:
        book_data["author_slug"] = author_slug
    if status and status != "published":
        # Schema defaults to "published", so only emit if different
        book_data["status"] = status
    if published_date:
        book_data["published_date"] = published_date
    if description_en:
        book_data["description_en"] = description_en
    if description_bn:
        book_data["description_bn"] = description_bn
    if copyright_notice:
        book_data["copyright_notice"] = copyright_notice
    if source:
        book_data["source"] = source
    if original_publisher:
        book_data["original_publisher"] = original_publisher
    if edition_note:
        book_data["edition_note"] = edition_note
    if translation_reviewed:
        book_data["translation_reviewed"] = True
    if cover_image:
        book_data["cover_image"] = cover_image
    if back_image:
        book_data["back_image"] = back_image
    if priority != 2:
        # Schema defaults to 2, so only emit if different
        book_data["priority"] = priority
    if publish_date:
        book_data["publish_date"] = publish_date
    book_data["paragraphs"] = paragraphs

    # Determine output filename
    if not slug:
        slug = title_en.lower().replace(" ", "-")
        # Clean slug: keep only alphanumeric and hyphens
        slug = re.sub(r"[^a-z0-9-]", "", slug)
        slug = re.sub(r"-+", "-", slug).strip("-")

    json_path = Path(output_dir) / f"{slug}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(book_data, f, ensure_ascii=False, indent=2)

    log.info(f"Exported {len(paragraphs)} paragraphs to {json_path}")
    log.info(
        f"  Bengali: {len(all_bn)} | English: {len(all_en)} | Paired: {paired_count}"
    )
    return json_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def load_config(config_path: str) -> dict:
    """Load and return the configuration from a JSON file."""
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_prompt(source_lang: str, target_lang: str) -> str:
    """Build the translation prompt for OpenCode."""
    return (
        f"Translate the following {source_lang} text to "
        f"{target_lang}. Provide both the original {source_lang} "
        f"text and the {target_lang} translation. Format the output "
        f"in markdown with the original text first under a "
        f"'## Original ({source_lang})' heading, followed by the "
        f"translation under a '## Translation ({target_lang})' "
        f"heading. Preserve paragraph breaks. If the page contains no "
        f"translatable text (e.g., it's a blank page or only has images),"
        f" respond with 'NO_TEXT_CONTENT'."
    )


def derive_book_info(pdf_path: str) -> tuple[str, str]:
    """Derive a book title and author from the PDF filename.

    Tries to parse patterns like 'Title-By-Author.pdf'.
    Falls back to using the filename as title with 'Unknown' author.
    """
    stem = Path(pdf_path).stem  # e.g. "Moyurakkhi-By-Humayun-Ahmed"

    # Try to split on common separators: "-By-", "_By_", " By "
    for sep in ["-By-", "_By_", " By ", "-by-", "_by_", " by "]:
        if sep in stem:
            parts = stem.split(sep, 1)
            title = parts[0].replace("-", " ").replace("_", " ").strip()
            author = parts[1].replace("-", " ").replace("_", " ").strip()
            return title, author

    # Fallback: use filename as title
    title = stem.replace("-", " ").replace("_", " ").strip()
    return title, "Unknown"


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    """Add common CLI args shared by multiple subcommands."""
    parser.add_argument(
        "--backend",
        "-b",
        default=None,
        choices=["gh-copilot", "opencode"],
        help=f"AI backend (default: {DEFAULT_BACKEND})",
    )
    parser.add_argument(
        "--attach",
        default=None,
        help="(opencode only) Attach to running OpenCode server URL",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=2,
        help="Delay between pages in seconds (default: 2)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        default=False,
        help="Enable verbose/debug logging",
    )


def _resolve_backend(args, config: dict | None = None) -> dict:
    """Resolve the AI backend and return a context dict with all needed info.

    Returns a dict with keys:
      backend, gh_cli, opencode_cli, attach_url
    """
    config = config or {}
    backend = getattr(args, "backend", None) or config.get("backend") or DEFAULT_BACKEND

    ctx = {
        "backend": backend,
        "gh_cli": None,
        "opencode_cli": None,
        "attach_url": None,
    }

    if backend == "gh-copilot":
        ctx["gh_cli"] = find_gh_cli()
        log.info(f"Backend: gh-copilot ({ctx['gh_cli']})")
    elif backend == "opencode":
        ctx["opencode_cli"] = find_opencode_cli()
        log.info(f"Backend: opencode ({ctx['opencode_cli']})")

        attach_url = getattr(args, "attach", None)
        if not attach_url:
            log.info("Looking for running OpenCode server...")
            attach_url = find_opencode_server()
            if attach_url:
                log.info(f"Found OpenCode server at: {attach_url}")
            else:
                log.error("No running OpenCode server found.")
                log.error(
                    "TIP: Start OpenCode desktop app or run 'opencode serve' first."
                )
                log.error("     Then use --attach http://localhost:<port>")
                sys.exit(1)
        ctx["attach_url"] = attach_url
    else:
        log.error(f"Unknown backend '{backend}'")
        sys.exit(1)

    return ctx


def main():
    global log

    parser = argparse.ArgumentParser(
        description="PDF-to-Book: Translate PDF books using AI (gh copilot or OpenCode)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full pipeline with two-pass (OCR + translate) — default uses gh-copilot + Claude
  python pdf_to_book.py run book.pdf

  # Full pipeline with AI refinement (OCR correction + translation review)
  python pdf_to_book.py run book.pdf --refine

  # Use config file
  python pdf_to_book.py run book.pdf -c config.json

  # Specify models
  python pdf_to_book.py run book.pdf --ocr-model claude-sonnet-4.6 --translate-model claude-opus-4.6

  # Use OpenCode backend instead (legacy)
  python pdf_to_book.py run book.pdf --backend opencode --ocr-model github-copilot/gpt-4o

  # Single-pass mode (one model does OCR + translation)
  python pdf_to_book.py run book.pdf --single-pass -m claude-sonnet-4.6

  # Only extract images from PDF
  python pdf_to_book.py extract book.pdf

  # Only OCR already-extracted images
  python pdf_to_book.py ocr -o output/moyurakkhi/

  # Only translate already-OCR'd text
  python pdf_to_book.py translate -o output/moyurakkhi/

  # Run AI refinement on existing output (standalone)
  python pdf_to_book.py refine -o output/moyurakkhi/

  # Combine translations into one file
  python pdf_to_book.py combine -o output/moyurakkhi/ --title "My Book" --author "Author"
        """,
    )

    # Global flags
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose/debug logging",
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # --- run: full pipeline ---
    run_parser = subparsers.add_parser("run", help="Run the full pipeline")
    run_parser.add_argument("pdf", help="Path to PDF file")
    run_parser.add_argument(
        "--config",
        "-c",
        default=None,
        help="Optional config JSON (overrides are merged)",
    )
    run_parser.add_argument(
        "--model",
        "-m",
        default=None,
        help="Single model for both OCR and translation (used in --single-pass mode)",
    )
    run_parser.add_argument(
        "--ocr-model",
        default=None,
        help="Model for OCR pass (default: claude-sonnet-4.6)",
    )
    run_parser.add_argument(
        "--translate-model",
        default=None,
        help="Model for translation pass (default: claude-sonnet-4.6)",
    )
    run_parser.add_argument(
        "--single-pass",
        action="store_true",
        help="Use single-pass mode: one model does OCR+translation together (uses --model)",
    )
    run_parser.add_argument(
        "--output",
        "-o",
        default=None,
        help="Output directory (default: output/<book-name>)",
    )
    run_parser.add_argument("--title", "-t", default=None, help="Book title")
    run_parser.add_argument("--author", "-a", default=None, help="Book author")
    run_parser.add_argument(
        "--from",
        dest="source_lang",
        default="Bengali",
        help="Source language (default: Bengali)",
    )
    run_parser.add_argument(
        "--to",
        dest="target_lang",
        default="English",
        help="Target language (default: English)",
    )
    run_parser.add_argument(
        "--dpi",
        type=int,
        default=300,
        help="Image DPI (default: 300)",
    )
    run_parser.add_argument("--start", type=int, default=1, help="Start page number")
    run_parser.add_argument("--end", type=int, default=None, help="End page number")
    run_parser.add_argument(
        "--skip",
        type=int,
        nargs="*",
        default=[],
        help="Page numbers to skip",
    )
    run_parser.add_argument(
        "--export-json",
        action="store_true",
        help="Also export to bangla-library JSON format after combining",
    )
    run_parser.add_argument(
        "--title-bn", default="", help="Bengali title (for JSON export)"
    )
    run_parser.add_argument(
        "--author-bn", default="", help="Author name in Bengali (for JSON export)"
    )
    run_parser.add_argument(
        "--year", default="", help="Publication year (for JSON export)"
    )
    run_parser.add_argument(
        "--category", default="Novel", help="Category (for JSON export, default: Novel)"
    )
    run_parser.add_argument(
        "--description", default="", help="English description (for JSON export)"
    )
    run_parser.add_argument(
        "--json-dest",
        default=None,
        help="Destination dir for JSON file (default: output dir)",
    )
    # --- bangla-library extended fields (for JSON export) ---
    run_parser.add_argument(
        "--author-slug",
        default="",
        help="Author slug for profile link (e.g. 'rabindranath-tagore')",
    )
    run_parser.add_argument(
        "--status",
        default="published",
        choices=["published", "unpublished"],
        help="Publication status (default: published)",
    )
    run_parser.add_argument(
        "--published-date",
        default="",
        help="ISO date when book was added (e.g. '2025-06-15'). Defaults to today.",
    )
    run_parser.add_argument(
        "--description-bn", default="", help="Bengali description (for JSON export)"
    )
    run_parser.add_argument(
        "--copyright-notice", default="", help="Copyright notice (for JSON export)"
    )
    run_parser.add_argument(
        "--source", default="", help="URL or reference to original PDF source"
    )
    run_parser.add_argument(
        "--original-publisher", default="", help="Original publisher name"
    )
    run_parser.add_argument(
        "--edition-note", default="", help="Notes about the edition used"
    )
    run_parser.add_argument(
        "--translation-reviewed",
        action="store_true",
        default=False,
        help="Mark translation as human-reviewed (default: False)",
    )
    run_parser.add_argument(
        "--cover-image",
        default="",
        help="Path/URL for cover image (e.g. '/bangla-library/covers/slug.jpg')",
    )
    run_parser.add_argument(
        "--back-image",
        default="",
        help="Path/URL for back cover image",
    )
    run_parser.add_argument(
        "--priority",
        type=int,
        default=2,
        help="Sort priority — lower values appear first (default: 2)",
    )
    run_parser.add_argument(
        "--publish-date",
        default="",
        help="Scheduled publish date in ISO format (e.g. '2025-07-01')",
    )
    _add_common_args(run_parser)
    run_parser.add_argument(
        "--refine",
        action="store_true",
        default=False,
        help="Enable AI refinement passes (OCR correction + translation review). Costs extra API calls.",
    )
    run_parser.add_argument(
        "--refine-model",
        default=None,
        help="Model for AI refinement passes (default: same as translate model)",
    )

    # --- extract: PDF to images only ---
    extract_parser = subparsers.add_parser(
        "extract", help="Extract PDF pages to images"
    )
    extract_parser.add_argument("pdf", help="Path to PDF file")
    extract_parser.add_argument(
        "--output", "-o", default="output", help="Output directory"
    )
    extract_parser.add_argument(
        "--dpi", type=int, default=300, help="Image DPI (default: 300)"
    )
    extract_parser.add_argument(
        "--format", default="png", help="Image format (default: png)"
    )
    extract_parser.add_argument(
        "--start", type=int, default=1, help="Start page number"
    )
    extract_parser.add_argument("--end", type=int, default=None, help="End page number")

    # --- ocr: images to text only ---
    ocr_parser = subparsers.add_parser("ocr", help="OCR page images to text files")
    ocr_parser.add_argument(
        "--output",
        "-o",
        default="output",
        help="Output directory (must contain pages/)",
    )
    ocr_parser.add_argument(
        "--ocr-model", default=None, help="Model for OCR (default: claude-sonnet-4.6)"
    )
    _add_common_args(ocr_parser)

    # --- translate: OCR text to markdown ---
    translate_parser = subparsers.add_parser(
        "translate", help="Translate OCR'd text files to markdown"
    )
    translate_parser.add_argument(
        "--output", "-o", default="output", help="Output directory (must contain ocr/)"
    )
    translate_parser.add_argument(
        "--translate-model",
        default=None,
        help="Model for translation (default: claude-sonnet-4.6)",
    )
    translate_parser.add_argument(
        "--from", dest="source_lang", default="Bengali", help="Source language"
    )
    translate_parser.add_argument(
        "--to", dest="target_lang", default="English", help="Target language"
    )
    _add_common_args(translate_parser)

    # --- combine: merge translations ---
    combine_parser = subparsers.add_parser(
        "combine", help="Combine translations into book"
    )
    combine_parser.add_argument(
        "--output", "-o", default="output", help="Output directory"
    )
    combine_parser.add_argument("--title", "-t", required=True, help="Book title")
    combine_parser.add_argument("--author", "-a", required=True, help="Book author")
    combine_parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        default=False,
        help="Enable verbose/debug logging",
    )

    # --- refine: standalone AI refinement ---
    refine_parser = subparsers.add_parser(
        "refine",
        help="Run AI refinement on existing OCR + translations (OCR correction + translation review)",
    )
    refine_parser.add_argument(
        "--output",
        "-o",
        default="output",
        help="Output directory (must contain ocr/ and translations/)",
    )
    refine_parser.add_argument(
        "--refine-model",
        default=None,
        help="Model for AI refinement (default: claude-sonnet-4.6)",
    )
    refine_parser.add_argument(
        "--skip-ocr-correction",
        action="store_true",
        default=False,
        help="Skip the OCR correction step (only refine translations)",
    )
    refine_parser.add_argument(
        "--skip-translation-review",
        action="store_true",
        default=False,
        help="Skip the translation review step (only correct OCR)",
    )
    _add_common_args(refine_parser)

    # --- export-json: export to bangla-library format ---
    export_parser = subparsers.add_parser(
        "export-json",
        help="Export translations to bangla-library JSON format",
    )
    export_parser.add_argument(
        "--output",
        "-o",
        default="output",
        help="Output directory (must contain translations/)",
    )
    export_parser.add_argument("--title-en", required=True, help="English title")
    export_parser.add_argument("--title-bn", default="", help="Bengali title")
    export_parser.add_argument("--author-en", default="", help="Author name (English)")
    export_parser.add_argument("--author-bn", default="", help="Author name (Bengali)")
    export_parser.add_argument("--year", default="", help="Publication year")
    export_parser.add_argument(
        "--category", default="Novel", help="Category (default: Novel)"
    )
    export_parser.add_argument("--description", default="", help="English description")
    export_parser.add_argument(
        "--slug",
        default=None,
        help="Output filename slug (default: derived from title)",
    )
    export_parser.add_argument(
        "--dest",
        default=None,
        help="Destination directory for JSON file (default: same as --output)",
    )
    # --- bangla-library extended fields ---
    export_parser.add_argument(
        "--author-slug",
        default="",
        help="Author slug for profile link (e.g. 'rabindranath-tagore')",
    )
    export_parser.add_argument(
        "--status",
        default="published",
        choices=["published", "unpublished"],
        help="Publication status (default: published)",
    )
    export_parser.add_argument(
        "--published-date",
        default="",
        help="ISO date when book was added (e.g. '2025-06-15'). Defaults to today.",
    )
    export_parser.add_argument(
        "--description-bn", default="", help="Bengali description"
    )
    export_parser.add_argument(
        "--copyright-notice",
        default="",
        help="Copyright notice for non-public-domain works",
    )
    export_parser.add_argument(
        "--source", default="", help="URL or reference to original PDF source"
    )
    export_parser.add_argument(
        "--original-publisher", default="", help="Original publisher name"
    )
    export_parser.add_argument(
        "--edition-note", default="", help="Notes about the edition used"
    )
    export_parser.add_argument(
        "--translation-reviewed",
        action="store_true",
        default=False,
        help="Mark translation as human-reviewed (default: False)",
    )
    export_parser.add_argument(
        "--cover-image",
        default="",
        help="Path/URL for cover image (e.g. '/bangla-library/covers/slug.jpg')",
    )
    export_parser.add_argument(
        "--back-image",
        default="",
        help="Path/URL for back cover image",
    )
    export_parser.add_argument(
        "--priority",
        type=int,
        default=2,
        help="Sort priority — lower values appear first (default: 2)",
    )
    export_parser.add_argument(
        "--publish-date",
        default="",
        help="Scheduled publish date in ISO format (e.g. '2025-07-01')",
    )
    export_parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        default=False,
        help="Enable verbose/debug logging",
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # ---- FULL PIPELINE ----
    if args.command == "run":
        pdf_path = args.pdf
        if not os.path.exists(pdf_path):
            log.error(f"PDF not found: {pdf_path}")
            sys.exit(1)

        # Load config file if provided, use as defaults
        config = {}
        if args.config:
            config = load_config(args.config)

        # Determine output dir early so we can set up log file
        derived_title, derived_author = derive_book_info(pdf_path)
        title = args.title or config.get("book_title") or derived_title
        author = args.author or config.get("author") or derived_author

        if args.output:
            output_dir = args.output
        elif config.get("output_dir"):
            output_dir = config["output_dir"]
        else:
            slug = title.lower().replace(" ", "-")
            output_dir = os.path.join("output", slug)

        # Initialize logging (with log file in output dir)
        log = setup_logging(output_dir, verbose=args.verbose)

        # Resolve backend
        ctx = _resolve_backend(args, config)

        # CLI args > config file > derived defaults
        source_lang = args.source_lang or config.get("source_language", "Bengali")
        target_lang = args.target_lang or config.get("target_language", "English")
        dpi = args.dpi or config.get("image_dpi", 300)
        start_page = args.start or config.get("start_page", 1)
        end_page = args.end or config.get("end_page")
        skip_pages = args.skip or config.get("skip_pages", [])
        delay = args.delay if args.delay != 2 else config.get("delay_between_pages", 2)
        max_retries = config.get("max_retries", 3)
        single_pass = args.single_pass
        do_refine = args.refine

        # Model resolution — default to Claude Sonnet 4.6 for both
        ocr_model = args.ocr_model or config.get("ocr_model") or "claude-sonnet-4.6"
        translate_model = (
            args.translate_model or config.get("translate_model") or "claude-sonnet-4.6"
        )
        refine_model = args.refine_model or translate_model
        if args.model:
            if single_pass:
                ocr_model = args.model
            else:
                ocr_model = args.model
                translate_model = args.model

        # Calculate total steps for step labels
        if single_pass:
            total_steps = 3  # extract, translate, combine
        else:
            # Base: extract(1) + OCR(2) + translate(3) + combine
            # Free refinement steps always run: normalize(+1), stitch(+1), dedup(+1), quality(+1)
            # AI refinement steps run only with --refine: OCR correction(+1), translation review(+1)
            total_steps = 4 + 4  # 8 base (4 core + 4 free refinement)
            if do_refine:
                total_steps += 2  # +2 for AI refinement
            # combine is the last step before bonus
            # so: extract, OCR, normalize, stitch, dedup, [ocr_correction], translate, [translation_review], quality, combine

        pipeline_start = time.time()
        log.info("=" * 60)
        log.info(f"  PDF-to-Book Pipeline Started")
        log.info(f"  Book:       {title} by {author}")
        log.info(f"  Backend:    {ctx['backend']}")
        if single_pass:
            log.info(f"  Mode:       single-pass")
            log.info(f"  Model:      {ocr_model}")
        else:
            log.info(f"  Mode:       two-pass (OCR + Translate)")
            log.info(f"  OCR model:  {ocr_model}")
            log.info(f"  Trans model:{translate_model}")
            if do_refine:
                log.info(f"  Refine:     ON (model: {refine_model})")
            else:
                log.info(f"  Refine:     OFF (use --refine to enable AI refinement)")
        log.info(f"  Languages:  {source_lang} -> {target_lang}")
        log.info(f"  Output:     {output_dir}")
        log.info(f"  Started:    {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        log.info("=" * 60)

        step_num = 0

        # Step 1: Extract
        step_num += 1
        log.info("")
        log.info(f">>> STEP {step_num}/{total_steps}: Extracting pages from PDF")
        log.info("-" * 40)
        extract_start = time.time()
        images = pdf_to_images(
            pdf_path,
            output_dir,
            dpi=dpi,
            fmt="png",
            start_page=start_page,
            end_page=end_page,
            skip_pages=skip_pages,
        )
        log.info(
            f"Extraction done in {_format_duration(time.time() - extract_start)} ({len(images)} pages)"
        )

        if single_pass:
            # Single-pass: send image directly for OCR + translation
            step_num += 1
            log.info("")
            log.info(
                f">>> STEP {step_num}/{total_steps}: Translating pages (single-pass)"
            )
            log.info("-" * 40)
            prompt = build_prompt(source_lang, target_lang)
            translations_dir = Path(output_dir) / "translations"
            translations_dir.mkdir(parents=True, exist_ok=True)

            translation_paths: list[Path] = []
            total = len(images)
            sp_start = time.time()
            for i, img_path in enumerate(images, 1):
                md_path = translations_dir / f"{img_path.stem}.md"
                if md_path.exists() and md_path.stat().st_size > 0:
                    elapsed = time.time() - sp_start
                    log.info(
                        _progress_bar(
                            i,
                            total,
                            elapsed=elapsed,
                            label=f"{img_path.name} SKIP (cached)",
                        )
                    )
                    translation_paths.append(md_path)
                    continue

                elapsed = time.time() - sp_start
                log.info(
                    _progress_bar(
                        i - 1,
                        total,
                        elapsed=elapsed,
                        label=f"{img_path.name} translating...",
                    )
                )
                page_start = time.time()
                result = translate_page_single_pass(
                    img_path,
                    prompt,
                    backend=ctx["backend"],
                    model=ocr_model,
                    max_retries=max_retries,
                    opencode_cli=ctx["opencode_cli"],
                    attach_url=ctx["attach_url"],
                    gh_cli=ctx["gh_cli"],
                )
                page_elapsed = time.time() - page_start
                if "NO_TEXT_CONTENT" in result:
                    md_path.write_text(
                        "<!-- No translatable text on this page -->\n", encoding="utf-8"
                    )
                    log.info(f"    OK (no text) in {page_elapsed:.1f}s")
                else:
                    md_path.write_text(result + "\n", encoding="utf-8")
                    log.info(
                        f"    OK: {len(result)} chars in {page_elapsed:.1f}s -> {md_path.name}"
                    )
                translation_paths.append(md_path)
                if i < total:
                    time.sleep(delay)
        else:
            # Two-pass: OCR then translate, with refinement steps interspersed

            # Step: OCR
            step_num += 1
            log.info("")
            log.info(
                f">>> STEP {step_num}/{total_steps}: OCR -- extracting text from images"
            )
            log.info("-" * 40)
            ocr_files = ocr_all_pages(
                images,
                output_dir,
                backend=ctx["backend"],
                ocr_model=ocr_model,
                delay=delay,
                max_retries=max_retries,
                opencode_cli=ctx["opencode_cli"],
                attach_url=ctx["attach_url"],
                gh_cli=ctx["gh_cli"],
            )

            # Step: Unicode normalization (FREE — always runs)
            step_num += 1
            log.info("")
            log.info(f">>> STEP {step_num}/{total_steps}: Unicode text normalization")
            log.info("-" * 40)
            norm_count = normalize_ocr_files(output_dir)
            log.info(f"Normalization done ({norm_count} files modified)")

            # Step: Cross-page stitching (FREE — always runs)
            step_num += 1
            log.info("")
            log.info(
                f">>> STEP {step_num}/{total_steps}: Cross-page continuity stitching"
            )
            log.info("-" * 40)
            ocr_dir_path = str(Path(output_dir) / "ocr")
            stitch_count, stitch_details = stitch_pages(ocr_dir_path)
            log.info(f"Stitching done ({stitch_count} fragments merged)")

            # Step: Duplicate detection (FREE — always runs)
            step_num += 1
            log.info("")
            log.info(f">>> STEP {step_num}/{total_steps}: Duplicate/overlap detection")
            log.info("-" * 40)
            duplicates = detect_duplicates(ocr_dir_path)
            if duplicates:
                log.warning(
                    f"Found {len(duplicates)} potential overlaps — review quality_report.json"
                )
            else:
                log.info("No duplicates detected")

            # Step: AI-based OCR correction (only with --refine)
            if do_refine:
                step_num += 1
                log.info("")
                log.info(
                    f">>> STEP {step_num}/{total_steps}: AI-based OCR error correction"
                )
                log.info("-" * 40)
                correct_ocr_all_pages(
                    output_dir,
                    backend=ctx["backend"],
                    model=refine_model,
                    delay=delay,
                    max_retries=max_retries,
                    opencode_cli=ctx["opencode_cli"],
                    attach_url=ctx["attach_url"],
                    gh_cli=ctx["gh_cli"],
                )

            # Step: Translate
            step_num += 1
            log.info("")
            log.info(f">>> STEP {step_num}/{total_steps}: Translating extracted text")
            log.info("-" * 40)

            # If OCR correction was done, translate from corrected files
            if do_refine:
                corrected_dir = Path(output_dir) / "ocr_corrected"
                if corrected_dir.exists():
                    ocr_files = sorted(corrected_dir.glob("page_*.txt"))

            translation_paths = translate_all_pages(
                ocr_files,
                output_dir,
                source_lang=source_lang,
                target_lang=target_lang,
                backend=ctx["backend"],
                translate_model=translate_model,
                delay=delay,
                max_retries=max_retries,
                opencode_cli=ctx["opencode_cli"],
                attach_url=ctx["attach_url"],
                gh_cli=ctx["gh_cli"],
            )

            # Step: AI-based translation refinement (only with --refine)
            if do_refine:
                step_num += 1
                log.info("")
                log.info(
                    f">>> STEP {step_num}/{total_steps}: AI-based translation refinement"
                )
                log.info("-" * 40)
                refined_paths = refine_all_translations(
                    output_dir,
                    backend=ctx["backend"],
                    model=refine_model,
                    delay=delay,
                    max_retries=max_retries,
                    opencode_cli=ctx["opencode_cli"],
                    attach_url=ctx["attach_url"],
                    gh_cli=ctx["gh_cli"],
                )
                # Use refined translations for combine step if available
                if refined_paths:
                    translation_paths = refined_paths

            # Step: Quality scoring (FREE — always runs)
            step_num += 1
            log.info("")
            log.info(f">>> STEP {step_num}/{total_steps}: Quality scoring")
            log.info("-" * 40)
            quality_report = score_all_pages(output_dir)
            # Include duplicate info in quality report
            if duplicates and quality_report:
                quality_report["duplicates"] = duplicates
                report_path = Path(output_dir) / "quality_report.json"
                with open(report_path, "w", encoding="utf-8") as f:
                    json.dump(quality_report, f, ensure_ascii=False, indent=2)

        # Final step: Combine
        step_num += 1
        log.info("")
        log.info(f">>> STEP {step_num}/{total_steps}: Combining into book")
        log.info("-" * 40)
        book_path = combine_translations(
            translation_paths,
            output_dir,
            title=title,
            author=author,
        )

        # Optional: JSON export for bangla-library
        json_path = None
        if args.export_json:
            log.info("")
            log.info(">>> BONUS STEP: Exporting to bangla-library JSON")
            log.info("-" * 40)
            json_dest = args.json_dest or output_dir

            # Resolve JSON export fields: CLI args > config file > defaults
            j_title_bn = args.title_bn or config.get("title_bn", "")
            j_author_bn = args.author_bn or config.get("author_bn", "")
            j_year = args.year or config.get("year", "")
            j_category = args.category or config.get("category", "Novel")
            j_description = args.description or config.get("description_en", "")
            j_author_slug = args.author_slug or config.get("author_slug", "")
            j_status = (
                args.status
                if args.status != "published"
                else config.get("status", "published")
            )
            j_published_date = args.published_date or config.get("published_date", "")
            j_description_bn = args.description_bn or config.get("description_bn", "")
            j_copyright = args.copyright_notice or config.get("copyright_notice", "")
            j_source = args.source or config.get("source", "")
            j_publisher = args.original_publisher or config.get(
                "original_publisher", ""
            )
            j_edition = args.edition_note or config.get("edition_note", "")
            j_reviewed = args.translation_reviewed or config.get(
                "translation_reviewed", False
            )
            j_cover_image = args.cover_image or config.get("cover_image", "")
            j_back_image = args.back_image or config.get("back_image", "")
            j_priority = (
                args.priority if args.priority != 2 else config.get("priority", 2)
            )
            j_publish_date = args.publish_date or config.get("publish_date", "")

            # Auto-fill published_date with today if not provided
            if not j_published_date:
                j_published_date = datetime.now().strftime("%Y-%m-%d")

            json_path = export_to_json(
                translation_paths,
                json_dest,
                title_en=title,
                title_bn=j_title_bn,
                author_en=author,
                author_bn=j_author_bn,
                year=j_year,
                category=j_category,
                description_en=j_description,
                author_slug=j_author_slug,
                status=j_status,
                published_date=j_published_date,
                description_bn=j_description_bn,
                copyright_notice=j_copyright,
                source=j_source,
                original_publisher=j_publisher,
                edition_note=j_edition,
                translation_reviewed=j_reviewed,
                cover_image=j_cover_image,
                back_image=j_back_image,
                priority=j_priority,
                publish_date=j_publish_date,
            )

        total_time = time.time() - pipeline_start
        log.info("")
        log.info("=" * 60)
        log.info(f"  PIPELINE COMPLETE")
        log.info(f"  Book:     {book_path}")
        if json_path:
            log.info(f"  JSON:     {json_path}")
        log.info(f"  Pages:    {len(images)}")
        log.info(f"  Duration: {_format_duration(total_time)}")
        log.info(f"  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        log.info("=" * 60)

    # ---- EXTRACT ONLY ----
    elif args.command == "extract":
        log = setup_logging(verbose=args.verbose)
        pdf_to_images(
            args.pdf,
            args.output,
            dpi=args.dpi,
            fmt=args.format,
            start_page=args.start,
            end_page=args.end,
        )

    # ---- OCR ONLY ----
    elif args.command == "ocr":
        log = setup_logging(args.output, verbose=args.verbose)
        ctx = _resolve_backend(args)

        pages_dir = Path(args.output) / "pages"
        if not pages_dir.exists():
            log.error(f"Pages directory not found: {pages_dir}")
            log.error("Run 'extract' first, or check your --output path.")
            sys.exit(1)

        images = sorted(pages_dir.glob("page_*.*"))
        if not images:
            log.error(f"No page images found in {pages_dir}")
            sys.exit(1)

        ocr_model = args.ocr_model or "claude-sonnet-4.6"
        ocr_all_pages(
            images,
            args.output,
            backend=ctx["backend"],
            ocr_model=ocr_model,
            delay=args.delay,
            opencode_cli=ctx["opencode_cli"],
            attach_url=ctx["attach_url"],
            gh_cli=ctx["gh_cli"],
        )

    # ---- TRANSLATE ONLY ----
    elif args.command == "translate":
        log = setup_logging(args.output, verbose=args.verbose)
        ctx = _resolve_backend(args)

        ocr_dir = Path(args.output) / "ocr"
        if not ocr_dir.exists():
            log.error(f"OCR directory not found: {ocr_dir}")
            log.error("Run 'ocr' first, or check your --output path.")
            sys.exit(1)

        txt_files = sorted(ocr_dir.glob("page_*.txt"))
        if not txt_files:
            log.error(f"No OCR text files found in {ocr_dir}")
            sys.exit(1)

        translate_model = args.translate_model or "claude-sonnet-4.6"
        translate_all_pages(
            txt_files,
            args.output,
            source_lang=args.source_lang,
            target_lang=args.target_lang,
            backend=ctx["backend"],
            translate_model=translate_model,
            delay=args.delay,
            opencode_cli=ctx["opencode_cli"],
            attach_url=ctx["attach_url"],
            gh_cli=ctx["gh_cli"],
        )

    # ---- COMBINE ONLY ----
    elif args.command == "combine":
        log = setup_logging(verbose=args.verbose)
        translations_dir = Path(args.output) / "translations"
        if not translations_dir.exists():
            log.error(f"Translations directory not found: {translations_dir}")
            sys.exit(1)

        md_files = sorted(translations_dir.glob("page_*.md"))
        if not md_files:
            log.error(f"No translation files found in {translations_dir}")
            sys.exit(1)

        combine_translations(md_files, args.output, args.title, args.author)

    # ---- REFINE ONLY ----
    elif args.command == "refine":
        log = setup_logging(args.output, verbose=args.verbose)
        ctx = _resolve_backend(args)

        output_dir = args.output
        refine_model = args.refine_model or "claude-sonnet-4.6"

        ocr_dir = Path(output_dir) / "ocr"
        translations_dir = Path(output_dir) / "translations"

        if not ocr_dir.exists():
            log.error(f"OCR directory not found: {ocr_dir}")
            log.error("Run 'ocr' first, or check your --output path.")
            sys.exit(1)
        if not translations_dir.exists():
            log.error(f"Translations directory not found: {translations_dir}")
            log.error("Run 'translate' first, or check your --output path.")
            sys.exit(1)

        refine_start = time.time()
        log.info("=" * 60)
        log.info(f"  AI Refinement Pipeline")
        log.info(f"  Backend:    {ctx['backend']}")
        log.info(f"  Model:      {refine_model}")
        log.info(f"  Output:     {output_dir}")
        log.info("=" * 60)

        step = 0
        skip_ocr = args.skip_ocr_correction
        skip_trans = args.skip_translation_review
        total_steps = 4  # normalize + stitch + dedup + quality (always)
        if not skip_ocr:
            total_steps += 1
        if not skip_trans:
            total_steps += 1

        # 1. Unicode normalization
        step += 1
        log.info("")
        log.info(f">>> STEP {step}/{total_steps}: Unicode text normalization")
        log.info("-" * 40)
        normalize_ocr_files(output_dir)

        # 2. Cross-page stitching
        step += 1
        log.info("")
        log.info(f">>> STEP {step}/{total_steps}: Cross-page continuity stitching")
        log.info("-" * 40)
        stitch_pages(str(ocr_dir))

        # 3. Duplicate detection
        step += 1
        log.info("")
        log.info(f">>> STEP {step}/{total_steps}: Duplicate/overlap detection")
        log.info("-" * 40)
        duplicates = detect_duplicates(str(ocr_dir))

        # 4. AI OCR correction (optional)
        if not skip_ocr:
            step += 1
            log.info("")
            log.info(f">>> STEP {step}/{total_steps}: AI-based OCR error correction")
            log.info("-" * 40)
            correct_ocr_all_pages(
                output_dir,
                backend=ctx["backend"],
                model=refine_model,
                delay=args.delay,
                opencode_cli=ctx["opencode_cli"],
                attach_url=ctx["attach_url"],
                gh_cli=ctx["gh_cli"],
            )

        # 5. AI translation refinement (optional)
        if not skip_trans:
            step += 1
            log.info("")
            log.info(f">>> STEP {step}/{total_steps}: AI-based translation refinement")
            log.info("-" * 40)
            refine_all_translations(
                output_dir,
                backend=ctx["backend"],
                model=refine_model,
                delay=args.delay,
                opencode_cli=ctx["opencode_cli"],
                attach_url=ctx["attach_url"],
                gh_cli=ctx["gh_cli"],
            )

        # 6. Quality scoring
        step += 1
        log.info("")
        log.info(f">>> STEP {step}/{total_steps}: Quality scoring")
        log.info("-" * 40)
        quality_report = score_all_pages(output_dir)
        if duplicates and quality_report:
            quality_report["duplicates"] = duplicates
            report_path = Path(output_dir) / "quality_report.json"
            with open(report_path, "w", encoding="utf-8") as f:
                json.dump(quality_report, f, ensure_ascii=False, indent=2)

        total_time = time.time() - refine_start
        log.info("")
        log.info("=" * 60)
        log.info(f"  REFINEMENT COMPLETE")
        log.info(f"  Duration: {_format_duration(total_time)}")
        log.info(f"  Report:   {Path(output_dir) / 'quality_report.json'}")
        log.info("=" * 60)

    # ---- EXPORT JSON ----
    elif args.command == "export-json":
        log = setup_logging(verbose=args.verbose)
        translations_dir = Path(args.output) / "translations"
        if not translations_dir.exists():
            log.error(f"Translations directory not found: {translations_dir}")
            log.error("Run the pipeline or 'translate' first.")
            sys.exit(1)

        md_files = sorted(translations_dir.glob("page_*.md"))
        if not md_files:
            log.error(f"No translation files found in {translations_dir}")
            sys.exit(1)

        dest_dir = args.dest or args.output

        # Auto-fill published_date with today if not provided
        pub_date = args.published_date
        if not pub_date:
            pub_date = datetime.now().strftime("%Y-%m-%d")

        export_to_json(
            md_files,
            dest_dir,
            title_en=args.title_en,
            title_bn=args.title_bn,
            author_en=args.author_en,
            author_bn=args.author_bn,
            year=args.year,
            category=args.category,
            description_en=args.description,
            slug=args.slug,
            author_slug=args.author_slug,
            status=args.status,
            published_date=pub_date,
            description_bn=args.description_bn,
            copyright_notice=args.copyright_notice,
            source=args.source,
            original_publisher=args.original_publisher,
            edition_note=args.edition_note,
            translation_reviewed=args.translation_reviewed,
            cover_image=args.cover_image,
            back_image=args.back_image,
            priority=args.priority,
            publish_date=args.publish_date,
        )


if __name__ == "__main__":
    main()
