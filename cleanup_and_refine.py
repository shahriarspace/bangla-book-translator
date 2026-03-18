#!/usr/bin/env python3
"""
Cleanup and AI-refine a pre-exported Tin Goyenda JSON file.
Steps:
  1. Remove garbage paragraphs (front matter, page headers, OCR noise)
  2. Insert story-title markers at story boundaries
  3. Batch AI refinement via gh copilot (OCR correction + translation)
  4. Output clean JSON matching bangla-library schema
"""

import json
import re
import sys
import os
import subprocess
import time
import argparse
import datetime

# Force unbuffered output
os.environ["PYTHONUNBUFFERED"] = "1"

LOG_FILE = None


def log(msg: str):
    """Print with timestamp and flush immediately."""
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    sys.stdout.write(line + "\n")
    sys.stdout.flush()
    if LOG_FILE:
        LOG_FILE.write(line + "\n")
        LOG_FILE.flush()


INPUT_FILE = (
    r"vol-029--arek-frankenstein,-maya-jal,-saikate-sabdhan-(bdebooks.com).json"
)
OUTPUT_FILE = r"tin-goyenda-vol-29.json"

# Story boundaries (from analysis)
STORY_BOUNDARIES = [
    {
        "start": 13,
        "end": 820,
        "title_bn": "আরেক ফ্রাঙ্কেনস্টাইন",
        "title_en": "Another Frankenstein",
    },
    {"start": 823, "end": 1696, "title_bn": "মায়াজাল", "title_en": "Web of Illusion"},
    {
        "start": 1700,
        "end": 2706,
        "title_bn": "সৈকতে সাবধান",
        "title_en": "Caution at the Beach",
    },
]

# Page header patterns to remove
PAGE_HEADER_PATTERNS = [
    re.compile(r"^আরেক ফ্রাঙ্কেনস্টাইন\s*\d*$"),
    re.compile(r"^মায়াজাল\s*\d*$"),
    re.compile(r"^সৈকতে সাবধান\s*\d*$"),
    re.compile(r"^\d+\s*ভলিউম\s*\d+$"),
    re.compile(r"^ভলিউম\s*\d+\s*\d*$"),
    re.compile(r"^\d+\s*₹?\s*[লে]?উম\s*\d+$"),  # OCR variants of "ভলিউম"
    re.compile(r"^OY\s+ভলিউম\s*\d+$"),
    re.compile(r"^Ur\s+ভলিউম\s*\d+$"),
]

# Pure garbage patterns
GARBAGE_PATTERNS = [
    re.compile(
        r"^[A-Za-z\s\d\.\,\!\?\-\+\|\>\<\;\:\(\)\[\]\/\\]+$"
    ),  # Pure English/ASCII with no Bengali
    re.compile(r"^[\|\>\<\+\-\.\,\s]+$"),  # Punctuation-only
    re.compile(r"^[0-9\s\.\-\/\,]+$"),  # Numbers-only
    re.compile(r"^.{0,3}$"),  # 3 chars or less
]


def is_garbage(bn_text: str, en_text: str) -> bool:
    """Check if a paragraph is garbage that should be removed."""
    bn = bn_text.strip()
    en = en_text.strip()

    # Empty
    if not bn:
        return True

    # Check page header patterns
    for pat in PAGE_HEADER_PATTERNS:
        if pat.match(bn):
            return True

    # Pure garbage patterns on bn text
    for pat in GARBAGE_PATTERNS:
        if pat.match(bn):
            return True

    # Very short Bengali with mostly ASCII
    bengali_chars = len(re.findall(r"[\u0980-\u09FF]", bn))
    if bengali_chars < 5 and len(bn) > 3:
        ascii_chars = len(re.findall(r"[A-Za-z]", bn))
        if ascii_chars > bengali_chars:
            return True

    return False


def has_significant_ocr_noise(bn_text: str) -> bool:
    """Check if Bengali text has significant OCR noise (random English mixed in)."""
    # Count Bengali vs ASCII characters
    bengali_chars = len(re.findall(r"[\u0980-\u09FF]", bn_text))
    ascii_upper = len(re.findall(r"[A-Z]", bn_text))

    if bengali_chars == 0:
        return True

    # If >15% of content is uppercase ASCII, likely OCR noise
    noise_ratio = ascii_upper / (bengali_chars + ascii_upper)
    return noise_ratio > 0.15


def clean_paragraphs(paragraphs: list) -> list:
    """Remove garbage paragraphs and return clean list."""
    # IDs to explicitly remove
    explicit_remove = set(range(1, 13))  # Front matter (1-12)
    explicit_remove.update([821, 822])  # Transition garbage between story 1-2
    explicit_remove.update([1697, 1698, 1699])  # Transition garbage between story 2-3
    explicit_remove.add(2707)  # Trailing garbage

    cleaned = []
    for p in paragraphs:
        pid = p["id"]

        # Explicitly remove known garbage
        if pid in explicit_remove:
            continue

        # Check if it's a story within our boundaries
        in_story = any(s["start"] <= pid <= s["end"] for s in STORY_BOUNDARIES)
        if not in_story:
            continue

        # Check garbage patterns
        if is_garbage(p["bn"], p["en"]):
            continue

        cleaned.append(p)

    return cleaned


def add_story_markers(paragraphs: list) -> list:
    """Insert story title paragraphs at story boundaries."""
    result = []
    story_idx = 0

    for p in paragraphs:
        # Check if this paragraph starts a new story
        if story_idx < len(STORY_BOUNDARIES):
            boundary = STORY_BOUNDARIES[story_idx]
            if p["id"] >= boundary["start"]:
                # Don't insert if the paragraph itself IS the title
                if p["bn"].strip() != boundary["title_bn"]:
                    # Insert a story title marker
                    result.append(
                        {
                            "id": -1,  # Will be re-numbered later
                            "bn": f"--- {boundary['title_bn']} ---",
                            "en": f"--- {boundary['title_en']} ---",
                            "_marker": True,
                        }
                    )
                else:
                    # Replace the standalone title with a formatted marker
                    result.append(
                        {
                            "id": -1,
                            "bn": f"--- {boundary['title_bn']} ---",
                            "en": f"--- {boundary['title_en']} ---",
                            "_marker": True,
                        }
                    )
                    story_idx += 1
                    continue
                story_idx += 1

        result.append(p)

    return result


def renumber_paragraphs(paragraphs: list) -> list:
    """Re-number paragraph IDs sequentially starting from 1."""
    result = []
    for i, p in enumerate(paragraphs, 1):
        new_p = {"id": i, "bn": p["bn"], "en": p["en"]}
        result.append(new_p)
    return result


def call_gh_copilot(prompt: str, model: str = "claude-haiku-4.5") -> str:
    """Call gh copilot CLI. Passes prompt via -p flag directly (no shell)."""
    gh_path = r"C:\Program Files\GitHub CLI\gh.exe"
    cmd = [
        gh_path,
        "copilot",
        "-s",
        "-p",
        prompt,
        "--model",
        model,
        "--no-ask-user",
        "--allow-all",
    ]

    t0 = time.time()
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=60, encoding="utf-8"
        )
        elapsed = time.time() - t0
        out = result.stdout.strip()
        log(
            f"    gh copilot returned {len(out)} chars in {elapsed:.1f}s (exit={result.returncode})"
        )
        if result.returncode != 0 and result.stderr:
            log(f"    STDERR: {result.stderr[:200]}")
        return out
    except subprocess.TimeoutExpired:
        elapsed = time.time() - t0
        log(f"    TIMEOUT after {elapsed:.1f}s — skipping batch")
        return ""
    except Exception as e:
        log(f"    Error calling gh copilot: {e}")
        return ""


def refine_batch(
    batch: list, batch_num: int, total_batches: int, model: str = "claude-haiku-4.5"
) -> list:
    """Refine a batch of paragraphs using AI."""

    # Build a compact prompt — must stay under ~7000 chars to fit Windows cmd limits
    paragraphs_text = json.dumps(batch, ensure_ascii=False, separators=(",", ":"))

    prompt = f"""Fix OCR'd Bengali text and retranslate to English. This is from "তিন গোয়েন্দা" (Three Investigators), a Bengali detective series for teens.

Bengali fixes: remove random English letters mixed in (TAD,oa,WIT,ee,ret etc), fix garbled Bengali chars. Do NOT add new content.
English fixes: retranslate the corrected Bengali into natural English.

Return ONLY a valid JSON array [{{"id":N,"bn":"...","en":"..."}},...]. No markdown fences, no explanation.

{paragraphs_text}"""

    log(
        f"  Batch {batch_num}/{total_batches} ({len(batch)} paras, prompt={len(prompt)} chars) — calling AI..."
    )

    if len(prompt) > 7500:
        log(
            f"  Batch {batch_num}: SKIP — prompt too long ({len(prompt)} chars), keeping originals"
        )
        return batch

    response = call_gh_copilot(prompt, model)

    if not response:
        log(f"  Batch {batch_num}: EMPTY response, keeping originals")
        return batch

    # Try to parse JSON from response
    try:
        # Try direct parse
        refined = json.loads(response)
        if isinstance(refined, list) and len(refined) == len(batch):
            log(f"  Batch {batch_num}: OK — refined {len(refined)} paragraphs")
            return refined
        elif isinstance(refined, list):
            log(
                f"  Batch {batch_num}: WARN — got {len(refined)} items, expected {len(batch)}"
            )
    except json.JSONDecodeError:
        pass

    # Try to extract JSON array from response
    try:
        match = re.search(r"\[.*\]", response, re.DOTALL)
        if match:
            refined = json.loads(match.group())
            if isinstance(refined, list) and len(refined) == len(batch):
                log(
                    f"  Batch {batch_num}: OK (extracted) — refined {len(refined)} paragraphs"
                )
                return refined
    except (json.JSONDecodeError, AttributeError):
        pass

    log(
        f"  Batch {batch_num}: FAILED to parse — keeping originals (response[0:100]: {response[:100]})"
    )
    return batch


def main():
    parser = argparse.ArgumentParser(description="Clean and refine Tin Goyenda JSON")
    parser.add_argument(
        "--skip-refine", action="store_true", help="Skip AI refinement (just clean)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5,
        help="Paragraphs per AI batch (default: 5)",
    )
    parser.add_argument(
        "--model", default="claude-haiku-4.5", help="AI model for refinement"
    )
    parser.add_argument(
        "--delay", type=float, default=3, help="Delay between AI calls in seconds"
    )
    parser.add_argument(
        "--start-batch",
        type=int,
        default=1,
        help="Start from this batch number (for resuming)",
    )
    parser.add_argument("--input", default=INPUT_FILE, help="Input JSON file")
    parser.add_argument("--output", default=OUTPUT_FILE, help="Output JSON file")
    args = parser.parse_args()

    global LOG_FILE
    log_path = args.output.replace(".json", ".log")
    LOG_FILE = open(log_path, "a", encoding="utf-8")
    log(f"=== Starting cleanup_and_refine ===")
    log(f"  Log file: {log_path}")

    # Load input
    log(f"Loading {args.input}...")
    with open(args.input, "r", encoding="utf-8") as f:
        data = json.load(f)

    paragraphs = data["paragraphs"]
    log(f"  Loaded {len(paragraphs)} paragraphs")

    # Step 1: Clean garbage
    log("--- Step 1: Removing garbage paragraphs ---")
    cleaned = clean_paragraphs(paragraphs)
    log(f"  Removed {len(paragraphs) - len(cleaned)} garbage paragraphs")
    log(f"  Remaining: {len(cleaned)} paragraphs")

    # Step 2: Add story markers
    log("--- Step 2: Adding story markers ---")
    with_markers = add_story_markers(cleaned)
    # Remove _marker keys
    for p in with_markers:
        p.pop("_marker", None)
    log(f"  Total with markers: {len(with_markers)} paragraphs")

    # Step 3: AI refinement (optional)
    if not args.skip_refine:
        log(
            f"--- Step 3: AI refinement (batch_size={args.batch_size}, model={args.model}) ---"
        )

        # Check for existing progress file
        progress_file = args.output + ".progress"
        refined_all = []

        if os.path.exists(progress_file) and args.start_batch > 1:
            log(f"  Loading progress from {progress_file}...")
            with open(progress_file, "r", encoding="utf-8") as f:
                refined_all = json.load(f)
            log(f"  Loaded {len(refined_all)} already-refined paragraphs")

        total_batches = (len(with_markers) + args.batch_size - 1) // args.batch_size
        start_idx = (args.start_batch - 1) * args.batch_size

        successes = 0
        failures = 0
        t_start = time.time()

        for i in range(start_idx, len(with_markers), args.batch_size):
            batch = with_markers[i : i + args.batch_size]
            batch_num = i // args.batch_size + 1

            if batch_num < args.start_batch:
                refined_all.extend(batch)
                continue

            # Skip story markers from refinement (keep as-is)
            needs_refine = []
            skip_indices = []
            for j, p in enumerate(batch):
                if p["bn"].startswith("---") and p["bn"].endswith("---"):
                    skip_indices.append(j)
                else:
                    needs_refine.append(p)

            if needs_refine:
                refined_batch = refine_batch(
                    needs_refine, batch_num, total_batches, args.model
                )

                if refined_batch != needs_refine:
                    successes += 1
                else:
                    failures += 1

                # Reconstruct batch with markers in place
                result = []
                refine_idx = 0
                for j in range(len(batch)):
                    if j in skip_indices:
                        result.append(batch[j])
                    else:
                        if refine_idx < len(refined_batch):
                            result.append(refined_batch[refine_idx])
                            refine_idx += 1
                        else:
                            result.append(batch[j])
                refined_all.extend(result)
            else:
                refined_all.extend(batch)

            # ETA calculation
            elapsed = time.time() - t_start
            done = batch_num - (args.start_batch - 1)
            remaining = total_batches - batch_num
            if done > 0:
                avg_per_batch = elapsed / done
                eta_secs = remaining * avg_per_batch
                eta_min = int(eta_secs // 60)
                eta_sec = int(eta_secs % 60)
                log(
                    f"  Progress: {batch_num}/{total_batches} | OK={successes} FAIL={failures} | ETA: {eta_min}m{eta_sec}s"
                )

            # Save progress every 5 batches
            if batch_num % 5 == 0:
                log(f"  Saving progress ({len(refined_all)} paragraphs)...")
                with open(progress_file, "w", encoding="utf-8") as f:
                    json.dump(refined_all, f, ensure_ascii=False, indent=2)

            if i + args.batch_size < len(with_markers):
                time.sleep(args.delay)

        with_markers = refined_all

        total_elapsed = time.time() - t_start
        log(
            f"  Refinement complete: {successes} OK, {failures} failed, {total_elapsed:.0f}s total"
        )

        # Clean up progress file
        if os.path.exists(progress_file):
            os.remove(progress_file)
    else:
        log("--- Step 3: Skipped AI refinement ---")

    # Step 4: Renumber and build output
    log("--- Step 4: Building output ---")
    final_paragraphs = renumber_paragraphs(with_markers)
    log(f"  Final paragraph count: {len(final_paragraphs)}")

    output = {
        "title_bn": "তিন গোয়েন্দা ভলিউম ২৯",
        "title_en": "Tin Goyenda Volume 29",
        "author_bn": "রকিব হাসান",
        "author_en": "Rokib Hasan",
        "author_slug": "rokib-hasan",
        "year": "1998",
        "published_date": "2026-03-18",
        "status": "published",
        "category": "Detective Fiction",
        "description_en": "Volume 29 of the beloved Tin Goyenda (Three Detectives) series, featuring three thrilling mysteries: 'Another Frankenstein' — a chilling encounter with a mad scientist's creation; 'Web of Illusion' — a case of deception and hidden motives; and 'Caution at the Beach' — danger lurks beneath the sun and sand. Based on the classic 'The Three Investigators' series, adapted for Bengali readers.",
        "description_bn": "জনপ্রিয় তিন গোয়েন্দা সিরিজের ২৯তম ভলিউম। তিনটি রোমাঞ্চকর রহস্য: 'আরেক ফ্রাঙ্কেনস্টাইন' — এক পাগল বিজ্ঞানীর সৃষ্টির সাথে ভয়ংকর মুখোমুখি; 'মায়াজাল' — প্রতারণা আর গোপন উদ্দেশ্যের জাল; এবং 'সৈকতে সাবধান' — রোদ আর বালির আড়ালে লুকিয়ে আছে বিপদ।",
        "copyright_notice": "This work may be under copyright. This translation is provided for educational and archival purposes only.",
        "original_publisher": "সেবা প্রকাশনী",
        "source": "bdebooks.com",
        "cover_image": "",
        "fun_facts": [],
        "paragraphs": final_paragraphs,
    }

    log(f"Writing {args.output}...")
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    # Auto-create author stub
    author_slug = output.get("author_slug", "")
    author_en = output.get("author_en", "")
    author_bn = output.get("author_bn", "")
    if author_en and author_slug:
        out_dir = os.path.dirname(os.path.abspath(args.output)) or "."
        authors_dir = os.path.join(out_dir, "authors")
        os.makedirs(authors_dir, exist_ok=True)
        author_path = os.path.join(authors_dir, f"{author_slug}.json")
        if not os.path.exists(author_path):
            author_data = {
                "name_bn": author_bn,
                "name_en": author_en,
                "nationality": "Bangladeshi",
                "genres": ["Detective Fiction", "Thriller"],
                "awards": [],
                "bio_en": "",
                "bio_bn": "",
                "facts": [],
            }
            with open(author_path, "w", encoding="utf-8") as f:
                json.dump(author_data, f, ensure_ascii=False, indent=2)
            log(f"  Auto-created author stub: {author_path}")
            log(f"  -> Edit to add bio, facts, image_url, etc.")
        else:
            log(f"  Author already exists: {author_path}")

    log(f"Done! Output: {args.output}")
    log(f"  Paragraphs: {len(final_paragraphs)}")


if __name__ == "__main__":
    main()
