# PDF-to-Book

Translate PDF books (Bengali or any language) to English using OpenCode AI. Outputs markdown files ready for Astro or any static site generator.

## Requirements

- Python 3.10+
- [OpenCode](https://opencode.ai) installed and authenticated
- PyMuPDF, Pillow (`pip install -r requirements.txt`)

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Just pass the PDF - that's it
python pdf_to_book.py run Moyurakkhi-By-Humayun-Ahmed.pdf
```

Title and author are auto-detected from the filename (`Title-By-Author.pdf` pattern).

## Usage

### Full pipeline (simplest)
```bash
# Auto-detect title/author from filename
python pdf_to_book.py run book.pdf

# Specify a model
python pdf_to_book.py run book.pdf -m github-copilot/claude-sonnet-4

# Specify languages, title, author
python pdf_to_book.py run book.pdf --from Bengali --to English -t "My Book" -a "Author"

# Process only pages 5-20
python pdf_to_book.py run book.pdf --start 5 --end 20

# Skip blank pages
python pdf_to_book.py run book.pdf --skip 1 2 70 71

# Use a config file for advanced options
python pdf_to_book.py run book.pdf --config config.json
```

### Extract pages only (PDF to images)
```bash
python pdf_to_book.py extract book.pdf --output output/ --dpi 300
```

### Translate pages only (images to markdown)
```bash
python pdf_to_book.py translate --output output/ --from Bengali --to English
```

### Combine translations only
```bash
python pdf_to_book.py combine --output output/ --title "My Book" --author "Author Name"
```

## Model Selection

Use `--model` / `-m` to pick which AI model OpenCode uses. Examples:

```bash
# Use Claude Sonnet (recommended for quality + speed balance)
python pdf_to_book.py run book.pdf -m github-copilot/claude-sonnet-4

# Use default model (whatever OpenCode is configured with)
python pdf_to_book.py run book.pdf
```

If no model is specified, OpenCode uses whatever model is configured in your session.

## Config File (Optional)

For advanced options, use `config.example.json` as a template. CLI flags override config values.

| Field | Description |
|-------|-------------|
| `book_title` | Title of the book |
| `author` | Author name |
| `source_language` | Language of the PDF (e.g. "Bengali") |
| `target_language` | Language to translate to (e.g. "English") |
| `start_page` / `end_page` | Page range to process (null = all) |
| `skip_pages` | List of page numbers to skip |
| `delay_between_pages` | Seconds to wait between API calls |
| `opencode_model` | Specific model to use (null = default) |

## Resume Support

If the script is interrupted, just run it again. Already-translated pages are skipped automatically.

## Output Structure

```
output/moyurakkhi/
  pages/
    page_0001.png
    page_0002.png
    ...
  translations/
    page_0001.md
    page_0002.md
    ...
  full_book.md      <- combined, with Astro frontmatter
```

## Using with Astro

The `full_book.md` file includes frontmatter and can be dropped directly into your Astro content collection:

```bash
cp output/moyurakkhi/full_book.md ../my-astro-site/src/content/books/moyurakkhi.md
```
