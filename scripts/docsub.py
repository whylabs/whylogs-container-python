import os
import re
import sys
import textwrap
import time
from typing import Dict, List, Optional, Tuple

COMMENT_SYMBOLS = {
    "py": "#",
    "md": "<!--",
    "mdx": "<!--",
    "yaml": "#",
    "yml": "#",
    "json": "//",
    "sh": "#",
    "env": "#",
    "Dockerfile": "#",
    "txt": "#",
}

IGNORED_DIRS = {".git", "venv", ".venv", "__pycache__", ".devcontainer"}


def unindent_code_snippet(snippet: str) -> str:
    """Dedent the snippet and strip any trailing newlines."""
    return textwrap.dedent(snippet).rstrip("\n")


def get_file_extension(file_name: str) -> str:
    base_name = os.path.basename(file_name)
    if base_name == "Dockerfile" or base_name.startswith("Dockerfile."):
        return "Dockerfile"
    _, ext = os.path.splitext(base_name)
    return ext.lstrip(".").lower()


def parse_source_file(source_file: str, comment_symbol: str) -> Dict[str, str]:
    """Parse the source file and return a dictionary with id as key and code block as value."""
    snippets: Dict[str, str] = {}

    pattern_start: re.Pattern[str] = re.compile(rf"^\s*{re.escape(comment_symbol)}\s+DOCSUB_START\s+(.+?)\b.*")
    pattern_end: re.Pattern[str] = re.compile(rf"^\s*{re.escape(comment_symbol)}\s+DOCSUB_END\b.*")

    with open(source_file, "r", encoding="utf-8") as file:
        lines = file.readlines()

    snippet_id: Optional[str] = None
    code_lines: List[str] = []
    collecting = False

    for line in lines:
        if collecting:
            if pattern_end.match(line):
                snippet = "".join(code_lines)
                assert snippet_id is not None  # Ensure snippet_id is not None
                snippets[snippet_id] = unindent_code_snippet(snippet)
                code_lines = []
                collecting = False
                snippet_id = None
            else:
                code_lines.append(line)
        else:
            match_start = pattern_start.match(line)
            if match_start:
                group1 = match_start.group(1)
                if group1 is not None:
                    snippet_id = group1.strip()
                    collecting = True
                    code_lines = []
                else:
                    print(f"Warning: No snippet ID found in {source_file}")
    return snippets


def process_template_content(content: str, snippets: Dict[str, str], comment_symbol: str) -> str:
    """Process the template content, replacing markers with code snippets."""
    insert_pattern_str = rf"{re.escape(comment_symbol)}\s+DOCSUB_INSERT\s+(.+?)\b.*"
    insert_pattern: re.Pattern[str] = re.compile(insert_pattern_str)

    def replace_func(match: re.Match[str]) -> str:
        group1 = match.group(1)
        if group1 is None:
            raise ValueError("No snippet ID found in match")
        snippet_id = group1.strip()
        if snippet_id in snippets:
            return snippets[snippet_id]
        else:
            raise KeyError(f"Snippet '{snippet_id}' not found.")

    return insert_pattern.sub(replace_func, content)


def render_template(
    template_file: str,
    output_file: str,
    snippets: Dict[str, str],
    comment_symbol: str,
    check_mode: bool = False,
) -> None:
    """Render the template with code snippets inserted and save to output_file."""
    with open(template_file, "r", encoding="utf-8") as file:
        content = file.read()

    try:
        processed_content = process_template_content(content, snippets, comment_symbol)
    except KeyError as e:
        print(f"Error processing {template_file}: {e}")
        return

    if check_mode:
        if check_for_changes(output_file, processed_content):
            print(f"Changes detected in {output_file}. Update needed.")
            sys.exit(1)
        else:
            print(f"No changes needed for {output_file}.")
    else:
        with open(output_file, "w", encoding="utf-8") as file:
            file.write(processed_content)


def check_for_changes(output_file: str, processed_content: str) -> bool:
    """Check if the output file content matches the processed content."""
    if os.path.exists(output_file):
        with open(output_file, "r", encoding="utf-8") as file:
            existing_content = file.read()
        return existing_content != processed_content
    return True


def find_files(base_dir: str) -> Tuple[List[str], List[str]]:
    """Find source files and template files in base_dir."""
    source_extensions = {f".{ext}" for ext in COMMENT_SYMBOLS.keys()}
    template_extensions = {".sub"}

    source_files: List[str] = []
    template_files: List[str] = []

    for root, dirs, files in os.walk(base_dir):
        dirs[:] = [d for d in dirs if d not in IGNORED_DIRS]
        for file in files:
            full_path = os.path.join(root, file)
            base_name = os.path.basename(file)
            if base_name == "Dockerfile" or base_name.startswith("Dockerfile."):
                source_files.append(full_path)
            elif os.path.splitext(file)[1] in source_extensions:
                source_files.append(full_path)
            elif os.path.splitext(file)[1] in template_extensions:
                template_files.append(full_path)
    return source_files, template_files


def get_snippets(source_files: List[str]) -> Dict[str, str]:
    """Get all the code snippets from the source files."""
    snippets_list = map(extract_snippets_from_file, source_files)
    snippets: Dict[str, str] = {}
    for file_snippets in snippets_list:
        snippets.update(file_snippets)
    return snippets


def extract_snippets_from_file(source_file: str) -> Dict[str, str]:
    """Extract snippets from a single source file."""
    file_extension = get_file_extension(source_file)
    comment_symbol = COMMENT_SYMBOLS.get(file_extension)
    if not comment_symbol:
        print(f"Warning: Comment symbol not found for extension '{file_extension}' in file '{source_file}'")
        return {}
    try:
        return parse_source_file(source_file, comment_symbol)
    except UnicodeDecodeError:
        print(f"Error reading {source_file}: UnicodeDecodeError")
        return {}


def process_templates(template_files: List[str], snippets: Dict[str, str], check_mode: bool) -> None:
    """Process all template files."""
    for template_file in template_files:
        output_file = template_file.rsplit(".sub", 1)[0]
        file_extension = get_file_extension(output_file)
        comment_symbol = COMMENT_SYMBOLS.get(file_extension)
        if not comment_symbol:
            print(f"Warning: Comment symbol not found for extension '{file_extension}' in file '{template_file}'")
            continue
        render_template(template_file, output_file, snippets, comment_symbol, check_mode)


def main():
    start_time = time.perf_counter()

    base_dir = "."

    args = sys.argv[1:]
    check_mode = "--check" in args
    snippets_mode = "--snippets" in args

    source_files, template_files = find_files(base_dir)
    snippets = get_snippets(source_files)

    if snippets_mode:
        for k, v in snippets.items():
            print(f"----- {k} -----\n{v}\n")
        sys.exit(0)

    process_templates(template_files, snippets, check_mode)

    elapsed_time = time.perf_counter() - start_time
    print(f"docsub time: {elapsed_time:.2f} seconds")


if __name__ == "__main__":
    main()
