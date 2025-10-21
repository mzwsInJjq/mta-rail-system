import sys
import json
from pathlib import Path
import argparse
import re

def build_mta_dict(path: Path, reverse: bool = False) -> dict:
    text = path.read_text(encoding="utf-8")
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    
    all_lines_data = {}

    i = 0
    while i < len(lines):
        # Match lines like '# 1: 38 stops' or 'A: 59 stops'
        match = re.match(r"#?\s*([A-Z\d]+X?):\s*(\d+)\s*stops", lines[i])
        if match:
            line_name = match.group(1)
            num_stops = int(match.group(2))
            
            # Initialize dicts for this specific line
            stop_id_to_name = {}
            name_to_index = {}

            i += 1
            if i >= len(lines):
                print(f"Error: Unexpected end of file after header for line {line_name}", file=sys.stderr)
                break
            
            try:
                stop_ids = eval(lines[i])
            except Exception as e:
                print(f"Error parsing stop_ids for line {line_name} on line {i+1}: {e}", file=sys.stderr)
                i += 2 # Skip stop_ids and stop_names
                continue
            i += 1
            if i >= len(lines):
                print(f"Error: Unexpected end of file after stop_ids for line {line_name}", file=sys.stderr)
                break

            try:
                stop_names = eval(lines[i])
            except Exception as e:
                print(f"Error parsing stop_names for line {line_name} on line {i+1}: {e}", file=sys.stderr)
                i += 1
                continue
            
            if len(stop_ids) != num_stops or len(stop_names) != num_stops:
                print(f"Warning: Mismatch in stop count for line {line_name}. Expected {num_stops}, found {len(stop_ids)} ids and {len(stop_names)} names.", file=sys.stderr)

            if reverse:
                stop_names.reverse()
                stop_ids.reverse()

            for idx, (stop_id, stop_name) in enumerate(zip(stop_ids, stop_names)):
                stop_id_to_name[stop_id] = stop_name
                name_to_index[stop_name] = idx
            
            all_lines_data[line_name] = {
                "stop_id_to_name": stop_id_to_name,
                "name_to_index": name_to_index
            }
            i += 1
        else:
            # This line is not a header, so we skip it.
            i += 1
    
    return all_lines_data


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build MTA station dictionaries from a text file.")
    parser.add_argument("file", nargs="?", default="stops.txt", help="Input file with station data (default: stops.txt)")
    parser.add_argument("--reverse", action="store_true", help="Reverse the station list for each line")
    args = parser.parse_args()

    p = Path(args.file)
    if not p.exists():
        print(f"File not found: {p}", file=sys.stderr)
        sys.exit(1)
    
    all_mta_dicts = build_mta_dict(p, reverse=args.reverse)

    # Define indentation levels
    match_indent = " " * 8
    case_indent = " " * 12
    dict_indent = " " * 16
    dict_entry_indent = " " * 20

    print(f"{match_indent}match args.route:")
    for line_name, line_dicts in sorted(all_mta_dicts.items()):
        print(f"{case_indent}case '{line_name}':")
        
        # Print stop_id_to_name dictionary
        print(f"{dict_indent}self.stop_id_to_name = {{")
        for stop_id, name in sorted(line_dicts['stop_id_to_name'].items()):
            print(f"{dict_entry_indent}{json.dumps(stop_id)}: {json.dumps(name)},")
        print(f"{dict_indent}}}")

        # Print name_to_index dictionary
        print(f"{dict_indent}self.name_to_index = {{")
        for name, index in sorted(line_dicts['name_to_index'].items(), key=lambda x: x[1], reverse=True):
            print(f"{dict_entry_indent}{json.dumps(name)}: {index},")
        print(f"{dict_indent}}}")
