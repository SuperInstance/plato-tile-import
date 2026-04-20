"""Tile import/export across multiple formats."""

import json, csv, re, os

class TileImporter:
    def import_json(self, path: str) -> list[dict]:
        with open(path) as f:
            data = json.load(f)
        return data if isinstance(data, list) else [data]

    def import_jsonl(self, path: str) -> list[dict]:
        tiles = []
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line:
                    tiles.append(json.loads(line))
        return tiles

    def import_csv(self, path: str, content_col: str = "content",
                   domain_col: str = "domain", conf_col: str = "confidence") -> list[dict]:
        tiles = []
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                tile = {"content": row.get(content_col, ""), "domain": row.get(domain_col, ""),
                        "confidence": float(row.get(conf_col, 0.5))}
                tiles.append(tile)
        return tiles

    def import_markdown(self, path: str) -> list[dict]:
        with open(path) as f:
            text = f.read()
        tiles = []
        for heading in re.finditer(r"^#+\s+(.+)$", text, re.MULTILINE):
            tiles.append({"content": heading.group(1), "domain": "heading"})
        for line in text.split("\n"):
            line = line.strip()
            if line and not line.startswith("#") and not line.startswith("```"):
                tiles.append({"content": line, "domain": "text"})
        return tiles

    def import_plaintext(self, path: str) -> list[dict]:
        with open(path) as f:
            text = f.read()
        tiles = []
        for para in text.split("\n\n"):
            para = para.strip()
            if para:
                tiles.append({"content": para, "domain": "text"})
        return tiles

    def export_json(self, tiles: list[dict], path: str):
        with open(path, "w") as f:
            json.dump(tiles, f, indent=2)

    def export_jsonl(self, tiles: list[dict], path: str):
        with open(path, "w") as f:
            for t in tiles:
                f.write(json.dumps({k: v for k, v in t.items() if not k.startswith("_")}) + "\n")

    def export_csv(self, tiles: list[dict], path: str):
        if not tiles: return
        keys = ["content", "domain", "confidence"]
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
            writer.writeheader()
            for t in tiles:
                writer.writerow({k: t.get(k, "") for k in keys})

    def detect_format(self, path: str) -> str:
        ext = os.path.splitext(path)[1].lower()
        if ext == ".jsonl": return "jsonl"
        if ext == ".json": return "json"
        if ext == ".csv": return "csv"
        if ext in (".md", ".markdown"): return "markdown"
        return "plaintext"

    def auto_import(self, path: str) -> list[dict]:
        fmt = self.detect_format(path)
        return {"json": self.import_json, "jsonl": self.import_jsonl,
                "csv": self.import_csv, "markdown": self.import_markdown,
                "plaintext": self.import_plaintext}[fmt](path)
