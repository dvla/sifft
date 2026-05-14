"""Configuration for file processing defaults and supported file types."""

EXTENSION_DEFAULTS = {
    ".csv": {"delimiter": ","},
    ".txt": {"delimiter": ","},
    ".out": {"delimiter": "|"},
    ".dat": {"delimiter": "|"},
    ".tsv": {"delimiter": "\t"},
    ".eot": {"delimiter": ","},
}

NO_EXTENSION_DEFAULTS = {"delimiter": ",", "header": "auto"}

EXCEL_EXTENSIONS = {".xlsx", ".xls"}
TEXT_EXTENSIONS = {".csv", ".txt", ".out", ".dat", ".tsv", ".eot"}

MAX_CORRUPT_RECORDS_PERCENT = 0.1
