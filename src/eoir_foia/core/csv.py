"""CSV processing operations."""

import csv
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from ..settings import JSON_DIR


@dataclass
class RowModification:
    """Track modifications made to rows during cleaning"""

    row_number: int
    modification_type: str  # 'truncate', 'pad', 'clean_value'
    column_affected: Optional[str]
    original_value: str
    new_value: str
    reason: str


# File-specific validation rules based on investigation findings
FILE_SPECIFIC_RULES = {
    "tbl_Court_Motions.csv": {
        "auto_truncate": True,
        "expected_extra_cols": 2,
        "skip_validation_cols": [
            # Only truly 100% null columns based on database analysis
            "REJ",  # 100.00% null
            "DATE_TO_BIA",  # 100.00% null
            "DECISION_RENDERED",  # 100.00% null
            "DATE_MAILED_TO_IJ",  # 100.00% null
            "DATE_RECD_FROM_BIA",  # 100.00% null
            "STRDJSCENARIO",  # 100.00% null
            "E_28_RECPTFLAG",  # 100.00% null
            # Note: Removed DATMOTIONDUE (93.99% null but 417K rows have data)
            # Note: Removed STRCERTOFSERVICECODE (17.87% null but 5.7M rows have data)
            # Note: Removed STRFILINGMETHOD (5.98% null but 6.5M rows have data)
            # Note: Removed STRFILINGPARTY (3.89% null but 6.7M rows have data)
        ],
    },
    "tblProBono.csv": {
        "high_nul_tolerance": True,
        "skip_validation_cols": [
            "WD_DEC",
            "strA1",
            "strA2",
            "strA3",
            "strPossibility",
            "strIntrprLang",
            "blnProcessed",
            "other_comp",
            "DEC_212C",
            "recd_212C",
            "blnOARequestedbyINS",
            "Other_dec2",
            "Charge_5",
            "blnOARequestedbyAlien",
            "DEC_245",
            "recd_245",
            "Charge_4",
            "blnIntrpr",
            "Charge_6",
            "WD_recd",
            "blnOARequestedbyAmicus",
        ],
    },
    "A_TblCase.csv": {
        "skip_validation_cols": [
            # These are confirmed to be 99.94% to 100% null (legacy/unused fields)
            "UP_BOND_DATE",  # 100.00% null
            "UP_BOND_RSN",  # 100.00% null
            "ZBOND_MRG_FLAG",  # 99.95% null
            "DETENTION_DATE",  # 100.00% null
            "DETENTION_LOCATION",  # 99.99% null
            "DCO_LOCATION",  # 99.99% null
            "DETENTION_FACILITY_TYPE",  # 100.00% null
            "LPR",  # 99.94% null
        ]
    },
}


class CsvValidator:
    def __init__(self, csvfile) -> None:
        """
        Initialize CsvValidator with file paths and configuration.
        Enhanced for Step 2 with validation and tracking.
        """
        self.csvfile = csvfile
        self.header = self.get_header()
        self.header_length = len(self.header)
        self.name = os.path.basename(self.csvfile)
        self.no_nul = os.path.abspath(self.csvfile).replace(".csv", "_no_nul.csv")

        # Step 1 counters
        self.row_count = 0
        self.empty_pk = 0

        # Step 2 enhancements
        self.bad_rows = []  # Track problematic rows
        self.modifications = []  # Track all changes made
        self.column_stats = {}  # Per-column quality metrics
        self.validation_rules = {}  # File-specific rules
        self.empty_primary_keys = []  # Track rows with empty primary keys

        # Primary key information
        self.primary_key_column = self.header[0] if self.header else None

        # Load table configuration
        try:
            with open(os.path.join(JSON_DIR, "tables.json"), "r") as f:
                self.table = json.load(f)[self.name]
            with open(
                os.path.join(
                    JSON_DIR, "table-dtypes", f"{self.name.replace('.csv', '.json')}"
                ),
                "r",
            ) as f:
                self.dtypes = json.load(f)
        except FileNotFoundError as e:
            print(f"Need to setup json file for table. {e}")
            self.table = None
            self.dtypes = {}

        # Load file-specific validation rules
        self.validation_rules = FILE_SPECIFIC_RULES.get(self.name, {})

    def get_header(self) -> list:
        """
        Return first line of csv file.
        """
        with open(
            self.csvfile, "r", newline="", encoding="latin-1", errors="replace"
        ) as f:
            reader = csv.reader(
                f,
                delimiter="\t",
                quoting=csv.QUOTE_NONE,
                escapechar="\\",
            )
            return next(reader)

    def validate_and_process_rows(self, skip_header=True) -> list:
        """
        Enhanced CSV generator with Step 2 validation and tracking.
        Intelligently handles row length issues and tracks bad values.
        """
        # Use no_nul file if it exists, otherwise use original
        file_to_read = self.no_nul if os.path.exists(self.no_nul) else self.csvfile

        with open(
            file_to_read, "r", newline="", encoding="latin-1", errors="replace"
        ) as f:
            for i, row in enumerate(
                csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
            ):
                if not row:
                    continue
                elif i == 0 and skip_header:
                    continue  # skip header row

                # Step 2: Enhanced processing
                row_number = i if skip_header else i + 1

                # 1. Validate primary key first
                pk_issue = self.validate_primary_key(row, row_number)
                if pk_issue:
                    self.empty_primary_keys.append(row_number)
                    self.empty_pk += 1
                    self.record_modification(pk_issue)
                    # Continue processing but flag the issue

                # 2. Handle variable length rows
                corrected_row, length_modifications = self.correct_row_length(
                    row, row_number
                )

                # 3. Detect bad values (but don't fix yet - that's Step 3)
                bad_values = self.detect_data_quality_issues(corrected_row)

                # 4. Track issues if any found
                all_modifications = length_modifications.copy()
                if pk_issue:
                    all_modifications.append(pk_issue)

                if bad_values or all_modifications:
                    self.bad_rows.append(
                        {
                            "row_number": row_number,
                            "original_row": row.copy(),
                            "corrected_row": corrected_row.copy(),
                            "bad_values": bad_values,
                            "modifications": all_modifications,
                            "has_empty_pk": pk_issue is not None,
                        }
                    )

                    # Add modifications to global tracking
                    for mod in length_modifications:
                        self.record_modification(mod)

                # 5. Clean data types and yield corrected row
                cleaned_row = self.clean_row(corrected_row, row_number)
                yield cleaned_row

        self.row_count = i

    @staticmethod
    def is_nul_like(value: str) -> bool:
        """
        Test if value should be converted to Nul
        Removes values which don't convey any meaning.
        """
        nul_like = set(["", "b6", "N/A", "A.2.a"])
        if value in nul_like:
            return True
        elif value.isspace():
            return True
        elif value[0] == "?" and value == len(value) * value[0]:
            return True
        elif value[0] == "0" and value == len(value) * value[0]:
            return True
        else:
            return False

    def replace_nul(self) -> None:
        """
        replace nul bytes in csv file. Write to self.no_nul
        """
        fi = open(self.csvfile, "rb")
        data = fi.read()
        fi.close()
        fo = open(self.no_nul, "wb")
        fo.write(data.replace(b"\x00", b""))
        fo.close()

    def del_no_nul(self) -> None:
        """
        Delete the no_nul file
        """
        if os.path.exists(self.no_nul):
            os.remove(self.no_nul)

    def _create_row_modification(
        self,
        row_number: int,
        mod_type: str,
        column: Optional[str],
        old_val: str,
        new_val: str,
        reason: str,
    ) -> RowModification:
        """
        Helper method to create RowModification objects consistently.
        Reduces code duplication in validation methods.
        """
        return RowModification(
            row_number=row_number,
            modification_type=mod_type,
            column_affected=column,
            original_value=old_val,
            new_value=new_val,
            reason=reason,
        )

    def correct_row_length(
        self, row: List[str], row_number: int
    ) -> Tuple[List[str], List[RowModification]]:
        """
        Intelligently handle rows with incorrect length.
        Returns corrected row and list of modifications made.
        """
        modifications = []
        corrected_row = row.copy()

        if len(row) == self.header_length:
            return corrected_row, modifications

        # Handle long rows (too many columns)
        if len(row) > self.header_length:
            # Check if file has auto-truncation rules
            if self.validation_rules.get("auto_truncate", False):
                expected_extra = self.validation_rules.get("expected_extra_cols", 0)

                if len(row) == self.header_length + expected_extra:
                    # Check if extra columns are empty (like tbl_Court_Motions)
                    extra_cols = row[self.header_length :]
                    if all(self.is_nul_like(col) for col in extra_cols):
                        corrected_row = row[: self.header_length]
                        modifications.append(
                            self._create_row_modification(
                                row_number,
                                "truncate",
                                None,
                                f"Length {len(row)}",
                                f"Length {len(corrected_row)}",
                                f"Auto-truncated {expected_extra} empty trailing columns",
                            )
                        )
                    else:
                        # Extra columns have data - flag as problematic
                        modifications.append(
                            self._create_row_modification(
                                row_number,
                                "flag_long_row",
                                None,
                                f"Length {len(row)}",
                                f"Length {len(row)}",
                                f"Row has {len(row) - self.header_length} extra columns with data",
                            )
                        )
                else:
                    # Unexpected length
                    modifications.append(
                        self._create_row_modification(
                            row_number,
                            "flag_unexpected_length",
                            None,
                            f"Length {len(row)}",
                            f"Length {len(row)}",
                            f"Unexpected row length: expected {self.header_length} + {expected_extra}, got {len(row)}",
                        )
                    )
            else:
                # No auto-truncation rule - flag as problematic
                modifications.append(
                    self._create_row_modification(
                        row_number,
                        "flag_long_row",
                        None,
                        f"Length {len(row)}",
                        f"Length {len(row)}",
                        f"Row too long: expected {self.header_length}, got {len(row)}",
                    )
                )

        # Handle short rows (too few columns)
        elif len(row) < self.header_length:
            # Pad with empty values
            missing_cols = self.header_length - len(row)
            corrected_row.extend([""] * missing_cols)
            modifications.append(
                self._create_row_modification(
                    row_number,
                    "pad",
                    None,
                    f"Length {len(row)}",
                    f"Length {len(corrected_row)}",
                    f"Padded {missing_cols} missing columns with empty values",
                )
            )

        return corrected_row, modifications

    def validate_primary_key(
        self, row: List[str], row_number: int
    ) -> Optional[RowModification]:
        """
        Validate that the primary key (first column) is not empty or null-like.
        Returns RowModification if primary key is invalid, None otherwise.
        """
        if not row:
            return self._create_row_modification(
                row_number,
                "empty_primary_key",
                self.primary_key_column,
                "[EMPTY ROW]",
                "INVALID",
                "Row is completely empty",
            )

        primary_key_value = row[0] if len(row) > 0 else ""

        if self.is_nul_like(primary_key_value):
            return self._create_row_modification(
                row_number,
                "empty_primary_key",
                self.primary_key_column,
                primary_key_value,
                "INVALID",
                "Primary key cannot be empty or null-like",
            )

        return None

    def detect_data_quality_issues(self, row: List[str]) -> List[Tuple[int, str, str]]:
        """
        Enhanced bad value detection that respects file-specific rules.
        Returns list of (column_index, value, reason) tuples.
        """
        bad_values = []

        if not self.dtypes:
            return bad_values

        # Get columns to skip validation for this file
        skip_cols = set(self.validation_rules.get("skip_validation_cols", []))

        for i, value in enumerate(row):
            if i >= len(self.header):
                break

            col_name = self.header[i]

            # Skip validation for known empty/legacy columns
            if col_name in skip_cols:
                continue

            # Skip validation for nul-like values in high-nul-tolerance files
            if self.validation_rules.get(
                "high_nul_tolerance", False
            ) and self.is_nul_like(value):
                continue

            # Get expected data type
            if col_name not in self.dtypes:
                continue

            dtype = self.dtypes[col_name]
            value = value.strip("\\").strip()

            if self.is_nul_like(value):
                continue  # Nul-like values are generally acceptable

            # Validate based on data type
            if dtype == "timestamp without time zone":
                if self.convert_timestamp(value) == r"\N":
                    bad_values.append((i, value, f"Invalid timestamp format"))
            elif dtype == "time without time zone":
                if self.convert_time(value) == r"\N":
                    bad_values.append((i, value, f"Invalid time format"))
            elif dtype == "integer":
                if self.convert_integer(value) == r"\N":
                    bad_values.append((i, value, f"Invalid integer format"))
            elif dtype.startswith("^"):  # Regex pattern
                if not re.match(dtype, value):
                    bad_values.append((i, value, f"Does not match pattern {dtype}"))
            elif dtype.endswith(".json"):  # Lookup table reference
                # Note: Full lookup validation would require loading lookup tables
                # For now, we'll skip this to avoid performance impact
                pass

        return bad_values

    def record_modification(self, modification: RowModification) -> None:
        """
        Add a modification to the tracking list.
        """
        self.modifications.append(modification)

    def generate_quality_report(self) -> Dict:
        """
        Generate comprehensive quality metrics report.
        """
        if not self.bad_rows:
            return {
                "total_rows": self.row_count,
                "structural_issues": 0,
                "data_quality_issues": 0,
                "modifications_summary": {},
                "recommendations": [],
            }

        structural_issues = len([r for r in self.bad_rows if r.get("modifications")])
        data_quality_issues = len([r for r in self.bad_rows if r.get("bad_values")])

        # Summarize modification types
        mod_summary = {}
        for mod in self.modifications:
            mod_type = mod.modification_type
            mod_summary[mod_type] = mod_summary.get(mod_type, 0) + 1

        # Generate recommendations
        recommendations = []
        if structural_issues > 0:
            recommendations.append(
                f"Review {structural_issues} rows with structural issues"
            )
        if data_quality_issues > 0:
            recommendations.append(
                f"Review {data_quality_issues} rows with data quality issues"
            )

        return {
            "total_rows": self.row_count,
            "structural_issues": structural_issues,
            "data_quality_issues": data_quality_issues,
            "modifications_summary": mod_summary,
            "recommendations": recommendations,
        }

    def random_sample_bad_rows(self, sample_size: int = 100) -> List[Dict]:
        """
        Extract random sample of problematic rows for manual inspection.
        """
        import random

        if len(self.bad_rows) <= sample_size:
            return self.bad_rows

        return random.sample(self.bad_rows, sample_size)

    @staticmethod
    def convert_integer(value: str) -> str:
        """
        Test if value is convertible to an integer, if not return null
        """
        try:
            value = value.replace("O", "0")
            int(value)
            return value
        except ValueError:
            return r"\N"

    @staticmethod
    def convert_timestamp(value: str) -> str:
        """
        Test if value is convertible to an timestamp, if not return null
        """
        try:
            datetime.fromisoformat(value)
            return value
        except ValueError:
            return r"\N"

    @staticmethod
    def convert_time(value: str) -> str:
        """
        Test if value is convertible to a time, if not return null
        """
        try:
            if len(value) == 4 and ":" in value:
                value = "0" + value
                value = value.replace(":", "")
            time.fromisoformat(value[:2] + ":" + value[2:])
            return value
        except ValueError:
            return r"\N"

    def clean_row(self, row: List[str], row_number: int) -> List[str]:
        """
        Apply data type conversions to row values, converting invalid values to \\N.
        Records modifications for tracking. Based on legacy clean_row() logic.
        """
        if not self.dtypes:
            # No dtype information available, return row with pipe characters removed
            return [str(cell).replace("|", "") for cell in row]
        
        cleaned_row = row.copy()
        
        for i, value in enumerate(cleaned_row):
            if i >= len(self.header):
                break  # Skip extra columns beyond header length
                
            col_name = self.header[i]
            if col_name not in self.dtypes:
                # No dtype info for this column, just remove pipe characters
                cleaned_row[i] = str(value).replace("|", "")
                continue
                
            dtype = self.dtypes[col_name]
            original_value = value
            value = str(value).strip("\\").strip()
            
            # Handle null-like values first
            if self.is_nul_like(value):
                cleaned_row[i] = r"\N"
                if original_value != r"\N":
                    self.record_modification(
                        self._create_row_modification(
                            row_number, "convert_to_null", col_name,
                            original_value, r"\N", "Converted null-like value"
                        )
                    )
                continue
            
            # Apply type-specific conversions
            if dtype == "timestamp without time zone":
                converted = self.convert_timestamp(value)
                cleaned_row[i] = converted
                if converted == r"\N" and original_value != r"\N":
                    self.record_modification(
                        self._create_row_modification(
                            row_number, "convert_to_null", col_name,
                            original_value, r"\N", "Invalid timestamp format"
                        )
                    )
            elif dtype == "time without time zone":
                converted = self.convert_time(value)
                cleaned_row[i] = converted
                if converted == r"\N" and original_value != r"\N":
                    self.record_modification(
                        self._create_row_modification(
                            row_number, "convert_to_null", col_name,
                            original_value, r"\N", "Invalid time format"
                        )
                    )
            elif dtype == "integer":
                converted = self.convert_integer(value)
                cleaned_row[i] = converted
                if converted == r"\N" and original_value != r"\N":
                    self.record_modification(
                        self._create_row_modification(
                            row_number, "convert_to_null", col_name,
                            original_value, r"\N", "Invalid integer format"
                        )
                    )
            else:
                # For other data types, just remove pipe characters
                cleaned_row[i] = value.replace("|", "")
        
        return cleaned_row


def dump_counts(count_file_path: str) -> Dict[str, int]:
    """
    Parse Count.txt file and return row counts for each table.

    Args:
        count_file_path: Path to Count.txt file

    Returns:
        Dictionary mapping table names to row counts
    """
    table_counts = {}

    with open(count_file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("TableName"):
                continue

            # Parse lines like "A_TblCase	8608373 rows copied."
            parts = line.split("\t")
            if len(parts) >= 2:
                table_name = parts[0]
                count_text = parts[1]

                # Extract number from "8608373 rows copied."
                match = re.search(r"(\d+)\s+rows\s+copied", count_text)
                if match:
                    row_count = int(match.group(1))
                    table_counts[table_name] = row_count

    return table_counts
