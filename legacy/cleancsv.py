import csv
import io
import json
import os
import re
import sys
from contextlib import contextmanager
from datetime import datetime, time
from typing import Iterator, Optional

import psycopg
from config.definitions import JSON_DIR


@contextmanager
def open_db(conn):
    """
    Supplies a cursor for database transactions.
    Usage: with open_db() as curs:
    :yields curs
    """
    try:
        curs = conn.cursor()
        yield curs
    except Exception as error:
        print(error)
    finally:
        conn.commit()
        conn.close()


@contextmanager
def get_reader_writer(file, rw: str):
    """
    Yield csv reader or writer given filepath and read or write string.
    """
    try:
        if rw == "r":
            with open(file, "r", newline="", encoding="utf-8", errors="replace") as f:
                reader = csv.reader(
                    f,
                    delimiter="\t",
                    dialect="excel-tab",
                    quoting=csv.QUOTE_NONE,
                    escapechar="\\",
                )
                yield reader
        elif rw == "w":
            with open(file, "a", newline="", encoding="utf-8", errors="replace") as f:
                writer = csv.writer(
                    f,
                    delimiter="\t",
                    dialect="excel-tab",
                    quoting=csv.QUOTE_NONE,
                    escapechar="\\",
                )
                yield writer
    except csv.Error as e:
        sys.exit(f"file, line {reader.line_num}, {e}")


class CleanCsv:
    def __init__(self, csvfile) -> None:
        """
        Set variables with filepaths
        """
        self.csvfile = csvfile
        self.header = self.get_header()
        self.header_length = len(self.header)
        self.name = os.path.basename(self.csvfile)
        self.js_name = f"{JSON_DIR}/table-dtypes/{self.name.replace('.csv', '.json')}"
        self.no_nul = os.path.abspath(self.csvfile).replace(".csv", "_no_nul.csv")
        self.bad_row = os.path.abspath(self.csvfile).replace(
            ".csv", "_br.csv"
        )  # [DEBUG]
        self.cleaned = os.path.abspath(self.csvfile).replace(
            ".csv", "_cleaned.csv"
        )  # [DEBUG]
        self.row_count = 0
        self.bad_count = 0
        self.empty_pk = 0
        try:
            with open(f"{JSON_DIR}/tables.json", "r") as f:
                self.table = json.load(f)[self.name]
            with open(
                f"{JSON_DIR}/table-dtypes/{os.path.basename(self.js_name)}",
                "r",
            ) as f:
                self.dtypes = json.load(f)
        except FileNotFoundError as e:
            print(f"Need to setup json file for table. {e}")

    def copy_to_table(self, connection, postfix, table="") -> None:
        """
        iter_csv uses StringIteratorIO to create a file-like object from the generator csv_gen().
        """
        with open_db(connection) as curs:
            if not table:
                table = self.table + "_" + postfix
            curs.execute("""SET session_replication_role = replica;""")
            copy_statement = (
                f"COPY {table} FROM STDIN WITH (FORMAT TEXT, DELIMITER '|', NULL '\\N')"
            )

            with curs.copy(copy_statement) as copy:
                for row in self.csv_gen_pk():
                    copy.write(row)

            curs.execute("""SET session_replication_role = DEFAULT;""")

    def write_chop(self):
        with open(self.cleaned, "w") as f:
            for row in self.chop_gen():
                f.write(row)

    def replace_nul(self) -> None:
        """
        Replace nul bytes in csv file. Write to self.no_nul
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
        os.remove(self.no_nul)

    def csv_gen_pk(self) -> iter:
        """
        Checks lines for empty primary keys.
        Assumes first value is primary key, not true for one table.
        """
        for i, row in enumerate(self.csv_gen()):
            if row.split("|")[0] == "\\N":
                self.empty_pk += 1
                continue
            else:
                yield row

    def csv_gen(self, skip_header=True) -> list:
        """
        Create a generator from csv file that yields cleaned and formatted rows.
        This is where handling of short or long rows occurs. Rows are added
        to short rows. Rows are removed from long rows if they are empty. If extra
        columns in long rows are not empty, the program attempts to remove values
        that would align the rows to the data types specified in the dtype file.
        """
        with open(
            self.no_nul, "r", newline="", encoding="utf-8", errors="replace"
        ) as f:
            for i, row in enumerate(
                csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
            ):
                try:
                    if not row:
                        continue
                    elif self.is_nul_like(row[0]):
                        self.empty_pk += 1
                        continue
                    elif i == 0 and skip_header:
                        continue  # skip header row
                    elif i == 0 and not skip_header:
                        yield "|".join(self.header) + "\n"
                    elif len(row) == self.header_length:
                        yield self.clean_row(row)
                    elif len(row) > self.header_length:
                        _bad_vals = self.get_bad_values(row)
                        if _bad_vals:
                            copy = self.shift_values(row)
                            if not copy or not self.get_bad_values(copy):
                                yield self.clean_row(self.remove_extra_cols(copy))
                        elif not _bad_vals:
                            clean_extra = self.remove_extra_cols(row)
                            if clean_extra:
                                yield self.clean_row(clean_extra)
                        else:
                            continue
                    elif len(row) < self.header_length:
                        yield self.clean_row(self.add_extra_cols(row))
                except (AttributeError, IndexError, TypeError) as e:
                    print(e)
                    import IPython

                    IPython.embed()
                    continue
            else:
                self.row_count = i

    def get_bad_rows(self) -> list:
        """
        Create a generator from csv file that yields cleaned and formatted rows.
        This is where handling of short or long rows are handled. Rows are added
        to short rows. Rows are removed from long rows if they are empty. If extra
        columns in long rows are not empty, the program attempts to remove values
        that would align the rows to the data types specified in the dtype file.
        """
        bad_rows = []
        with open(
            self.no_nul, "r", newline="", encoding="utf-8", errors="replace"
        ) as f:
            for i, row in enumerate(
                csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
            ):
                if i == 0:
                    continue  # skip header row
                elif len(row) > self.header_length:
                    bad_rows.append(row)
                    self.bad_count += 1
            else:
                print(
                    f"Writing {self.bad_count} bad rows, of {i} rows to {self.bad_row}"
                )

        with get_reader_writer(self.bad_row, "w") as w:
            for row in bad_rows:
                w.writerow(row)

    def chop_gen(self) -> None:
        with open(
            self.no_nul, "r", newline="", encoding="utf-8", errors="replace"
        ) as f:
            for i, row in enumerate(
                csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
            ):
                if i == 0:
                    continue  # skip header row
                elif len(row) > self.header_length:
                    yield "\t".join(row[: self.header_length]) + "\n"
                else:
                    yield "\t".join(row) + "\n"
            else:
                print(
                    f"Writing {self.bad_count} bad rows, of {i} rows to {self.bad_row}"
                )

    def clean_row(self, row) -> str:
        """
        Iterate over the row and check that values are the correct datatype, otherwise convert to null
        """
        for i, value in enumerate(row):
            dtype = list(self.dtypes.values())[
                i
            ]  # Gets the column datatype (json file name, regex, integer,timestamp, time or text)
            value = value.strip("\\").strip()
            if self.is_nul_like(value):
                row[i] = r"\N"
                continue
            elif dtype == "timestamp without time zone":
                row[i] = self.convert_timestamp(value)
                continue
            elif dtype == "time without time zone":
                row[i] = self.convert_time(value)
                continue
            elif dtype == "integer":
                row[i] = self.convert_integer(value)
                continue
            else:
                row[i] = value.replace("|", "")
        return "|".join(row) + "\n"

    def get_bad_values(self, row) -> list[(int, str)]:
        """
        The first step in cleaning a row with shifted data. Map the bad values.
        """
        bad_values = []
        codes = self.get_codes()
        for i, value in enumerate(row):
            try:
                dtype = list(self.dtypes.values())[
                    i
                ]  # Gets the column datatype (json file name, regex, integer,timestamp, time or text)
                value = value.strip("\\").strip()
                if self.is_nul_like(value):
                    continue
                elif dtype[0] == "^":  # dtype is a regex
                    if not re.match(dtype, value):
                        bad_values.append((i, value))
                elif dtype.endswith(".json"):
                    if value not in codes[dtype].keys():  # see if value is in lookups
                        bad_values.append((i, value))
                elif dtype == "timestamp without time zone":
                    if self.convert_timestamp(value) == r"\N":
                        bad_values.append((i, value))
                elif dtype == "time without time zone":
                    if self.convert_time(value) == r"\N":
                        bad_values.append((i, value))
                elif dtype == "integer":
                    if self.convert_integer(value) == r"\N":
                        bad_values.append((i, value))
                else:
                    continue
            except IndexError:
                # import IPython; IPython.embed()
                return bad_values

    def shift_values(self, row):
        """
        Certain bad values can be removed. E.g. '\x07' row.remove('\x07')
        Other bad rows each value needs to be checked to see if it belongs in another column.
        """
        bad_vals = self.get_bad_values(row)
        for bv in bad_vals:
            row_copy = row[:]  # deep copy of row
            ix = bv[0]
            for i in range(ix, 0, -1):
                if self.is_nul_like(row[i]):
                    row_copy.pop(i)
                if not self.get_bad_values(row_copy):
                    return row_copy
                else:
                    pass

    def remove_extra_cols(self, row) -> list:
        """
        If the extra columns are blank, remove them, otherwise return none.
        """
        extra_cols = row[self.header_length :]
        for value in extra_cols:
            if not self.is_nul_like:
                return None
        return row[: self.header_length]

    def add_extra_cols(self, row) -> list:
        """
        Append null values to short rows.
        """
        for i in range(self.header_length - len(row)):
            row.append("")
        return row

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

    def get_bad_line(self, lineno="") -> list:
        """
        When PostgreSQL copy_from fails, it may give helpful context on the line that failed.
        You can get the bad row by inputting the line here.
        """
        with open(
            self.no_nul, "r", newline="", encoding="utf-8", errors="replace"
        ) as f:
            for i, row in enumerate(
                csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
            ):
                if i == lineno:
                    return row

    def get_bad_row(self, value: str, column: str) -> list:
        """
        When PostgreSQL copy_from fails, it may give helpful context on the line that failed.
        You can use the first value in that bad row as the lineno to look up a bad line
        """
        _bad_rows = []
        index = list(self.dtypes.keys()).index(column)
        with open(
            self.no_nul, "r", newline="", encoding="utf-8", errors="replace"
        ) as f:
            for i, row in enumerate(
                csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
            ):
                try:
                    if row[index] == value:
                        print(f"Bad value located in row {i}: {row}")
                        _bad_rows.append(row)
                except IndexError:
                    continue
                    # As of now short rows will throw an index error.
        return _bad_rows

    def get_codes(self) -> dict:
        """
        Facilitates looking up the full description for codes in the database. E.g., 'MX' -> 'Mexico'
        Get a dictionary with json filename as key, and a code: description dictionary as the value
        """
        json_files = [file for file in self.dtypes.values() if file.endswith(".json")]
        json_dicts = []
        for file in json_files:
            with open(f"{JSON_DIR}/lookups/{file}", "r") as f:
                json_dicts.append(json.load(f))
        return dict(zip(json_files, json_dicts))

    def generate_table_type_file(self) -> None:
        """
        Facilitates the creation of a .json file with csv headers as key, and data type as value.
        Data type can be integer, time without tiemzone, timestamp without time zone, regular expression, or a codes.json file
        """
        with open(self.js_name, "w", encoding="utf-8") as f:
            json.dump(
                dict(zip(self.header, [""] * self.header_length)),
                f,
                ensure_ascii=False,
                indent=4,
            )

    def get_header(self) -> list:
        """
        Return first line of csv file
        """
        with get_reader_writer(self.csvfile, "r") as r:
            return next(r)
