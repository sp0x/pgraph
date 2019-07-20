import os
import csv
import io
import graph

rootdir = "F:\leaks\MINFIN_BREACH"
user = "neo4j"
passw = "cardingsp0x"
url = "bolt://localhost"
neo = graph.GraphHelper(url, user, passw)

class DumpInfo:

    def __init__(self, has_egn):
        self.has_egn = has_egn
        self.row_count = 0


egn_set = set()


def process_file(fpath):
    output = DumpInfo(False)
    with io.open(fpath, mode="r", encoding="utf-8") as csv_f:
        csv_reader = csv.reader(csv_f, delimiter=",")
        header = next(csv_reader)
        header_indexes = {v: i for i, v in enumerate(header)}
        egn_headers = [x for x in header if "egn" in str(x).lower()]
        output.has_egn = len(egn_headers) > 0

        if output.has_egn:

            for row in csv_reader:
                for egn_fld in egn_headers:
                    egn_ix = header_indexes[egn_fld]
                    if egn_ix >= len(row)-1:
                        continue
                    egn_set.add(row[egn_ix])
                output.row_count += 1

    return output


total_egn_rows = 0

for root, subdirs, files in os.walk(rootdir):
    print('--\nroot = ' + root)
    for filename in files:
        file_path = os.path.join(root, filename)
        di = process_file(file_path)
        if di.has_egn:
            print('\t- EGN-ed[%d] file %s (full path: %s)' % (di.row_count, filename, file_path))
        total_egn_rows += di.row_count
    print("--Egn rows: %d" % total_egn_rows)

print("Total egn count: %d" % len(egn_set))
