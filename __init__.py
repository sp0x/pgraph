import os
import csv
import io
import graph

rootdir = "D:\mfleak\MINFIN_BREACH"
user = "neo4j"
passw = "cardingsp0x"
url = "bolt://localhost"
neo = graph.GraphHelper(url, user, passw)


class DumpInfo:

    def __init__(self, has_egn):
        self.has_egn = has_egn
        self.row_count = 0
        self.is_person_file = False
        self.header = []


egn_set = set()
possible_person_fields = set()
person_files = set()


def add_person_fields(fields, file):
    for f in fields:
        if f == "PERSON_ID":
            continue
        possible_person_fields.add(f)
    person_files.add(file)


def perse_person_file(fpath, dump_info):
    pass


def create_person_model():
    pcount = 0
    for person_file in person_files:
        print("Filling in data from person file: " + person_file)
        with io.open(person_file, mode='r', encoding='utf-8') as csv_f:
            with neo.get_session() as session:
                csv_reader = csv.reader(csv_f, delimiter=',')
                header = next(csv_reader)
                assert 'PERSON_ID' in header
                for person_record in csv_reader:
                    if len(person_record)==0:
                        continue
                    pcount += 1
                    pdict = {header[index]: value for (index, value) in enumerate(person_record)}
                    #neo.update_person_info(session, pdict, person_file)
    print("Total person record count: " + str(pcount))


def process_file(fpath, go_over_rows=True):
    output = DumpInfo(False)
    with io.open(fpath, mode="r", encoding="utf-8") as csv_f:
        csv_reader = csv.reader(csv_f, delimiter=",")
        header = next(csv_reader)
        header_indexes = {v: i for i, v in enumerate(header)}
        egn_headers = [x for x in header if "egn" in str(x).lower()]
        output.has_egn = len(egn_headers) > 0
        file_egns = set()
        output.is_person_file = "PERSON_ID" in header
        output.header = header
        if output.is_person_file:
            add_person_fields(header, fpath)

        if go_over_rows and output.has_egn:

            for row in csv_reader:
                for egn_fld in egn_headers:
                    egn_ix = header_indexes[egn_fld]
                    if egn_ix >= len(row) - 1:
                        continue
                    egn_set.add(row[egn_ix])
                    file_egns.add(row[egn_ix])

                output.row_count += 1
        # if output.has_egn:
        #     neo.with_session(lambda s: neo.create_egn_accounts_from_file(s, fpath, header, file_egns))
        # else:

    return output


if __name__ == "__main__":
    total_egn_rows = 0

    for root, subdirs, files in os.walk(rootdir):
        print('--\nroot = ' + root)
        for filename in files:
            file_path = os.path.join(root, filename)
            di = process_file(file_path, False)
            if di.has_egn:
                print('\t- EGN-ed[%d] file %s (full path: %s)' % (di.row_count, filename, file_path))
            total_egn_rows += di.row_count
            # 1: Just create the file
            # neo.with_session(lambda s: neo.create_file(s, file_path, di.header))

            # 2: Parse the person file if needed
            if di.is_person_file:
                perse_person_file(file_path, di)

        print("--Egn rows: %d" % total_egn_rows)
        print("Person Fields: " + str(possible_person_fields))
        print("Person Files: " + str([os.path.basename(x) for x in person_files]))
    # 3: Create the person model for all people
    create_person_model()

    print("Total egn count: %d" % len(egn_set))
