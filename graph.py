from neo4j import GraphDatabase
import os
import utils


class GraphHelper(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def with_session(self, callback):
        with self._driver.session() as session:
            callback(session)

    def get_session(self):
        return self._driver.session()

    def create_egn_accounts_from_file(self, s, fpath, header, file_egns):
        filename = os.path.basename(fpath)
        egn_headers = utils.get_egn_headers(header)
        has_person_data = "PERSON_ID" in header
        if len(egn_headers) > 1:
            print("WTF..")
        identity_buff = create_identity_buff(file_egns)
        s.run("""
                CREATE (f:File { 
                name: $fname,
                 headers: $headers,
                  egn_header: $egn_fld,
                   has_person_data: $has_pdata
                } )
                """ + identity_buff + """
                RETURN f
                """, fname=filename,
              headers=header,
              egn_fld=egn_headers[0],
              has_pdata=has_person_data
              )

    def create_file(self, s, fpath, header):
        filename = os.path.basename(fpath)
        has_person_data = "PERSON_ID" in header
        egn_headers = utils.get_egn_headers(header)
        egn_header = egn_headers[0] if len(egn_headers) > 0 else None
        s.run("""
        CREATE (f:File { name: $fname, 
        headers: $headers, 
        egn_header: $egn_header,
        has_person_data: $has_pdata
        } )
        RETURN f
        """, fname=filename, egn_header=egn_header, headers=header, has_pdata=has_person_data)

    def create_index(self):
        #CREATE INDEX ON :Movie(title);
        pass

    def update_person_info(self, s, person_record, person_file):
        filename = os.path.basename(person_file)
        if len(person_record)==0:
            return
        pid = str(person_record['PERSON_ID'])
        result = s.run("""
        MATCH (p:Person {pid: $person_id})
        RETURN p""", person_id=str(pid))
        ret = result.single()
        person_exists = ret is not None and len(ret) > 0
        if not person_exists:
            s.run("""
            CREATE (p:Person {
            pid: $person_id
            })""", person_id=pid)
        else:
            return
        del person_record['PERSON_ID']
        s.run("""
        MATCH (p:Person {pid: '""" + pid + """'})
        SET p += $props""", props=person_record)
        s.run("""MATCH (p1:Person { pid: '""" + pid + """'}), (f1:File { name: $fname })
        CREATE (p1)-[r:IN_FILE]->(f1)
        """, person_id=pid, fname=filename)
        return ret


def create_identity_buff(egns):
    output = ""
    for ix, egn in enumerate(egns):
        output += "MERGE (p%d:Identity {egn: \"%s\"} )\n" % (ix, egn)
    output += "CREATE\n"
    for ix, egn in enumerate(egns):
        output += "  (p%d)-[:IN_FILE]->(%s)" % \
                  (ix, "f")
        if ix < len(egns) - 1:
            output += ",\n"
    return output
