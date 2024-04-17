import time
import json
import pickle
import requests as req
from bs4 import BeautifulSoup as bs
from get_config import Config
from mysql_tools import connection, create_db

class CrawlerBS:

    def __init__(self):
        self.config = Config("beautifulsoup", "crawler.yaml").get()
        self.refer = self.config["url"]["refer"]
        self.headers = {"User-Agent": self.config["user_agent"]}
        self.db_name = self.config["db_name"]
        create_db(self.db_name)

    def crawl_note_mat(self):
        resp = req.get("%s%s/" % (self.refer, self.config["target"]["note"]), headers=self.headers)
        resp.raise_for_status()
        html = bs(resp.text, "lxml")
        types = html.find("div", id="app").find("div", class_="off-canvas-wrapper grid-container").find("div", id="main-content").find_all("div", class_="cell gone4empty")
        elements = html.find("div", id="app").find("div", class_="off-canvas-wrapper grid-container").find("div", id="main-content").find_all("div", class_="grid-x grid-margin-y grid-margin-x")
        name_list = []
        for i in range(len(types)):
            if i % 2 != 0:
                continue
            name_list.append(types[i].text.replace("\n", "").lower().strip())
        data = {}
        for i in range(len(elements)):
            temp = elements[i].text.split("\n")
            data[name_list[i]] = []
            for j in temp:
                if j != "":
                    data[name_list[i]].append(j.lower().strip())
        return data

    def insert_note_mat(self):
        with connection(self.db_name) as conn:
            with conn.cursor() as cursor:
                cursor.execute("select id, name from note")
                note_rows = cursor.fetchall()
                init_note = {} if not note_rows else {row["name"]: row["id"] for row in note_rows}
                cursor.execute("select name, note_id from material")
                mat_rows = cursor.fetchall()
                init_mat = {} if not mat_rows else {row["name"]: row["note_id"] for row in mat_rows}
        note_mat = self.crawl_note_mat()
        conn = connection(self.db_name)
        try:
            with conn.cursor() as cursor:
                for note, mat in note_mat.items():
                    if note not in init_note:
                        sql_note = "insert into note (name) values (%s)"
                        cursor.execute(sql_note, note)
                        note_id = cursor.lastrowid
                    else:
                        note_id = init_note[note]
                    mat = set(mat) - set(init_mat.keys())
                    mat_note_id = [(mat_item, note_id) for mat_item in mat]
                    sql_mat = "insert into material (name, note_id) values (%s, %s)"
                    cursor.executemany(sql_mat, mat_note_id)
                    conn.commit()
        except Exception as e:
            print(e)
            conn.rollback()
        conn.close()

    def crawl_group(self):
        resp = req.get("%s%s/" % (self.refer, self.config["target"]["group"]), headers=self.headers)
        resp.raise_for_status()
        html = bs(resp.text, "lxml")
        types = html.find("div", id="app").find("div", class_="off-canvas-wrapper grid-container").find("div", id="main-content").find_all("h2")
        elements = html.find("div", id="app").find("div", class_="off-canvas-wrapper grid-container").find("div", id="main-content").find_all("div", class_="cell small-6")
        name_list = []
        for i in types:
            name_list.append(i.text.replace("\n", "").lower().strip())
        data = {}
        for i in range(len(elements)):
            if i % 2 != 0:
                el = elements[i].text.lower().strip()
                el = [item.strip() for item in el.split("\n") if item.strip()]
                data[name_list[i // 2]] = [name_list[i // 2]] + el
        return data

    def insert_group(self):
        with connection(self.db_name) as conn:
            with conn.cursor() as cursor:
                cursor.execute("select id, name from main_group")
                mgroup_rows = cursor.fetchall()
                init_mgroup = {} if not mgroup_rows else {row["name"]: row["id"] for row in mgroup_rows}
                cursor.execute("select name, main_group_id from sub_group")
                sgroup_rows = cursor.fetchall()
                init_sgroup = {} if not sgroup_rows else {row["name"]: row["main_group_id"] for row in sgroup_rows}
        main_sub = self.crawl_group()
        conn = connection(self.db_name)
        try:
            with conn.cursor() as cursor:
                for main, sub in main_sub.items():
                    if main not in init_mgroup:
                        sql_mgroup = "insert into main_group (name) values (%s)"
                        cursor.execute(sql_mgroup, main)
                        mgroup_id = cursor.lastrowid
                    else:
                        mgroup_id = init_mgroup[main]
                    sub = set(sub) - set(init_sgroup.keys())
                    main_sub_id = [(sgroup_item, mgroup_id) for sgroup_item in sub]
                    sql_sgroup = "insert into sub_group (name, main_group_id) values (%s, %s)"
                    cursor.executemany(sql_sgroup, main_sub_id)
                    conn.commit()
        except Exception as e:
            print(e)
            conn.rollback()
        conn.close()

    def crawl_perfumer(self):
        resp = req.get("%s%s/" % (self.refer, self.config["target"]["perfm"]), headers=self.headers)
        resp.raise_for_status()
        html = bs(resp.text, "lxml")
        elements = html.find("div", id="app").find("div", class_="off-canvas-wrapper grid-container").find("div", id="main-content").find_all("div", class_="cell small-12 medium-4")
        name_list = []
        for i in range(len(elements)):
            name_list.append(elements[i].text.replace("\n", "").strip())
        return name_list

    def insert_perfumer(self):
        with connection(self.db_name) as conn:
            with conn.cursor() as cursor:
                cursor.execute("select name from perfumer")
                perfm_rows = cursor.fetchall()
                init_perfm = {} if not perfm_rows else {row["name"] for row in perfm_rows}
        perfm = self.crawl_perfumer()
        conn = connection(self.db_name)
        try:
            with conn.cursor() as cursor:
                perfm = list(set(perfm) - set(init_perfm))
                sql_perfm = "insert into perfumer (name) values (%s)"
                cursor.executemany(sql_perfm, perfm)
                conn.commit()
        except Exception as e:
            print(e)
            conn.rollback()
        conn.close()

    def insert_sequence(self):
        seq = self.config["target"]["seq"]
        conn = connection(self.db_name)
        try:
            with conn.cursor() as cursor:
                cursor.execute("select name from sequence")
                seq_rows = cursor.fetchall()
                init_seq = {} if not seq_rows else {row["name"] for row in seq_rows}
                seq = [s for s in seq if s not in init_seq]
                sql_seq = "insert into sequence (name) values (%s)"
                cursor.executemany(sql_seq, seq)
                conn.commit()
        except Exception as e:
            print(e)
            conn.rollback()
        conn.close()

    def crawl_perf(self, name, url):
        print(url)
        ts = time.perf_counter()
        try:
            resp = req.get(url, headers=self.headers)
            resp.raise_for_status()
        except Exception as e:
            print(e)
            if resp.status_code == 429:
                print("We must wait!")
                print(resp.headers)
        html = bs(resp.text, "lxml")
        main = html.find("div", id="app").find("div", id="main-content").find("div", class_="grid-x grid-margin-x").find("div", class_="small-12 medium-12 large-9 cell")
        text = main.find("div", itemprop="description").find("p").text.strip()
        group_id = req.post("%s" % (self.config["url"]["parse_group"]), data=json.dumps({"text": text}), headers={"Content-Type": "application/json"}).json()["group"]
        year = req.post("%s" % (self.config["url"]["parse_year"]), data=json.dumps({"text": text}), headers={"Content-Type": "application/json"}).json()["year"]
        try:
            perfm = main.find("div", class_="grid-x grid-padding-x grid-padding-y small-up-2 medium-up-2").text
        except Exception as e:
            print(e)
            perfm = "unknown"
        try:
            seq_mat = {}
            h4_tags = main.find("div", id="pyramid").find("div", class_="cell").find("div", style="display: flex; flex-direction: column; justify-content: center; text-align: center; background: white;").find_all("h4")
            if h4_tags:
                for tag in h4_tags:
                    seq = tag.text.split(" ")[0].lower().strip()
                    seq_mat[seq] = []
                    next_div = tag.find_next("div")
                    if next_div:
                        for i in next_div:
                            seq_mat[seq].append(i.text.lower().strip())
            else:
                seq = "base"
                seq_mat[seq] = []
                for i in main.find("div", id="pyramid").find("div", class_="cell").find("div", style="display: flex; flex-direction: column; justify-content: center; text-align: center; background: white;").find_all("div")[1]:
                    seq_mat[seq].append(i.text.lower().strip())
        except Exception as e:
            print(e)
        te = time.perf_counter()
        if seq_mat:
            print("seq_mat: {}".format(seq_mat))
            data = {"name": name, "group_id": group_id, "year": year, "perfumer": perfm, "seq_mat": seq_mat}
        else:
            data = {"name": name, "group_id": group_id, "year": year, "perfumer": perfm}
        print("processing time: {} sec.".format(te - ts))
        return data

    def insert_perf(self, brand):
        with connection(self.db_name) as conn:
            with conn.cursor() as cursor:
                cursor.execute("select id, name from brand")
                brand_rows = cursor.fetchall()
                init_brand = {} if not brand_rows else {row["name"]: row["id"] for row in brand_rows}
                cursor.execute("select id, name, brand_id from perfume")
                perf_rows = cursor.fetchall()
                init_perf = {} if not perf_rows else {row["name"]: {"id": row["id"], "brand_id": row["brand_id"]} for row in perf_rows}
                cursor.execute("select id, name from sub_group")
                sgroup_rows = cursor.fetchall()
                init_sgroup = {} if not sgroup_rows else {row["name"]: row["id"] for row in sgroup_rows}
                cursor.execute("select id, name from sequence")
                seq_rows = cursor.fetchall()
                init_seq = {} if not seq_rows else {row["name"]: row["id"] for row in seq_rows}
        print("init_brand: {}".format(init_brand))
        print("init_perf: {}".format(init_perf))
        print("init_sgroup: {}".format(init_sgroup))
        print("init_seq: {}".format(init_seq))
        with open ("./data/filtered/%s.pkl" % (brand.strip().replace(" ", "")), "rb") as f:
            brand_d = pickle.load(f)
        count, idx = 0, 0
        brand_id = 0
        if brand not in init_brand:
            with connection(self.db_name) as conn:
                with conn.cursor() as cursor:
                    sql_brand = "insert into brand (name) values (%s)"
                    cursor.execute(sql_brand, brand)
                    brand_id = cursor.lastrowid
                    init_brand[brand] = brand_id
                    conn.commit()
        else:
            brand_id = init_brand[brand]

        for name, url in brand_d.items():
            if count % 10 == 0 and count != 0:
                print("break and restart the program")
                break
            idx += 1
            print("%d:\t%s" % (idx, name))
            if name in init_perf and init_perf[name]["brand_id"] == brand_id:
                continue
            else:
                print("wait for 60 sec...")
                time.sleep(60)
                data = self.crawl_perf(name, url)
                with connection(self.db_name) as conn:
                    with conn.cursor() as cursor:
                        sql_perf = "insert into perfume (name, perfumer, sub_group_id, year, brand_id) values (%s, %s, %s, %s, %s)"
                        cursor.execute(sql_perf, (name, data["perfumer"], data["group_id"], data["year"], brand_id))
                        if "seq_mat" in data:
                            perf_id = cursor.lastrowid
                            init_perf[name] = {"id": perf_id, "brand_id": brand_id}
                            sql_perf_mat = "insert into perf_mat (perfume_id, material, sequence_id) values (%s, %s, %s)"
                            insert_list = []
                            for seq, mat in data["seq_mat"].items():
                                for _ in mat:
                                    insert_list.append((perf_id, _, init_seq[seq]))
                            cursor.executemany(sql_perf_mat, insert_list)
                        conn.commit()
            print(data)
            print("\n")
            count += 1
