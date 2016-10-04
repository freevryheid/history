#!/usr/bin/env python
import csv
# import sys







class Hist():

    def __init__(self):
        district = {
            1: "paris",
            2: "fort_worth",
            3: "wichita_falls",
            4: "amarillo",
            5: "lubbock",
            6: "odessa",
            7: "san_angelo",
            8: "abilene",
            9: "waco",
            10: "tyler",
            11: "lufkin",
            12: "houston",
            13: "yoakum",
            14: "austin",
            15: "san_antonio",
            16: "corpus_christi",
            17: "bryan",
            18: "dallas",
            19: "atlanta",
            20: "beaumont",
            21: "pharr",
            22: "laredo",
            23: "brownwood",
            24: "el_paso",
            25: "childress",
        }
        self.data = []
        self.rows = csv.DictReader(open("district.csv"))
        for row in self.rows:
            self.data.append(row)
        self.data = sorted(self.data, key=lambda k: k['csj'])
        self.dname = district[int(self.data[0]['district'])]+".csv"
        id = 1
        prj = 1
        for i, d in enumerate(self.data):
            csj = d['csj']
            if i > 0:
                if csj != self.data[i-1]['csj']:
                    prj += 1
            d['id'] = id
            d['prj'] = prj
            id += 1

    def update(self, id, key, val):
        for d in self.data:
            if d['id'] == id:
                d[key] = val
                break

    def output(self):
        with open(self.dname, 'w') as f:
            w = csv.DictWriter(f, lineterminator='\n', fieldnames=self.rows.fieldnames)
            w.writeheader()
            for d in self.data:
                w.writerow(d)

    def numlayer(self, layer):
        if layer.startswith("Ultra"):
            return 1
        elif layer.startswith("Thin"):
            return 2
        elif layer.startswith("Permeable"):
            return 3
        elif layer.startswith("Stone"):
            return 4
        elif layer.startswith("Micro"):
            return 5
        elif layer.startswith("Membrane"):
            return 6
        elif layer.startswith("Concrete"):
            return 7
        elif layer.startswith("Superpave"):
            return 8
        elif layer.startswith("Dense"):
            return 9
        elif layer.startswith("Fog"):
            return 10
        elif layer.startswith("Seal"):
            return 11
        elif layer.startswith("Milling"):
            return 12
        elif layer.startswith("Flex"):
            return 13
        elif layer.startswith("Fabric"):
            return 14
        elif layer.startswith("Geo"):
            return 15
        elif "stabilized" in layer:
            return 16
        elif "treated" in layer:
            return 17
        elif layer.startswith("Fill"):
            return 18
        elif layer.startswith("Subgrade"):
            return 19
        else:
            return 1000

    def main(self):
        counter = 1
        while True:
            print(counter)
            sd = [r for r in self.data if r['prj'] == counter]
            if len(sd) == 0:
                print("breaking ...")
                break
            elif len(sd) == 1:
                id = sd[0]['id']
                self.update(id, 'layerno', 1)
            else:
                td = []
                for s in sd:
                    id = s['id']
                    layer = self.numlayer(s['layer'])
                    td.append({'layer': layer, 'id': id})
                std = sorted(td, key=lambda k: k['layer'])
                for i, t in enumerate(std):
                    self.update(t['id'], 'layerno', i+1)
            counter += 1
        self.output()

if __name__ == "__main__":
    hist = Hist()
    hist.main()
