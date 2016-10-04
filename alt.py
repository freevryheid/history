#!/usr/bin/env python
import psycopg2
import psycopg2.extras
# from collections import OrderedDict
# import sqlite3
import re
import datetime
import csv
import sys
from datetime import date as dt


class DB():

    def __init__(self):
        pass

    def qry(self, con, query, args, one, commit):
        # cur = con.cursor()
        cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(query, args)
        if commit:
            con.commit()
            res = 0
        else:
            # r = [OrderedDict((cur.description[i][0], value) for i, value in enumerate(row)) for row in cur.fetchall()]
            r = [dict((cur.description[i][0], value) for i, value in enumerate(row)) for row in cur.fetchall()]
            res = (r[0] if r else None) if one else r
        # cur.connection.close()
        con.close()
        return res

    def sqry(self, query, args=(), one=False, commit=False):
        # con = sqlite3.connect('newsa.db')
        con = psycopg2.connect(database="smgr", user="grassy", password="")
        return self.qry(con, query, args, one, commit)

    def hist(self, distn):
        # sql = "select * from history where district_number" + str(distn).zfill(2)
        # q = self.sqry(sql)
        q = self.sqry("select * from history where district_number = %s;", (distn,),)
        return q


class Hist():

    def __init__(self):
        self.itms = {
            '19930105': 'Milling',
            '19930112': 'Subgrade',
            '19930132': 'Fill',
            '19930247': 'Flexible base',
            '19930251': 'Flexible base',
            '19930260': 'Lime treated',
            '19930262': 'Lime treated',
            '19930275': 'Cement treated',
            '19930305': 'Milling',
            '19930310': 'Prime coat',
            '19930314': 'Fog seal',
            '19930316': 'Seal coat',
            '19930318': 'Seal coat',
            '19930330': 'Limestone rock asphalt LRA',
            '19930332': 'Limestone rock asphalt LRA',
            '19930334': 'Hot mix cold laid HMCL',
            '19930340': 'Dense-graded',
            '19930345': 'Asphalt stabilized base',
            '19930351': 'Flexible base',
            # '19930352': 'Joint/crack sealing',
            '19930354': 'Milling',
            '19930360': 'Concrete pavement',
            '19930361': 'Concrete pavement',
            # '19930428': 'Concrete pavement',
            # '19930429': 'Concrete pavement',
            '19931112': 'Geogrid',
            '19931159': 'Asphalt treated',
            '19933138': 'Microsurfacing',
            '19933146': 'Dense-graded',
            '19933231': 'Permeable friction course PFC',
            '19933248': 'Stone Matrix asphalt SMA',
            '19933293': 'Thin overlay mix TOM',
            '19933324': 'Ultra-thin bonded wearing course UTBWC',
            '19933371': 'Permeable friction course PFC',
            '19933433': 'Ultra-thin bonded wearing course UTBWC',
            '19933818': 'Permeable friction course PFC',
            '19935559': 'Concrete pavement',
            '19935743': 'Geogrid',
            '19950105': 'Milling',
            '19950132': 'Fill',
            '19950316': 'Seal coat',
            '19950340': 'Dense-graded',
            '19950354': 'Milling',
            '19953117': 'Dense-graded',
            '20040105': 'Milling',
            '20040112': 'Subgrade',
            '20040132': 'Fill',
            '20040247': 'Flexible base',
            '20040251': 'Flexible base',
            '20040260': 'Lime treated',
            '20040275': 'Cement treated',
            '20040305': 'Milling',
            '20040310': 'Prime coat',
            '20040314': 'Prime coat',
            '20040315': 'Fog seal',
            '20040316': 'Seal coat',
            '20040330': 'Limestone rock asphalt LRA',
            '20040334': 'Hot mix cold laid HMCL',
            '20040340': 'Dense-graded',
            '20040341': 'Dense-graded',
            '20040342': 'Permeable friction course PFC',
            '20040346': 'Stone matrix asphalt SMA',
            '20040350': 'Microsurfacing',
            '20040351': 'Flexible base',
            '20040354': 'Milling',
            '20040356': 'Fabric underseal',
            '20040360': 'Concrete pavement',
            '20040361': 'Concrete pavement',
            # '20040428': 'Concrete pavement',
            # '20040429': 'Concrete pavement',
            '20043000': 'Thin bonded permeable friction course TBPFC',
            '20043001': 'Ultra-thin bonded wearing course UTBWC',
            '20043066': 'Asphalt treated',
            '20043086': 'Asphalt treated',
            '20043101': 'Asphalt treated',
            '20043127': 'Thin bonded permeable friction course TBPFC',
            '20043142': 'Ultra-thin bonded wearing course UTBWC',
            '20043178': 'Hot in-place recycling',
            '20043221': 'Seal coat',
            '20043224': 'Dense-graded',
            '20043233': 'Membrane',
            '20043235': 'Seal coat',
            '20043267': 'Dense-graded',
            '20043268': 'Dense-graded',
            '20043270': 'Superpave SP',
            '20043271': 'Stone matrix asphalt SMA',
            '20043278': 'Seal coat',
            '20043283': 'Thin overlay mix TOM',
            '20045214': 'Geogrid',
            '20045261': 'Geogrid',
            '20045287': 'Geogrid',
            '20045398': 'Geogrid',
            '20045467': 'Geogrid',
            '20045816': 'Geogrid',
            '20140348': 'Thin bonded wearing course TBWC',
        }
        self.fieldnames = [
            'dtin',
            'src',
            'district',
            'county',
            'dnam',
            'cnam',
            'hwy',
            'brm',
            'bdp',
            'erm',
            'edp',
            'lane',
            # 'ccsj',
            'csj',
            'pjdesc',
            'proj_length',
            'workid',
            'wrk',
            'funcode',
            'descr',
            'layer',
            'itm',
            'lin',
            'layerno',
            'mthk',
            'activity',
            'letdate',
            'compdate',
            'qty',
            'unt',
            'cost',
            'spec_yr',
            'rehab',
            'prj',
            'id'
        ]

    def chkEq(self, lst):
        return lst[1:] == lst[:-1]

    def fixhwy(self, hwy):
        bed = hwy[0:2]
        if bed == 'LP':
            bed = 'SL'
        if bed == 'SP':
            bed = 'SS'
        nbr = hwy[3:]
        a = re.findall(r'\D+', nbr)
        if len(a) > 0:
            nbr = nbr.replace(a[0], "")
            a = a[0].replace("-", "")
        else:
            a = ""
        nbr = int(nbr)
        nbr = str(nbr).rjust(4, '0')
        return bed + nbr + a

    def fixdate(self, dt):
        dt = datetime.datetime.strptime(dt, "%d-%b-%y").date()
        yr = str(dt.year)
        mn = str(dt.month)
        dy = str(dt.month)
        dt = yr + mn.zfill(2) + dy.zfill(2)
        return dt

    def csv_writer(self, csvfile):
        writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames, lineterminator='\n')
        writer.writeheader()
        return writer

    def main(self):
        distn = sys.argv[1]
        # distn = 19
        today = dt.today()
        dtin = today.strftime("%Y%m%d")
        csvout = open('district.csv', 'w')
        writer = self.csv_writer(csvout)
        rows = db.hist(distn)
        for row in rows:
            hwy = row['highway_number']
            spc_yr = row['spc_yr']
            descr = row['itmdesc']
            itm = row['itm']
            unt = row["unt_t"]
            cost = row['cost']
            unt_pric = row['unt_pric']
            letdate = row['letdate']
            compdate = row['cmpdate']
            qty = row['qty']

            if hwy.strip() in ['VA', 'PW', 'CR', 'CS']:
                continue
            if spc_yr+itm not in self.itms.keys():
                continue

            calc_cost = True
            unt_list = unt.split(' | ')
            if len(unt_list) > 1:
                if self.chkEq(unt_list):
                    calc_cost = False
            if calc_cost:
                qty = ''
                unt = ''
                cost = cost
            else:
                qty = qty
                unt = unt_list[0]
                cost = unt_pric
            #if letdate is not None:
            #    letdate = self.fixdate(letdate)[0:-2]
            #if compdate is not None:
            #    compdate = self.fixdate(compdate)

            # fix descr
            descr = descr.split(" | ")
            descr = set(descr)
            descr = list(descr)
            descr = " | ".join(descr)

            # find thicknesses
            # thks = [int(s) for s in re.findall(r'(\d+\.?\d*)(?=.*")', descr)]
            # thks = [int(s) for s in re.findall(r'(\d+\.?\s?\d*\/?\d*)(?=.*")', descr)]
            thks = [s for s in re.findall(r'(\d+\.?\s?\d*\/?\d*)(?=\s?"|IN)', descr)]
            thks = [s.strip() for s in thks]
            if len(thks) > 0:
                mthk = max(thks)
            else:
                mthk = ''
            layer = self.itms[spc_yr+itm]
            if layer == "Seal coat":
                grades = [s for s in re.findall(r'(?<=GR)-?\s?(\d)', descr)]
                grades = [s.strip() for s in grades]
                if ('3' in grades and '4' in grades and '5' in grades):
                    layer += " 3 CR"
                elif ('3' in grades and '4' in grades) or ('3' in grades and '5' in grades) or ('4' in grades and '5' in grades):
                    layer += " 2 CR"
                elif '3' in grades:
                    layer += " grade 3"
                elif '4' in grades:
                    layer += " grade 4"
                elif '5' in grades:
                    layer += " grade 5"
            elif layer == "Dense-graded":
                mix = [s for s in re.findall(r'(?<=TY)-?\s?(\w)', descr)]
                mix = [s.strip() for s in mix]
                if len(mix) == 1:
                    if ('A' in mix):
                        layer += " TY-A"
                    elif ('B' in mix):
                        layer += " TY-B"
                    elif ('C' in mix):
                        layer += " TY-C"
                    elif ('D' in mix):
                        layer += " TY-D"
                    elif ('F' in mix):
                        layer += " TY-F"
            elif layer == "Flexible base":
                mix = [s for s in re.findall(r'(?<=GR)-?\s?(\w)', descr)]
                mix = [s.strip() for s in mix]
                if len(mix) == 1:
                    if ('1' in mix):
                        layer += " Gr 1"
                    elif ('2' in mix):
                        layer += " Gr 2"
                    elif ('3' in mix):
                        layer += " Gr 3"
                    elif ('4' in mix):
                        layer += " Gr 4"
                    elif ('5' in mix):
                        layer += " Gr 5"
                    elif ('6' in mix):
                        layer += " Gr 6"
            elif layer == "Ultra-thin bonded wearing course UTBWC":
                mix = [s for s in re.findall(r'(?<=TY)-?\s?(\w)', descr)]
                mix = [s.strip() for s in mix]
                if len(mix) == 1:
                    if ('C' in mix):
                        layer += " TY-C"
                    elif ('F' in mix):
                        layer += " TY-F"
            # lane info
            lane = ''
            # if row['roadbed_location'] is not None:
            #     lane += row['roadbed_location']
            # if row['direction_type'] is not None:
            #     lane += ' '+row['direction_type']
            writer.writerow({
                'dtin': dtin,
                'src': 'SM',
                'district': distn,
                'county': row['county_number'],
                'dnam': row['district'],
                'cnam': row['county'],
                'hwy': self.fixhwy(hwy),
                'brm': row['brm'],
                'bdp': row['bdp'],
                'erm': row['erm'],
                'edp': row['edp'],
                'lane': lane,
                # 'ccsj': row['cont_id'],
                'csj': row['prj_nbr'],
                'pjdesc': row['pjdesc'].strip(),
                'proj_length': row['proj_length'],
                'workid': '',
                'wrk': row['proj_class'],
                'funcode': '',
                'descr': descr,
                'layer': layer,
                'itm': itm,
                'lin': row['lin'],
                'layerno': '',
                'mthk': mthk,
                'activity': '',
                'letdate': letdate,
                'compdate': compdate,
                'qty': qty,
                'unt': unt,
                'cost': cost,
                'spec_yr': spc_yr,
                'rehab': '',
                # 'rehab': row['pavement_mgmt_plan'],
            })
        csvout.close()

if __name__ == "__main__":
    db = DB()
    hist = Hist()
    hist.main()
