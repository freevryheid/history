import
  os, strutils, csv, db_postgres, sequtils, math

# proc unique(ss: seq[string]): seq[string] =
#   result = @[]
#   for s in ss:
#     if not (s in result):
#       result.add(s)

proc fixtrm(i: string): string =
  result = i
  var j = i.strip()
  if j.len() < 4:
    result = j.align(4, padding = '0')

let
  debugs = paramStr(1)
  db_pmis = open("localhost", "grassy", "", "pmis")

var
  hwys = db_pmis.getAllRows(sql"select concat(trim(substr(route_name,1,7)),trm) as hwytrm from pa_trm;")
  debug: bool = false

if debugs == "1":
  debug = true

# close database
db_pmis.close()

let known_layers = [
  "Dense-graded TY-C",
  "Dense-graded TY-D",
  "Dense-graded TY-F",
  "Stone matrix asphalt SMA-C",
  "Stone matrix asphalt SMA-D",
  "Stone matrix asphalt SMA-F",
  "Stone matrix asphalt SMAR-C",
  "Stone matrix asphalt SMAR-F",
  "Permeable friction course PFC-C",
  "Permeable friction course PFC-F",
  "Permeable friction course PFCR-C",
  "Permeable friction course PFCR-F",
  "Thin overlay mix TOM-C",
  "Thin overlay mix TOM-F",
  "Superpave SP-C",
  "Superpave SP-D",
  "Coarse matrix high binder CMHB",
  "Ultra thin bonded wearing course",
  "CRCP",
  "Bonded concrete overlay",
  "Bonded concrete overlay",
  "Bonded concrete overlay",
  "Unbonded CRCP concrete overlay",
  "Unbonded JCP concrete overlay",
  "Ultra-thin white topping",
  "Jointed reinforced concrete",
  "Jointed plain concrete",
  "Seal coat grade 2",
  "Seal coat grade 3",
  "Seal coat grade 4",
  "Seal coat grade 5",
  "Milling",
  "Strip seal",
  "Slurry seal",
  "Thin bonded friction course",
  "Hot in-place recycling",
  "High friction surface treatment (hfst)",
  "Flexible base Gr 1",
  "Flexible base Gr 2",
  "Flexible base Gr 1-2",
  "Flexible base Gr 3",
  "Flexible base Gr 4",
  "Flexible base Gr 5",
  "Lime treated base",
  "Fly ash treated base",
  "Lime / fly ash treated base",
  "Asphalt treated base",
  "Emulsion treated base",
  "Foamed asphalt base",
  "Cement treated base",
  "No stabilization treatment subgrade",
  "Asphalt treated subgrade",
  "Cement treated subgrade",
  "Lime treated subgrade",
  "Fly ash treated subgrade",
  "Lime / Fly ash treated subgrade",
  "Emulsion treated subgrade",
  "Other subgrade",
  "Subgrade",
  "Concrete pavement",
  "Asphalt pavement",
  "Crack attenuating mix CAM",
  "Flexible base",
  "Flexible pavement structure repair",
  "Fog seal",
  "HMA PFC",
  "Prime coat",
  "Seal coat",
  "Superpave SP-B",
  "Surface treatment",
  "Dense-graded TY-A",
  "Dense-graded TY-B",
  "Fabric underseal",
  "Fill",
  "Flexible base",
  "Geogrid",
  "Hot mix cold laid HMCL",
  "Limestone rock asphalt LRA",
  "Membrane",
  "Microsurfacing",
  "Seal coat 2 CR",
  "Seal coat 3 CR",
  "Thin bonded permeable friction course TBPFC",
  "Ultra-thin bonded wearing course UTBWC",
  "Permeable friction course PFC",
  "Flexible base Gr 6",
  "Asphalt stabilized base",
  "Dense-graded HMA",
  "Stone matrix asphalt SMA",
]

let known_units = [
  "",
  "C",
  "ACR",
  "BAG",
  "BLL",
  "BDF",
  "BK",
  "BOX",
  "BKT",
  "BND",
  "CS",
  "CYM",
  "CAN",
  "CLMI",
  "COI",
  "CNT",
  "CUF",
  "CY",
  "D",
  "DMI",
  "DRU",
  "DAY",
  "DOZ",
  "EA",
  "F",
  "FT",
  "GLA",
  "GAL",
  "IN",
  "JAR",
  "JNT",
  "KG",
  "KM",
  "KIT",
  "LNM",
  "LS",
  "LF",
  "LTR",
  "LOT",
  "MGR",
  "M",
  "MIL",
  "ML",
  "MM",
  "NOS",
  "NONE",
  "OZ",
  "PAK",
  "PKG",
  "PAD",
  "PR",
  "PL",
  "MYR",
  "PC",
  "PT",
  "PTA",
  "LB",
  "LBA",
  "QTA",
  "QT",
  "REA",
  "ROL",
  "RO",
  "SVC",
  "SET",
  "SHT",
  "SQF",
  "SM",
  "STA",
  "SY",
  "TUB",
  "TON",
  "UNI",
  "MON",
  "MWK",
  "HOU",
  "YD",
  "PAI",
  "BOT",
  "DOL",
  "LFT",
  "CUY1",
  "CRG",
  "CYL",
  "CTN",
  "SQYD1",
  "JUG",
  "CM"
]

# loop thru csv files
# for csvFile in walkFiles("../post/baks/*.csv"):
for csvFile in walkFiles("../post/baks/done/*.csv"):

  var
    rows = readAll(csvFile, "mytmp")
    dtin,src,district,county,hwy,brm,bdp,erm,edp,lane,csj,pjdesc,workid,wrk,funcode,descr,layer,itm,lin,layerno,mthk,activity,letdate,compdate,qty,unt,cost,spec_yr,rehab: seq[string] = @[]
    shwy: seq[string] = @[]

  for h in hwys:
    shwy.add(h[0])

  for i,ls in rows:
    dtin.add(ls[0])
    src.add(ls[1])
    district.add(ls[2])
    county.add(ls[3])
    hwy.add(ls[4])
    brm.add(ls[5])
    bdp.add(ls[6])
    erm.add(ls[7])
    edp.add(ls[8])
    lane.add(ls[9])
    csj.add(ls[10])
    pjdesc.add(ls[11])
    workid.add(ls[12])
    wrk.add(ls[13])
    funcode.add(ls[14])
    descr.add(ls[15])
    layer.add(ls[16])
    itm.add(ls[17])
    lin.add(ls[18])
    layerno.add(ls[19])
    mthk.add(ls[20])
    activity.add(ls[21])
    letdate.add(ls[22])
    compdate.add(ls[23])
    qty.add(ls[24])
    unt.add(ls[25])
    cost.add(ls[26])
    spec_yr.add(ls[27])
    rehab.add(ls[28])

  # test headers
  doAssert(dtin[0] == "dtin")
  doAssert(src[0] == "src")
  doAssert(district[0] == "district")
  doAssert(county[0] == "county")
  doAssert(hwy[0] == "hwy")
  doAssert(brm[0] == "brm")
  doAssert(bdp[0] == "bdp")
  doAssert(erm[0] == "erm")
  doAssert(edp[0] == "edp")
  doAssert(lane[0] == "lane")
  doAssert(csj[0] == "csj")
  doAssert(pjdesc[0] == "pjdesc")
  doAssert(workid[0] == "workid")
  doAssert(wrk[0] == "wrk")
  doAssert(funcode[0] == "funcode")
  doAssert(descr[0] == "descr")
  doAssert(layer[0] == "layer")
  doAssert(itm[0] == "itm")
  doAssert(lin[0] == "lin")
  doAssert(layerno[0] == "layerno")
  doAssert(mthk[0] == "mthk")
  doAssert(activity[0] == "activity")
  doAssert(letdate[0] == "letdate")
  doAssert(compdate[0] == "compdate")
  doAssert(qty[0] == "qty")
  doAssert(unt[0] == "unt")
  doAssert(cost[0] == "cost")
  doAssert(spec_yr[0] == "spec_yr")
  doAssert(rehab[0] == "rehab")
  # echo "+ passed: test headers"

  # main loop
  for i in 0..<len(dtin):
    if debug: echo csvFile, " line: ", i+1
    if i > 0:
      if debug: echo "test for blank completion date"
      doAssert(compdate[i].len>0)
      if debug: echo "test for blank in brm"
      doAssert(brm[i] != "")
      if debug: echo "test for blank in erm"
      doAssert(erm[i] != "")
      if debug: echo "test for blank in bdp"
      doAssert(bdp[i] != "")
      if debug: echo "test for blank in edp"
      doAssert(edp[i] != "")
      if debug: echo "test that lane begins with M or F"
      doAssert(lane[i].startsWith('M') or lane[i].startsWith('F'))
      if debug: echo "test len csj == 9"
      doAssert(csj[i].len() == 9)
      if debug: echo "test layer in known_layers"
      doAssert(layer[i] in known_layers)
      if debug: echo "test for brm starting with 0"
      if len(brm[i]) > 1:
        doAssert(brm[i][0] != '0')
      if debug: echo "test for erm starting with 0"
      if len(erm[i]) > 1:
        doAssert(erm[i][0] != '0')
      if debug: echo "test for reversed brm/erm"
      doAssert(parseInt(brm[i][0..2]) <= parseInt(erm[i][0..2]))
      if debug: echo "test for negative bdp"
      doAssert(bdp[i][0] != '-')
      if debug: echo "test for negative edp"
      doAssert(edp[i][0] != '-')
      if debug: echo "test for brm markers"
      doAssert(hwy[i] & fixtrm(brm[i]) in shwy)
      if debug: echo "test for erm markers"
      doAssert(hwy[i] & fixtrm(erm[i]) in shwy)
      if debug: echo "test unit in known_units"
      doAssert(unt[i] in known_units)
