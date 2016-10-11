import
  os, strutils, csv, db_postgres, sequtils

proc unique(ss: seq[string]): seq[string] =
  result = @[]
  for s in ss:
    if not (s in result):
      result.add(s)

proc fixtrm(i: string): string =
  var j = i.strip()
  if j.len() < 4:
    result = j.align(4, padding = '0')
  else:
    result = j.align(5, padding = '0')

let
  #csvFile = paramStr(1)
  db_pmis = open("localhost", "grassy", "", "pmis")

var
  hwys = db_pmis.getAllRows(sql"select concat(trim(substr(hwy,1,7)),trm) as hwytrm from hwys_trm;")

# close database
db_pmis.close()

# layer in LAYER
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

# loop thru csv files
for csvFile in walkFiles("../post/*.csv"):

  # if not csvFile.startsWith("../post/tyler"):
  #   continue

  var
    #csvFile = extractFilename(t)
    rows = readAll(csvFile, "mytmp")
    dtin,src,district,county,hwy,brm,bdp,erm,edp,lane,csj,pjdesc,workid,wrk,funcode,descr,layer,itm,lin,layerno,mthk,activity,letdate,compdate,qty,unt,cost,spec_yr,rehab: seq[string] = @[]
    shwy: seq[string] = @[]

  echo "processing " & csvFile

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

  # tests
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
  echo "+ passed: test headers"

  # test for blank completion date
  for i,d in compdate:
    # echo csvFile, " line: ", i+1 
    doAssert(d.len>0)
  echo "+passed: no blank completion dates" 

  # test for blanks in brm/erm
  doAssert(not ("" in brm.unique()))
  doAssert(not ("" in erm.unique()))
  echo "+ passed: no blanks in brm/erm"

  # test lane begins with M or F or l
  for i, b in lane:
     if i > 0:
       # echo "line: ", i, " field: ", b
       doAssert(b.startsWith("M") or b.startsWith("F"))
  echo "+ passed: lanes begins with M or F"

  # test len csj == 9
  for c in csj[1..^1].unique():
    doAssert(c.len() == 9)
  echo "+ passed: csj len"


  # test layer in known_layers
  for lay in layer[1..^1].unique():
    # echo lay
    doAssert(lay in known_layers)
  echo "+ passed: no unknown layers"

  # check trm info
  # need lane and trm limits

  # check brm markers
  for i, b in brm:
    if i > 0:
      #echo $(i+1), " BRM >> ", hwy[i] & fixtrm(b)
      doAssert(hwy[i] & fixtrm(b) in shwy)
  echo "+ passed: good brms"

    # check erm markers
  for i, b in erm:
    if i > 0:
      #echo $(i+1), " ERM >> ", hwy[i] & fixtrm(b)
      doAssert(hwy[i] & fixtrm(b) in shwy)
  echo "+ passed: good erms"

# To track done trm errors use the following sql (examples): 
# select * from dcis where control_sect_job ~ '156701024';
# select * from m where hwy ~ 'FM0720' and c_sec ~ '1567-01' order by data_date, frm_dfo;
# select * from m where hwy ~ 'SH0020' and c_sec ~ '0002-02' order by data_date, frm_dfo;
# select * from hwys_trm where hwy ~ 'SH0020' order by trm;


