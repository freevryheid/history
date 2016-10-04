-- psql smgr -f gen_history
drop table if exists history;
create temp table titm as select trim(cont_id) as cont_id1, trim(prj_nbr) as prj_nbr1, ln_itm_nbr, itm_cd, spc_yr, bid_qty, unt_pric, net_c_o_qty, desc1 as itmdesc, unt_t from itm where bid_qty+net_c_o_qty > 0;
create temp table tprj as select trim(cont_id) as cont_id2, trim(prj_nbr) as prj_nbr2, desc1 as prjdesc from prj;
create temp table tdcis as select trim(control_sect_job) as prj_nbr3, actual_let_date, district_number, county_number, highway_number, proj_length, proj_class, beg_mile_point, end_mile_point, beg_ref_marker_nbr, beg_ref_marker_disp, end_ref_marker_nbr, end_ref_marker_disp from dcis;  
create temp table t1 as select * from titm join tprj on titm.cont_id1 = tprj.cont_id2 and titm.prj_nbr1 = tprj.prj_nbr2; 
create temp table t2 as select * from t1 join tdcis on t1.prj_nbr2 = tdcis.prj_nbr3; 
create temp table t3 as select * from t2 join prj_cmpl_vw on cont_id1 = cont_id and prj_nbr1 = prj_nbr;
create temp table t4 as select t3.*, county.cnam, district.dnam from t3 join county on t3.county_number = county.cnum join district on county.dnum = district.dnum;
create table history as select prj_nbr, substr(itm_cd,1,4) as itm, min(ln_itm_nbr) as lin, min(spc_yr) as spc_yr, min(prjdesc) as pjdesc, sum(bid_qty+net_c_o_qty) as qty, avg(unt_pric) as unt_pric, sum((bid_qty+net_c_o_qty)*unt_pric) as cost, string_agg(itmdesc,' | ') as itmdesc, string_agg(trim(unt_t),' | ') as unt_t, min(actual_let_date) as letdate, min(actl_cmpl_dt) as cmpdate, min(district_number) as district_number, min(county_number) as county_number, min(dnam) as district, min(cnam) as county, min(highway_number) as highway_number, min(proj_length) as proj_length, min(proj_class) as proj_class, min(beg_ref_marker_nbr) as brm, min(beg_ref_marker_disp) as bdp, min(end_ref_marker_nbr) as erm, min(end_ref_marker_disp) as edp from t4 group by prj_nbr, itm;
alter table history add constraint history_pkey primary key (prj_nbr, itm);


