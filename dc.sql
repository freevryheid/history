select t2.cnum, t1.dnam, t2.cnam from district t1 join county t2 on t1.dnum = t2.dnum order by t2.cnum;
