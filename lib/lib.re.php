<?php

function re_conn()
{
    global $RE;
    $conn = "host=".$RE['host']." port=5432 dbname=".$RE['name']." user=".$RE['user']." password=".$RE['pass']." ";
    $dbconn = pg_connect($conn);
    if (!$dbconn) {
        echo "An error occured.\n";
        return -1;
    }
    
    $RE['dbconn'] = $dbconn;
    return 1;
}

function re_close()
{
    global $RE;
    pg_close($RE['dbconn']);
}

function re_query($query)
{
    global $RE;
    return pg_query($RE['dbconn'],$query);
}

function re_begin()
{
    re_query("begin;");
}

function re_commit()
{
    re_query("commit;");
}

function re_rollback()
{
    re_query("rollback;");
}

function re_racc_get_bacc_id($racc)
{
    $id = 0;
    
    $sql = "select billing_account_id from calling_number where calling_number = '".$racc."';";
    $res = re_query($sql);
    
    while($row = pg_fetch_row($res)) {
        $id = $row[0];
    }
    
    return $id;
}

function re_get_sub_num($start,$end)
{
    $num = 0;
    
    $sql = "SELECT count(*) from pcard,calling_number
            where start_date >= '".$start."' and 
            start_date < '".$end."' and 
            pcard.billing_account_id = calling_number.billing_account_id";
    $res = re_query($sql);
    
    while($row = pg_fetch_array($res)) {
        $num = $row[0];
    }

    return $num;
}

function re_get_bal_num($start,$end)
{
    $num = 0;
    
    $sql = "SELECT count(*) from balance where start_date >= '".$start."' and start_date < '".$end."'";
    $res = re_query($sql);

    while($row = pg_fetch_array($res)) {
	    $num = $row[0];
    }

    return $num;
}

function re_get_cdrs($src,$start,$end)
{
    $arr = "";
    
    $sql = "select id from cdrs where start_ts >= '".$start."' and start_ts < '".$end."' and calling_number = '".$src."'";
    $res = re_query($sql);
    
    $i=0;
    while($row = pg_fetch_array($res)) {
        $arr[$i] = $row[0];
	    $i++;
    }

    return $arr;
}

function re_bal_id_get_bacc($bal_id)
{
    $bacc = "";
    
    $sql = "select b.username from billing_account as b,balance as bal 
            where bal.id = ".$bal_id." and b.id = bal.billing_account_id";
    $res = re_query($sql);
    
    while($row = pg_fetch_array($res)) {
	    $bacc = $row[0];
    }

    return $bacc;
}

function re_bal_id_get_bacc_id($bal_id)
{
    $id = 0;
    
    $sql = "select b.id from billing_account as b,balance as bal 
            where bal.id = ".$bal_id." and b.id = bal.billing_account_id";    
    $res = re_query($sql);

    while($row = pg_fetch_array($res)) {
	    $id = $row[0];
    }
    
    return $id;
}

function re_get_tariff_name($rate_id)
{
    $tariff_name = "";

    $sql = "select name from tariff where id = ".$rate_id."";
    $res = re_query($sql);

    while($row = pg_fetch_array($res)) {
	    $tariff_name = $row[0];
    }
    
    return $tariff_name;
}

function re_get_rating_stat($prefix,$time1,$time2)
{
    $arr = "";

    $sql = "select calling_number.calling_number,sum(call_billsec) as billsec,sum(call_price) as amount 
            from calling_number,rating 
            where rating.billing_account_id = calling_number.billing_account_id and 
            calling_number like '".$prefix."%' and 
            rating.billing_account_id = calling_number.billing_account_id and 
            rating.call_ts >= '".$time1."' and rating.call_ts < '".$time2."' and 
            rating.call_billsec > 0 
            group by calling_number.calling_number 
            order by billsec";
    $res = re_query($sql);

    $i=0;
    while($row = pg_fetch_array($res)) {
        $arr[$i]['clg']     = $row[0];
        $arr[$i]['billsec'] = $row[1];
        $arr[$i]['amount']  = $row[2];

        $i++;
    }

    return $arr;
}

function re_get_tariff_report($bid,$start,$end)
{
    $arr = "";

    $sql = "SELECT tr.id,sum(rt.call_price),sum(rt.call_billsec)
            from rating as rt,cdrs,rate,tariff as tr  
            where rt.billing_account_id = ".$bid." and 
            cdrs.id = rt.call_id and rt.call_price > 0 and 
            cdrs.start_ts >= '".$start."' and cdrs.start_ts < '".$end."' and 
            rt.rate_id = rate.id and tr.id = tariff_id group by tr.id;";
    $res = re_query($sql);

    $c = 0;
    while($row = pg_fetch_array($res)) {
        $arr[$c]['TariffID'] = $row[0];
        $arr[$c]['TariffName'] = re_get_tariff_name($row[0]);

        if($row[1] > 0) $arr[$c]['TariffAmount'] = $row[1];
        else $arr[$c]['TariffAmount'] = 0;

        $arr[$c]['TariffBillsec'] = $row[2];

        $c++;
    }

    $sql = "SELECT tr.id,sum(rt.call_price),sum(rt.call_billsec)
            from rating as rt,cdrs,rate,tariff as tr  
            where rt.billing_account_id = ".$bid." and 
            cdrs.id = rt.call_id and rt.call_price < 0 and 
            cdrs.start_ts >= '".$start."' and cdrs.start_ts < '".$end."' and 
            rt.rate_id = rate.id and tr.id = tariff_id group by tr.id;";
    $res = re_query($sql);

    while($row = pg_fetch_array($res)) {
        $arr[$c]['TariffID'] = $row[0];
        $free_billsec = re_get_tid_free_billsec($row[0]);
        $arr[$c]['TariffName'] = re_get_tariff_name($row[0])." (вкл.минути)";

        if($row[1] > 0) $arr[$c]['TariffAmount'] = $row[1];
        else $arr[$c]['TariffAmount'] = 0;

        $arr[$c]['TariffBillsec'] = $row[2]." / ".$free_billsec;

        $c++;
    }

    $sql = "SELECT tr.id,sum(rt.call_price),sum(rt.call_billsec)
            from rating as rt,cdrs,rate,tariff as tr  
            where rt.billing_account_id = ".$bid." and 
            cdrs.id = rt.call_id and rt.call_price = 0 and 
            cdrs.start_ts >= '".$start."' and cdrs.start_ts < '".$end."' and 
            rt.rate_id = rate.id and tr.id = tariff_id group by tr.id;";
    $res = re_query($sql);

    while($row = pg_fetch_array($res)) {
        $arr[$c]['TariffID'] = $row[0];
        $arr[$c]['TariffName'] = re_get_tariff_name($row[0])." (НЕОГРАНИЧЕНИ)";
        $arr[$c]['TariffAmount'] = 0;
        $arr[$c]['TariffBillsec'] = $row[2];

	    $c++;
    }

    return $arr;
}

function re_get_user_balances($bid,$status,$date)
{
    $balances = '';

    $query = "SELECT id,amount,start_date,end_date,active 
              from balance 
              where billing_account_id = ".$bid."";
    if(!empty($status)) $query .= " and active = '".$status."'";
    if(!empty($date)) $query .= " and start_date like '".$date."%'";
    $query .= " order by start_date";
    $res = re_query($query);
    
    $i = 0;
    while($row = pg_fetch_row($res)) {
        $tariff_report = re_get_tariff_report($bid,$row[2],$row[3]);
        $balances[$i] = array(
				'UserBalanceID' => $row[0],
				'Amount'        => $row[1],
				'StartDate'     => $row[2],
				'EndDate'       => $row[3],
				'BalanceStatus' => $row[4],
				'TariffReport'  => $tariff_report
			    );
        $i++;
    }
    
    return $balances;
}

function re_bacc_id_get_balances($bid)
{
    $balances = '';
    
    $query = "SELECT id,amount,start_date,end_date 
              from balance 
              where billing_account_id = ".$bid." and active = 't'";
    $res = re_query($query);
    
    $i = 0;
    while($row = pg_fetch_row($res)) {
        $balances[$i]['id'] = $row[0];
        $balances[$i]['amount'] = $row[1];
        $balances[$i]['start_date'] = $row[2];
        $balances[$i]['end_date'] = $row[3];

        $i++;
    }

    return $balances;
}

function re_get_balance_id($bid)
{
    $bal_id = 0;

    $query = "SELECT id from balance where billing_account_id = ".$bid." and active = 't'";
    $res = re_query($query);

    $i = 0;
    while($row = pg_fetch_row($res)) {
	    $bal_id = $row[0];
    }

    return $bal_id;
}

function re_get_current_balance($bid)
{
    $bal['start_date'] = NULL;
    $bal['amount'] = 0;

    $date = date('Y-m-d');

    $query = "SELECT start_date,amount 
              from balance where 
              billing_account_id = ".$bid." and 
              start_date <= '".$date."' and 
              end_date >= '".$date."' 
              and active = 't';";
    $res = re_query($query);

    while($row = pg_fetch_row($res)) {
        $bal['start_date'] = $row[0];
        $bal['amount'] = $row[1];
    }

    return $bal;
}

function re_get_balance($bid)
{
    $balances = '';
    $query = "SELECT id,amount,start_date,end_date,active,billing_account_id 
              from balance 
              where id = ".$bid."";
    $res = re_query($query);
    
    $i = 0;
    while($row = pg_fetch_row($res)) {
	    $tariff_report = re_get_tariff_report($row[5],$row[2],$row[3]);
	    $balances[$i] = array(
				'UserBalanceID' => $row[0],
				'Amount'        => $row[1],
				'StartDate'     => $row[2],
				'EndDate'       => $row[3],
				'BalanceStatus' => $row[4],
				'TariffReport'  => $tariff_report
			    );
	    $i++;
    }

    return $balances;
}

function re_get_balance_v2($bid)
{
    $balances = '';

    $query = "SELECT id,amount,start_date,end_date,active,billing_account_id 
              from balance 
              where id = ".$bid."";
    $res = re_query($query);

    $i = 0;
    while($row = pg_fetch_row($res)) {
	    $balances[$i] = array(
				'UserBalanceID' => $row[0],
				'Amount'        => $row[1],
				'StartDate'     => $row[2],
				'EndDate'       => $row[3],
				'BalanceStatus' => $row[4],
			    );
	    $i++;
    }

    return $balances;
}

function re_get_all_balances($end)
{
    $balances = '';

    $query = "SELECT bacc.username,bal.id,bal.amount,bal.start_date,bal.end_date,bal.active 
              from balance as bal,billing_account as bacc 
              where bacc.id = bal.billing_account_id and 
              bal.end_date = '".$end."' and 
              bal.active = 't' and 
              bacc.username like '1%' 
              order by bacc.id";
    $res = re_query($query);
    
    $i=0;
    while($row = pg_fetch_row($res)) {
	    $balances[$i] = array(
			'BillingAccount' => $row[0],
			'UserBalanceID'  => $row[1],
			'Amount'         => $row[2],
			'StartDate'      => $row[3],
			'EndDate'        => $row[4],
			'BalanceStatus'  => $row[5]
			);
	    $i++;
    }

    return $balances;
}

function re_get_all_balances_v2($end,$bacc_like)
{
    $balances = '';

    $query = "SELECT bacc.username,bal.id,bal.amount,bal.start_date,bal.end_date,bal.active 
              from balance as bal,billing_account as bacc 
              where bacc.id = bal.billing_account_id and 
              bal.end_date = '".$end."' and 
              bal.active = 't' and 
              bacc.username like '".$bacc_like."' 
              order by bacc.id";
    $res = re_query($query);

    $i=0;
    while($row = pg_fetch_row($res)) {
	    $balances[$i] = array(
			'BillingAccount' => $row[0],
			'UserBalanceID'  => $row[1],
			'Amount'         => $row[2],
			'StartDate'      => $row[3],
			'EndDate'        => $row[4],
			'BalanceStatus'  => $row[5]
			);
	    $i++;
    }

    return $balances;
}

function re_get_bal_last_updates($bal_id)
{
    $arr = "";

    $sql = "select last_update,last_update_flag from balance where id = ".$bal_id."";
    $res = re_query($sql);

    while($row = pg_fetch_row($res)) {
	    $arr['last'] = $row[0];
	    $arr['lflag'] = $row[1];
    }

    return $arr;
}

function re_get_bill_file($bal_id)
{
    $arr = "";

    $sql = "SELECT username,amount,start_date,end_date,active 
	    from billing_account,balance where balance.id = ".$bal_id." and 
	    billing_account.id = balance.billing_account_id and active = 't';";
    $res = re_query($sql);

    while($row = pg_fetch_array($res)) {
	    $arr['bacc'] = $row[0];
	    $arr['amount'] = $row[1];
	    $arr['start'] = $row[2];
	    $arr['end'] = $row[3];
	    $arr['active'] = $row[4];
    }

    return $arr;
}

function re_get_rated_calls_report($balance_id)
{
    $arr = "";
    $billing_account_id = 0;

    $sql = "SELECT bal.billing_account_id,bal.start_date,bal.end_date from balance as bal where id = ".$balance_id."";
    $res = re_query($sql);

    while($row = pg_fetch_array($res)) {
	    $billing_account_id = $row[0];
	    $start_date = $row[1];
	    $end_date = $row[2];
    }

    $sql = "SELECT cdrs.start_ts,cdrs.calling_number,cdrs.called_number,rt.call_price,rt.call_billsec 
            from rating as rt,cdrs 
            where rt.call_id = cdrs.id and 
            rt.billing_account_id = ".$billing_account_id." and 
            cdrs.start_ts >= '".$start_date."' and 
            cdrs.start_ts < '".$end_date."' order by cdrs.start_ts";
    $res = re_query($sql);

    $i=0;
    while($row = pg_fetch_array($res)) {
	    $arr[$i]['ts']      = $row[0];
	    $arr[$i]['clg']     = $row[1];
	    $arr[$i]['cld']     = $row[2];

        if($row[3] > 0) $arr[$i]['cprice']  = $row[3];
	    else $arr[$i]['cprice'] = 0;

        $arr[$i]['billsec'] = $row[4];

        $i++;
    }

    return $arr;
}

function re_get_rated_calls_report_2($balance_id)
{
    $arr = "";

    $sql = "SELECT bal.billing_account_id,bal.start_date,bal.end_date,bal.amount,bacc.username,cr.name 
            from balance as bal,billing_account as bacc,currency as cr 
            where bal.id = ".$balance_id." and 
            bacc.id = bal.billing_account_id and 
            bacc.currency_id = cr.id";
    $res = re_query($sql);

    while($row = pg_fetch_array($res)) {
        $bill['billing_account_id'] = $row[0];
        $bill['start_date'] = $row[1];
        $bill['end_date'] = $row[2];
        $bill['amount'] = $row[3];
        $bill['bacc'] = $row[4];
        $bill['curr'] = $row[5];
    }
    
    $sql = "SELECT cdrs.start_ts,cdrs.calling_number,cdrs.called_number,rt.call_price,rt.call_billsec,tr.name 
            from rating as rt,cdrs,rate as rr,tariff as tr 
            where rt.call_id = cdrs.id and 
            rt.billing_account_id = ".$bill['billing_account_id']." and 
            cdrs.start_ts >= '".$bill['start_date']."' and 
            cdrs.start_ts < '".$bill['end_date']."' and 
            rt.rate_id = rr.id and 
            rr.tariff_id = tr.id 
            order by cdrs.start_ts
            ";
    $res = re_query($sql);

    $mins = 0;
    $i=0;
    while($row = pg_fetch_array($res)) {
        $arr[$i]['ts']      = $row[0];
        $arr[$i]['clg']     = $row[1];
        $arr[$i]['cld']     = $row[2];

        if($row[3] > 0) $arr[$i]['cprice']  = $row[3];
        else $arr[$i]['cprice'] = 0;

        $arr[$i]['billsec'] = $row[4];
        $mins = $mins + ($arr[$i]['billsec']/60);
        $arr[$i]['tariff'] = $row[5];
        $i++;
    }

    $bill['calls'] = $arr;
    $bill['mins'] = $mins;

    return $bill;
}

function re_get_operators()
{
    $arr = '';

    $query = "select id,username,cdr_server_id 
              from billing_account 
              where leg = 'a+b' 
              order by id;";
    $res = re_query($query);

    $i=0;
    while($row = pg_fetch_row($res)) {
	    $arr[$i]['id'] = $row[0];
	    $arr[$i]['username'] = $row[1];
	    $arr[$i]['cdr_server_id'] = $row[2];
	    $i++;
    }

    return $arr;
}

function re_get_operator_reports($bacc_id)
{
    $arr = '';

    $sql = "SELECT id,start_period,end_period,leg,to_timestamp(create_ts) 
            from report 
            where billing_account_id = ".$bacc_id." order by start_period";
    $res = re_query($sql);

    $i=0;
    while($row = pg_fetch_row($res)) {
	    $arr[$i]['id'] = $row[0];
	    $arr[$i]['start_period'] = $row[1];
	    $arr[$i]['end_period']   = $row[2];
	    $arr[$i]['leg'] = $row[3];
	    $arr[$i]['ts']  = $row[4];

        $i++;
    }

    return $arr;
}

function re_get_operator_traffic($report_id)
{
    $arr = '';

    $sql = "select 
            t.count,t.amount,t.mins,pr.prefix,tr.name 
            from traffic as t,tariff as tr,rate as r,prefix as pr 
            where t.rate_id = r.id and 
            r.prefix_id = pr.id and 
            tr.id = t.tariff_id and 
            t.report_id = ".$report_id." 
            order by pr.prefix;";
    $res = re_query($sql);

    $i=0;
    while($row = pg_fetch_row($res)) {
	    $arr[$i]['count']   = $row[0];
	    $arr[$i]['amount']  = $row[1];
	    $arr[$i]['mins']    = $row[2];
	    $arr[$i]['prefix']  = $row[3];
	    $arr[$i]['tariff']  = $row[4];

        $i++;
    }

    return $arr;
}

function re_get_operator_traffic_brief($report_id)
{
    $arr = '';

    $sql = "select tariff_id,sum(mins),sum(count),sum(amount) 
            from traffic where report_id = ".$report_id." 
            group by tariff_id;";
    $res = re_query($sql);

    $i=0;
    while($row = pg_fetch_row($res)) {
	    $arr[$i]['tariff_id'] = $row[0];
	    $arr[$i]['mins']      = $row[1];
	    $arr[$i]['count']   = $row[2];
	    $arr[$i]['amount']  = $row[3];
	    $arr[$i]['tariff']  = re_get_tariff_name($arr[$i]['tariff_id']);

        $i++;
    }

    return $arr;
}

function re_get_nsg_counters($cdr_server_id,$cur,$step,$var,$val)
{
    $arr = '';

    $query = "select sum(billsec)/60,count(*) 
              from cdrs 
              where 
              ts <= to_timestamp(".$cur.") and 
              ts >= to_timestamp(".$step.") and 
              billsec > 0 and 
              cdr_server_id = ".$cdr_server_id." and 
              ".$var." like '".$val."';
             ";
    $res = re_query($query);

    while($row = pg_fetch_row($res)) {
	    $arr['mins']  = $row[0];
	    $arr['calls'] = $row[1];
    }

    return $arr;
}

function re_get_rating_modes()
{
    $mode = '';

    $query = "select id,name from rating_mode order by id";
    $res = re_query($query);

    $i=0;
    while($row = pg_fetch_row($res)) {
	    $mode[$i]['id'] = $row[0];
	    $mode[$i]['name'] = $row[1];

        $i++;
    }

    return $mode;
}

function re_get_rating_mode($id)
{
    $mode = '';

    $query = "select name from rating_mode where id = ".$id."";
    $res = re_query($query);

    while($row = pg_fetch_row($res)) {
		$mode = $row[0];
    }

    return $mode;
}

function re_get_rating_mode_id($mode)
{
    $id = 0;

    $query = "select id from rating_mode where name = '".$mode."'";
    $res = re_query($query);

    while($row = pg_fetch_row($res)) {
	    $id = $row[0];
    }

    return $id;
}

function re_get_account_code_bid($bid)
{
    $acc = "";

    $query = "select account_code 
              from account_code 
              where billing_account_id = ".$bid."";
    $res = re_query($query);

    while($row = pg_fetch_row($res)) {
	    $acc = $row[0];
    }

    return $acc;
}


function re_get_billing_account_id($billing_account)
{
    $id = 0;

    $query = "select id from billing_account where username = '".$billing_account."'";
    $res = re_query($query);

    while($row = pg_fetch_row($res)) {
	    $id = $row[0];
    }

    return $id;
}

function re_get_bacc($bacc_id)
{
    $bacc = "";

    $query = "select username,billing_day,day_of_payment from billing_account where id = ".$bacc_id."";
    $res = re_query($query);

    while($row = pg_fetch_row($res)) {
	    $bacc['acc'] = $row[0];
	    $bacc['bday'] = $row[1];
	    $bacc['dday'] = $row[2];
    }

    return $bacc;
}

function re_get_baccs($username)
{
    $bacc = "";

    $query = "select bacc.id,bacc.username,bacc.billing_day,bacc.leg,cr.name
              from billing_account as bacc,currency as cr
              where bacc.currency_id = cr.id order by bacc.id";
    if(!empty($username)) $query .= " and username like '".$username."%'";

    $res = re_query($query);

    $i=0;
    while($row = pg_fetch_row($res)) {
	    $bacc[$i]['id'] = $row[0];
	    $bacc[$i]['acc'] = $row[1];
	    $bacc[$i]['bday'] = $row[2];
	    $bacc[$i]['leg'] = $row[3];
	    $bacc[$i]['curr'] = $row[4];

        $i++;
    }

    return $bacc;
}

function re_get_currency()
{
    $arr = "";

    $sql = "select id,name from currency order by id;";
    $res = re_query($sql);

    $i = 0;
    while($row = pg_fetch_row($res)) {
	    $arr[$i]['id']   = $row[0];
	    $arr[$i]['name'] = $row[1];

        $i++;
    }

    return $arr;
}

function re_get_cdr_servers()
{
    $arr = "";

    $sql = "select cdr_servers.id,cdr_profiles.profile_name from cdr_servers,cdr_profiles where cdr_servers.cdr_profiles_id = cdr_profiles.id";
    $res = re_query($sql);

    $i = 0;
    while($row = pg_fetch_row($res)) {
	    $arr[$i]['id']   = $row[0];
	    $arr[$i]['server_name'] = $row[1];

        $i++;
    }

    return $arr;
}

function re_get_round_modes()
{
    $arr = "";

    $sql = "select id,name from round_mode order by id;";
    $res = re_query($sql);

    $i = 0;
    while($row = pg_fetch_row($res)) {
	    $arr[$i]['id']   = $row[0];
	    $arr[$i]['name'] = $row[1];

        $i++;
    }

    return $arr;
}

function re_get_bacc_data($username)
{
    $arr = "";

    $sql = "SELECT 
            bacc.id,bacc.username,cr.name,bacc.leg,cdr.server_name,bacc.billing_day,rm.name,bacc.day_of_payment,
            bacc.cdr_server_id,bacc.round_mode_id,bacc.currency_id 
            from 
            cdr_servers as cdr,billing_account as bacc,currency as cr,round_mode as rm 
            where cdr.id = bacc.cdr_server_id and 
            rm.id = bacc.round_mode_id and 
            cr.id = bacc.currency_id and 
            bacc.username like '".$username."%';";
    $res = re_query($sql);

    $i = 0;
    while($row = pg_fetch_row($res)) {
        $arr[$i]['id'] = $row[0];
        $arr[$i]['username'] = $row[1];
        $arr[$i]['currency'] = $row[2];
        $arr[$i]['leg'] = $row[3];
        $arr[$i]['cdr_server'] = $row[4];
        $arr[$i]['bday']  = $row[5];
        $arr[$i]['round'] = $row[6];
        $arr[$i]['dday']  = $row[7];
        $arr[$i]['cdr_server_id'] = $row[8];
        $arr[$i]['round_mode_id'] = $row[9];
        $arr[$i]['currency_id']   = $row[10];

        $i++;
    }

    return $arr;
}

function re_get_bacc_data_v2($username)
{
    $arr = "";

    $sql = "SELECT 
            bacc.id,bacc.username,cr.name,bacc.leg,cdr_pr.profile_name,bacc.billing_day,rm.name,bacc.day_of_payment,
            bacc.cdr_server_id,bacc.round_mode_id,bacc.currency_id 
            from 
            cdr_servers as cdr,cdr_profiles as cdr_pr,billing_account as bacc,currency as cr,round_mode as rm 
            where cdr.id = bacc.cdr_server_id and 
            cdr.cdr_profiles_id = cdr_pr.id and 
            rm.id = bacc.round_mode_id and 
            cr.id = bacc.currency_id and 
            bacc.username like '".$username."%';";
    $res = re_query($sql);

    $i = 0;
    while($row = pg_fetch_row($res)) {
        $arr[$i]['id'] = $row[0];
        $arr[$i]['username'] = $row[1];
        $arr[$i]['currency'] = $row[2];
        $arr[$i]['leg'] = $row[3];
        $arr[$i]['cdr_server'] = $row[4];
        $arr[$i]['bday']  = $row[5];
        $arr[$i]['round'] = $row[6];
        $arr[$i]['dday']  = $row[7];
        $arr[$i]['cdr_server_id'] = $row[8];
        $arr[$i]['round_mode_id'] = $row[9];
        $arr[$i]['currency_id']   = $row[10];

        $i++;
    }

    return $arr;
}

function re_get_bacc_data_2($bacc_id)
{
    $arr = "";

    $sql = "SELECT 
            bacc.id,bacc.username,cr.name,bacc.leg,cdr.server_name,bacc.billing_day,rm.name,bacc.day_of_payment,
            bacc.cdr_server_id,bacc.round_mode_id,bacc.currency_id 
            from 
            cdr_servers as cdr,billing_account as bacc,currency as cr,round_mode as rm 
            where cdr.id = bacc.cdr_server_id and 
            rm.id = bacc.round_mode_id and 
            cr.id = bacc.currency_id and 
            bacc.id = ".$bacc_id.";";
    $res = re_query($sql);

    while($row = pg_fetch_row($res)) {
        $arr['id'] = $row[0];
        $arr['username'] = $row[1];
        $arr['currency'] = $row[2];
        $arr['leg'] = $row[3];
        $arr['cdr_server'] = $row[4];
        $arr['bday']  = $row[5];
        $arr['round'] = $row[6];
        $arr['dday']  = $row[7];
        $arr['cdr_server_id'] = $row[8];
        $arr['round_mode_id'] = $row[9];
        $arr['currency_id']   = $row[10];
    }

    return $arr;
}

function re_get_bacc_data_2_v2($bacc_id)
{
    $arr = "";

    $sql = "SELECT 
            bacc.id,bacc.username,cr.name,bacc.leg,cdr_pr.profile_name,bacc.billing_day,rm.name,bacc.day_of_payment,
            bacc.cdr_server_id,bacc.round_mode_id,bacc.currency_id 
            from 
            cdr_servers as cdr,cdr_profiles as cdr_pr,billing_account as bacc,currency as cr,round_mode as rm 
            where cdr.id = bacc.cdr_server_id and 
            cdr.cdr_profiles_id = cdr_pr.id and 
            rm.id = bacc.round_mode_id and 
            cr.id = bacc.currency_id and 
            bacc.id = ".$bacc_id.";";
    $res = re_query($sql);

    while($row = pg_fetch_row($res)) {
        $arr['id'] = $row[0];
        $arr['username'] = $row[1];
        $arr['currency'] = $row[2];
        $arr['leg'] = $row[3];
        $arr['cdr_server'] = $row[4];
        $arr['bday']  = $row[5];
        $arr['round'] = $row[6];
        $arr['dday']  = $row[7];
        $arr['cdr_server_id'] = $row[8];
        $arr['round_mode_id'] = $row[9];
        $arr['currency_id']   = $row[10];
    }

    return $arr;
}

function re_get_prefix()
{
    $arr = "";

    $query = "select id,prefix,comm from prefix";
    $res = re_query($query);

    $i=0;
    while($row = pg_fetch_row($res)) {
	    $arr[$i]['id'] = $row[0];
	    $arr[$i]['prefix'] = $row[1];
	    $arr[$i]['comm'] = $row[2];

        $i++;
    }

    return $arr;
}

function re_get_bills($id)
{
    $bacc = "";

    $query = "select balance.id,balance.amount,
              balance.last_update,balance.start_date,balance.end_date,balance.active,billing_account.username
              from balance,billing_account
              where balance.billing_account_id = ".$id." and balance.billing_account_id = billing_account.id";
    $res = re_query($query);

    $i=0;
    while($row = pg_fetch_row($res)) {
        $bacc[$i]['id'] = $row[0];
        $bacc[$i]['amount'] = $row[1];
        $bacc[$i]['last_update'] = $row[2];
        $bacc[$i]['start_date'] = $row[3];
        $bacc[$i]['end_date'] = $row[4];
        $bacc[$i]['active'] = $row[5];
        $bacc[$i]['username'] = $row[6];

        $i++;
    }

    return $bacc;
}

function re_get_balance2($id)
{
    $bal = "";

    $query = "select balance.id,balance.amount,
              balance.last_update,balance.start_date,balance.end_date,balance.active,billing_account.username
              from balance,billing_account
              where balance.id = ".$id." and balance.billing_account_id = billing_account.id";
    $res = re_query($query);

    $i=0;
    while($row = pg_fetch_row($res)) {
        $bal[$i]['id'] = $row[0];
        $bal[$i]['amount'] = $row[1];
        $bal[$i]['last_update'] = $row[2];
        $bal[$i]['start_date'] = $row[3];
        $bal[$i]['end_date'] = $row[4];
        $bal[$i]['active'] = $row[5];
        $bal[$i]['username'] = $row[6];

        $i++;
    }

    return $bal;
}

function re_get_free_billsec($free_billsec_id)
{
    $sec = 0;

    $sql = "select free_billsec from free_billsec where id = ".$free_billsec_id.";";
    $res = re_query($sql);

    while($row = pg_fetch_row($res)) {
        $sec = $row[0];
    }

    return $sec;
}

function re_get_tid_free_billsec($tid)
{
    $sec = 0;

    $sql = "SELECT fr.free_billsec from tariff as tr,free_billsec as fr 
            where tr.id = ".$tid." and tr.free_billsec_id = fr.id;";
    $res = re_query($sql);

    while($row = pg_fetch_row($res)) {
        $sec = $row[0];
    }

    return $sec;
}

function re_get_free_billsec_bal()
{
    $arr = "";

    $sql = "select id,balance_id from free_billsec_balance order by id;";
    $res = re_query($sql);

    $i=0;
    while($row = pg_fetch_row($res)) {
	    $arr[$i]['id'] = $row[0];
	    $arr[$i]['balance_id'] = $row[1];

        $i++;
    }

    return $arr;
}

function re_get_free_billsec_bal_id($bid)
{
    $id = 0;

    $sql = "select id from free_billsec_balance where balance_id = ".$bid."";
    $res = re_query($sql);

    while($row = pg_fetch_row($res)) {
	    $id = $row[0];
    }

    return $id;
}

function re_get_calc_functions($tariff_id)
{
    $arr = "";

    $sql = "select id,tariff_id,pos,delta_time,fee,iterations from calc_function where tariff_id = ".$tariff_id." order by pos;";
    $res = re_query($sql);

    $i = 0;
    while($row = pg_fetch_row($res)) {
        $arr[$i]['id'] = $row[0];
        $arr[$i]['tariff_id'] = $row[1];
        $arr[$i]['pos'] = $row[2];
        $arr[$i]['delta_time'] = $row[3];
        $arr[$i]['fee'] = $row[4];
        $arr[$i]['iterations'] = $row[5];

        $i++;
    }

    return $arr;
}

function re_get_bplan_name($bid)
{
    $sql = "select name from bill_plan where id = ".$bid.";";
    $res = re_query($sql);

    while($row = pg_fetch_row($res)) {
	    return $row[0];
    }
}

function re_clg_get_bplan_id($clg)
{
    
}

function re_get_bill_plans_tree_id($root_id)
{
    $arr = "";

    $sql = "select tree.bill_plan_id,bp.name 
            from bill_plan_tree as tree,bill_plan as bp 
            where bp.id = tree.bill_plan_id and 
            tree.root_bplan_id = ".$root_id.";";
    $res = re_query($sql);

    $i=0;
    while($row = pg_fetch_row($res)) {
        $arr[$i]['id'] = $row[0];
        $arr[$i]['name'] = $row[1];

        $i++;
    }

    return $arr;
}

function re_get_rates_bplan_id($bplan_id)
{
    $arr = "";

    $sql = "select rt.id,pr.id,pr.prefix,tr.id,tr.name,tr.free_billsec_id 
            from rate as rt,prefix as pr,tariff as tr 
            where tr.id = rt.tariff_id and 
            rt.prefix_id = pr.id and 
            rt.bill_plan_id = ".$bplan_id.";";
    $res = re_query($sql);

    $i = 0;
    while($row = pg_fetch_row($res)) {
        $arr[$i]['rate_id'] = $row[0];
        $arr[$i]['prefix_id'] = $row[1];
        $arr[$i]['prefix']    = $row[2];
        $arr[$i]['tariff_id']   = $row[3];
        $arr[$i]['tariff_name'] = $row[4];
        $arr[$i]['free_billsec_id'] = $row[5];

        $i++;
    }

    return $arr;
}

function re_get_bill_plans()
{
    $query = "select id,name,to_timestamp(start_period),to_timestamp(end_period) from bill_plan";
    $res = re_query($query);

    $i = 0;
    while($row = pg_fetch_row($res)) {
        $arr[$i]['id'] = $row[0];
        $arr[$i]['name'] = $row[1];
        $arr[$i]['start_period'] = $row[2];
        $arr[$i]['end_period'] = $row[3];

        $i++;
    }

    return $arr;
}

function re_get_bill_plans_2($bplan_id)
{
    $query = "select id,name,to_timestamp(start_period),to_timestamp(end_period) from bill_plan where id = ".$bplan_id."";
    $res = re_query($query);

    $i = 0;
    while($row = pg_fetch_row($res)) {
        $arr[$i]['id'] = $row[0];
        $arr[$i]['name'] = $row[1];
        $arr[$i]['start_period'] = $row[2];
        $arr[$i]['end_period'] = $row[3];

        $i++;
    }

    return $arr;
}

function re_get_bill_plan_tree()
{
    $query = "SELECT root_bplan_id from bill_plan_tree group by root_bplan_id;";
    $res = re_query($query);

    $i = 0;
    while($row = pg_fetch_row($res)) {
	    $arr[$i]['id'] = $row[0];
	    $i++;
    }

    $p=0;$i=0;
    while($arr[$i]['id']) {
        $query2 = "select id,name,to_timestamp(start_period),to_timestamp(end_period) from bill_plan where id = ".$arr[$i]['id']."";
        $res2 = re_query($query2);

        while($row = pg_fetch_row($res2)) {
	        $arr2[$p]['id'] = $row[0];
	        $arr2[$p]['name'] = $row[1];
	        $arr2[$p]['start_period'] = $row[2];
	        $arr2[$p]['end_period'] = $row[3];

            $p++;
	    }

        $i++;
    }

    return $arr2;
}

function re_get_bill_plan_id($bill_plan)
{
    $id = 0;

    $query = "select id from bill_plan where name = '".$bill_plan."'";
    $res = re_query($query);

    while($row = pg_fetch_row($res)) {
	    $id = $row[0];
    }

    return $id;
}

function re_get_bacc_num_clg($bacc_id)
{
    $num = 0;

    $query = "SELECT count(*) from calling_number where billing_account_id = ".$bacc_id.";";
    $res = re_query($query);

    while($row = pg_fetch_row($res)) {
	    $num = $row[0];
    }

    return $num;
}

function re_insert_bill_plan($bplan)
{
    $query = "insert into bill_plan (name,start_period,end_period) values ('".$bplan['name']."'";

    if(!empty($bplan['start_period'])) $query .= ",extract(epoch from timestamp '".$bplan['start_period']."') ";
    else $query .= ",0";

    if(!empty($bplan['end_period'])) $query .= ",extract(epoch from timestamp '".$bplan['end_period']."'))";
    else $query .= ",0";

    $query .= ")";

    re_query($query);

    return re_get_bill_plan_id($bplan['name']);
}

function re_insert_billing_account($user)
{
    $username_test = $user['billing_account'];

    $dday = 0;

    $query = "insert into billing_account
              (username,currency_id,leg,cdr_server_id,billing_day,day_of_payment)
              values ('".$user['billing_account']."',".$user['curr_id'].",'".$user['leg']."',".$user['cdr_server_id'].",'".$user['billing_day']."',".$dday.")";
    re_query($query);
}

function re_insert_billing_account_2($user)
{
    $username_test = $user['username'];

    if(empty($user['dday']))
    {
//	if($user['billing_day'] == '01') $dday = 14;
//	else if($user['billing_day'] == '11') $dday = 25;
//	else if($user['billing_day'] == '21') $dday = 5;
//	else 
$dday = 0;
    }
    else $dday = $user['dday'];

    if(empty($user['round_mode_id'])) $rr = 0;
    else $rr = $user['round_mode_id'];

    $query = "insert into billing_account 
              (username,currency_id,leg,cdr_server_id,billing_day,day_of_payment,round_mode_id) 
              values ('".$user['username']."',".$user['curr_id'].",'".$user['leg']."',".$user['cdr_server_id'].",'".$user['billing_day']."',".$dday.",".$rr.")";
    re_query($query);

    return re_get_billing_account_id($user['username']);
}

function re_get_rating_account_id($rating_mode,$rating_account)
{
    $id = 0;

    $query = "select id from ".$rating_mode." where ".$rating_mode." = '".$rating_account."'";
    $res = re_query($query);

    while($row = pg_fetch_row($res)) {
	    $id = $row[0];
    }

    return $id;
}

function re_get_racc_bacc_id($rmode,$bacc_id)
{
    $racc = "";

    $query = "select ".$rmode." from ".$rmode." where billing_account_id = ".$bacc_id."";
    $res = re_query($query);

    while($row = pg_fetch_row($res)) {
	    $racc = $row[0];
    }

    return $racc;
}

function re_get_bdata($clg)
{
    $bdata = "";

    $query = "SELECT bacc.billing_day,bacc.day_of_payment,bacc.id,clg.id 
              from calling_number as clg,billing_account as bacc 
              where clg.billing_account_id = bacc.id and 
              clg.calling_number = '".$clg."';";
    $res = re_query($query);

    while($row = pg_fetch_row($res)) {
	    $bdata['bday'] = $row[0];
	    $bdata['dday'] = $row[1];
	    $bdata['bacc'] = $row[2];
	    $bdata['clg']  = $row[3];
    }

    return $bdata;
}

function re_get_racc($rmode,$racc_id)
{
    $racc = "";

    $query = "select ".$rmode." from ".$rmode." where id = ".$racc_id."";
    $res = re_query($query);

    while($row = pg_fetch_row($res)) {
	    $racc = $row[0];
    }

    return $racc;
}

function re_get_racc_bplan_id($rmode,$racc_id)
{
    $id = 0;

    $query = "select bill_plan_id from ".$rmode."_deff where ".$rmode."_id = ".$racc_id."";
    $res = re_query($query);

    while($row = pg_fetch_row($res)) {
	    $id = $row[0];
    }

    return $id;
}

function re_get_racc_deff_id($rmode,$racc_id)
{
    $id = 0;

    $query = "select id from ".$rmode."_deff where ".$rmode."_id = ".$racc_id."";
    $res = re_query($query);

    while($row = pg_fetch_row($res)) {
	    $id = $row[0];
    }

    return $id;
}

function re_get_racc_acc($rmode,$racc_id)
{
    $acc = "";

    $query = "select ".$rmode." from ".$rmode." where id = ".$racc_id."";
    $res = re_query($query);

    while($row = pg_fetch_row($res)) {
	    $acc = $row[0];
    }

    return $acc;
}

function re_get_racc_2($rmode,$racc)
{
    $arr = "";

    $query = "select id,billing_account_id,".$rmode." from ".$rmode." where ".$rmode." like '".$racc."%' order by ".$rmode."";
    $res = re_query($query);

    $i=0;
    while($row = pg_fetch_row($res)) {
	    $arr[$i]['id'] = $row[0];
	    $arr[$i]['racc'] = $row[2];
	    $arr[$i]['billing_account_id'] = $row[1];
	    $arr[$i]['rmode'] = $rmode;

        $i++;
    }

    return $arr;
}

function re_get_racc_3($rmode,$bid)
{
    $arr = "";

    $query = "select racc.id,racc.".$rmode.",df.bill_plan_id,df.sm_bill_plan_id 
              from ".$rmode." as racc,".$rmode."_deff as df 
              where racc.id  = df.".$rmode."_id and 
              racc.billing_account_id = ".$bid." 
              order by racc.".$rmode."";

    $res = re_query($query);

    $i=0;
    while($row = pg_fetch_row($res)) {
	    $arr[$i]['id'] = $row[0];
	    $arr[$i]['racc'] = $row[1];
	    $arr[$i]['bill_plan_id'] = $row[2];
	    $arr[$i]['bplan'] = re_get_bplan_name($row[2]);
	    $arr[$i]['sm_bill_plan_id'] = $row[3];
	    $arr[$i]['sm_bplan'] = re_get_bplan_name($row[3]);

        $i++;
    }

    return $arr;
}

function re_get_rmode_bplan($rmode,$racc_id)
{
    $name = "";

    $sql = "select bp.name from ".$rmode."_deff as tm,bill_plan as bp 
            where bp.id = tm.bill_plan_id and 
            tm.calling_number_id = ".$racc_id.";";
    $res = re_query($sql);

    while($row = pg_fetch_row($res)) {
	    $name = $row[0];
    }

    return $name;
}

function re_insert_rating_account($rating_mode,$rating_account,$billing_account_id,$bill_plan_id,$sm_bill_plan_id)
{
    $id = re_get_rating_account_id($rating_mode,$rating_account);

    if($id == 0) {
	    $query = "insert into ".$rating_mode." (".$rating_mode.",billing_account_id) values ('".$rating_account."',".$billing_account_id.")";
	    re_query($query);
    }

    $id = re_get_rating_account_id($rating_mode,$rating_account);

    if($rating_mode == 'calling_number') {
	    $query = "insert into ".$rating_mode."_deff
              (".$rating_mode."_id,bill_plan_id,sm_bill_plan_id)
              values
              (".$id.",".$bill_plan_id.",".$sm_bill_plan_id.")";
    } else {
	    $query = "insert into ".$rating_mode."_deff
              (".$rating_mode."_id,bill_plan_id)
              values
              (".$id.",".$bill_plan_id.")";
    }

    if($id) re_query($query);
    else { 
	    return 0;
    }
}
 
function re_insert_rating_account_2($rating_mode,$rating_account,$billing_account_id,$bill_plan_id,$clg_nadi,$cld_nadi)
{  
    $query = "insert into ".$rating_mode." 
              (".$rating_mode.",billing_account_id) 
              values ('".$rating_account."',".$billing_account_id.")";
    re_query($query);

    $id = re_get_rating_account_id($rating_mode,$rating_account);

    $query = "insert into ".$rating_mode."_deff 
              (".$rating_mode."_id,bill_plan_id,clg_nadi,cld_nadi)
              values
              (".$id.",".$bill_plan_id.",".$clg_nadi.",".$cld_nadi.")";
    if($id) re_query($query);
    else 
    { 
		return 0;
    }
}

function re_get_pcard_type_id($pcard_type)
{
    $id = 0;

    $query = "select id
              from pcard_type
              where 
              name like '".$pcard_type."%'";
    $result = re_query($query);

    while($row = pg_fetch_row($result)) {
		$id = $row[0];
    }

    return $id;
}

function re_get_pcard_status_id($pcard_status)
{
    $id = 0;

    $query = "select id
              from pcard_status
              where 
              status like '".$pcard_status."%'";
    $result = re_query($query);

    while($row = pg_fetch_row($result)) {
		$id = $row[0];
    }

    return $id;
}

function re_get_pcard_id($bacc,$type_id,$status_id,$amount,$start,$end)
{
    $id = 0;

    $query = "select id
              from pcard
              where 
              billing_account_id = ".$bacc." and
              pcard_type_id = ".$type_id." and
              pcard_status_id = ".$status_id." and
              amount = ".$amount." and
              start_date = '".$start."' and
              end_date = '".$end."'";
    $result = re_query($query);

    while($row = pg_fetch_row($result)) {
	    $id = $row[0];
    }

    return $id;
}

function re_get_pcard_id_2($bacc)
{
    $id = 0;

    $query = "select id from pcard where billing_account_id = ".$bacc."";
    $result = re_query($query);

    while($row = pg_fetch_row($result)) {
	    $id = $row[0];
    }

    return $id;
}

function re_get_pcard_data($bacc_id)
{
    $arr = "";

    $query = "SELECT 
              pc.amount,ps.status,pt.name,pc.start_date,pc.end_date,pc.last_update,pc.id,pt.id,ps.id,pc.sim,
              pc.call_number 
              from pcard as pc,pcard_type as pt,pcard_status as ps 
              where pc.billing_account_id = ".$bacc_id." and 
              pc.pcard_status_id = ps.id and 
              pc.pcard_type_id = pt.id 
              ";
    $result = re_query($query);

    while($row = pg_fetch_row($result)) {
	    $arr['pc_amount'] = $row[0];
	    $arr['pc_status'] = $row[1];
	    $arr['pc_type']   = $row[2];
	    $arr['pc_sdate']  = $row[3];
	    $arr['pc_edate']  = $row[4];
	    $arr['pc_last']   = $row[5];
	    $arr['pc_id']     = $row[6];
	    $arr['pc_type_id']   = $row[7];
	    $arr['pc_status_id'] = $row[8];
	    $arr['pc_sim'] = $row[9];
	    $arr['pc_num'] = $row[10];
    }

    return $arr;
}

function re_get_pcard_data_2($bacc_id)
{
    $arr = "";

    $query = "SELECT 
              pc.amount,ps.status,pt.name,pc.start_date,pc.end_date,pc.last_update,pc.id,pt.id,ps.id,pc.sim,
              pc.call_number 
              from pcard as pc,pcard_type as pt,pcard_status as ps 
              where pc.billing_account_id = ".$bacc_id." and 
              pc.pcard_status_id = ps.id and 
              pc.pcard_type_id = pt.id 
              order by pc.start_date";
    $result = re_query($query);

    $i=0;
    while($row = pg_fetch_row($result)) {
	    $arr[$i]['pc_amount'] = $row[0];
	    $arr[$i]['pc_status'] = $row[1];
	    $arr[$i]['pc_type']   = $row[2];
	    $arr[$i]['pc_sdate']  = $row[3];
	    $arr[$i]['pc_edate']  = $row[4];
	    $arr[$i]['pc_last']   = $row[5];
	    $arr[$i]['pc_id']     = $row[6];
	    $arr[$i]['pc_type_id']   = $row[7];
	    $arr[$i]['pc_status_id'] = $row[8];
	    $arr[$i]['pc_sim'] = $row[9];
	    $arr[$i]['pc_num'] = $row[10];

	    $i++;
    }

    return $arr;
}

function re_get_blocked_pcards()
{
    $arr = "";

    $query = "SELECT pc.id,pc.last_update,cn.calling_number,bacc.username,bacc.id 
              from pcard as pc,calling_number as cn,billing_account as bacc 
              where pc.pcard_status_id = 2 and 
              pc.billing_account_id = cn.billing_account_id and 
              bacc.id = pc.billing_account_id 
              order by pc.last_update;";
    $result = re_query($query);

    $i = 0;
    while($row = pg_fetch_row($result)) {
	    $arr[$i]['id'] = $row[0];
	    $arr[$i]['last_update'] = $row[1];
	    $arr[$i]['calling_number'] = $row[2];
	    $arr[$i]['bacc_username'] = $row[3];
	    $arr[$i]['bacc_id'] = $row[4];

        $i++;
    }

    return $arr;
}

function re_get_inuse_pcards()
{
    $arr = "";

    $query = "SELECT pc.id,pc.last_update,cn.calling_number,bacc.username,bacc.id 
              from pcard as pc,calling_number as cn,billing_account as bacc 
              where pc.pcard_status_id = 1 and pc.sim > 0 and 
              pc.billing_account_id = cn.billing_account_id and 
              bacc.id = pc.billing_account_id 
              order by pc.billing_account_id;";
    $result = re_query($query);

    $i = 0;
    while($row = pg_fetch_row($result)) {
	    $arr[$i]['id'] = $row[0];
	    $arr[$i]['last_update'] = $row[1];
	    $arr[$i]['calling_number'] = $row[2];
	    $arr[$i]['bacc_username'] = $row[3];
	    $arr[$i]['bacc_id'] = $row[4];

        $i++;
    }

    return $arr;
}

function re_insert_clg_history($clg,$bacc_id,$bplan_id,$operation)
{
    $sql = "insert into clg_history 
            (calling_number,billing_account_id,bill_plan_id,last_update,operation) 
            values ('".$clg."',".$bacc_id.",".$bplan_id.",'now()','".$operation."')";
    re_query($sql);
}

function re_insert_pcard($bacc_id,$pcard_tp,$pcard_sts,$amount,$start_date,$end_date)
{
    $query = "insert into pcard (billing_account_id,pcard_status_id,pcard_type_id,amount,start_date,end_date,last_update) 
              values (".$bacc_id.",".$pcard_sts.",".$pcard_tp.",".$amount.",'".$start_date."','".$end_date."','now()')";

    re_query($query);
}

function re_insert_pcard_2($pcard)
{
    $query = "insert into pcard (billing_account_id,pcard_status_id,pcard_type_id,amount,start_date,end_date,last_update) 
              values (".$pcard['bacc_id'].",".$pcard['pc_status_id'].",".$pcard['pc_type_id'].","
              .$pcard['pc_amount'].",'".$pcard['pc_sdate']."','".$pcard['pc_edate']."','now()')";
    re_query($query);
}

function re_delete_balances($bid)
{
    $sql = "delete from balance where billing_account_id = ".$bid.";";
    re_query($sql);
}

function re_delete_balance($id)
{
    $sql = "delete from balance where id = ".$id.";";
    re_query($sql);
}

function re_delete_free_balance($id)
{
    $sql = "delete from free_billsec_balance where id = ".$id.";";
    re_query($sql);
}

function re_delete_bacc($bid)
{
    $sql = "delete from billing_account where id = ".$bid.";";
    re_query($sql);
}

function re_delete_pcard($bid)
{
    $sql = "delete from pcard where billing_account_id = ".$bid.";";
    re_query($sql);
}

function re_delete_pcard_2($id)
{
    $sql = "delete from pcard where id = ".$id.";";
    re_query($sql);
}

function re_delete_racc($rmode,$racc_id)
{
    $sql = "delete from ".$rmode." where id = ".$racc_id.";";
    re_query($sql);
}

function re_delete_racc_bid($rmode,$bid)
{
    $sql = "delete from ".$rmode." where billing_account_id = ".$bid.";";
    re_query($sql);
}

function re_delete_racc_deff($rmode,$racc_id)
{
    $sql = "delete from ".$rmode."_deff where ".$rmode."_id=".$racc_id.";";
    re_query($sql);
}

function re_delete_rating($cdr_id)
{
    $sql = "delete from rating where call_id = ".$cdr_id."";
    re_query($sql);
}

function re_update_bplan($racc_id,$bplan_id)
{
    $sql = "update calling_number_deff set bill_plan_id = ".$bplan_id." where calling_number_id = ".$racc_id.";";
    re_query($sql);
}

function re_update_sm_bplan($racc_id,$bplan_id)
{
    $sql = "update calling_number_deff set sm_bill_plan_id = ".$bplan_id." where calling_number_id = ".$racc_id.";";
    re_query($sql);
}

function re_update_cdr($cdr_id)
{
    $sql = "update cdrs set leg_a = 0 where id = ".$cdr_id."";
    re_query($sql);
}

function re_update_bacc_days($bacc)
{
    $sql = "update billing_account 
            set billing_day = '".$bacc['bday']."',
            day_of_payment = ".$bacc['dday']." 
            where id = ".$bacc['id']."";
    re_query($sql);
}

function re_update_bacc($bacc)
{
    $sql = "update billing_account 
            set username = '".$bacc['username']."',
            currency_id = ".$bacc['curr_id'].",
            cdr_server_id = ".$bacc['cdr_server_id'].",
            round_mode_id = ".$bacc['round_mode_id'].",
            leg = '".$bacc['leg']."',
            billing_day = '".$bacc['bday']."',
            day_of_payment = ".$bacc['dday']." 
            where id = ".$bacc['id']."";
    re_query($sql);
}

function re_update_bacc_2($id,$usr,$bday)
{
    $sql = "update billing_account 
            set username = '".$usr."',
            billing_day = '".$bday."' 
            where id = ".$id."";
    re_query($sql);
}

function re_update_bacc_username($id,$usr)
{
    $sql = "update billing_account set username = '".$usr."' where id = ".$id."";
    re_query($sql);
}

function re_update_credit_limit($pcard_id,$credit)
{
    $query = "update pcard set amount = ".$credit.",last_update ='now()' where id = ".$pcard_id."";
    re_query($query);
}

function re_update_credit_limit_v2($bacc_id,$credit)
{
    $query = "update pcard set amount = ".$credit.",last_update ='now()' where billing_account_id = ".$bacc_id."";
    re_query($query);
}

function re_update_pcard_status($pcard_id,$status_id)
{
    $query = "update pcard set pcard_status_id = ".$status_id.",last_update ='now()' where id = ".$pcard_id."";
    re_query($query);
}

function re_pcard_deactive($pcard_id)
{
    $query = "update pcard set pcard_status_id = 0,last_update ='now()',end_date = '".date("Y-m-d")."' where id = ".$pcard_id."";
    re_query($query);
}

function re_update_balance_active($id,$status)
{
    $query = "update balance set active = '".$status."',last_update_flag ='now()' where id = ".$id."";
    re_query($query);
}

function re_update_balance_end_date($id,$ed)
{
    $query = "update balance set end_date = '".$ed."',last_update_flag ='now()' where id = ".$id."";
    re_query($query);
}

function re_update_pcard_call_number($pcard_id,$num)
{
    $query = "update pcard set call_number = ".$num.",last_update ='now()' where id = ".$pcard_id."";
    re_query($query);
}

function re_update_pcard($pcard)
{
    $query = "update pcard set 
              amount = ".$pcard['pc_amount'].",
              last_update ='now()',
              start_date = '".$pcard['pc_sdate']."',
              end_date = '".$pcard['pc_edate']."',
              sim = ".$pcard['pc_sim'].",
              call_number = ".$pcard['pc_num'].",
              pcard_status_id = ".$pcard['pc_status_id']." 
              where id = ".$pcard['aid']."";
    re_query($query);
}

function re_create_account($user)
{
    if(empty($user['rating_mode'])) return 0;

    $username_test = $user['billing_account'];

    // Insert BillingAccount
    $bacc_check_flag = 0;

bacc_check:
    $billing_account_id = re_get_billing_account_id($user['billing_account']);

    if(($billing_account_id == 0)&&($bacc_check_flag == 0)) {
	    re_insert_billing_account($user);
	    $bacc_check_flag = 1;
	    goto bacc_check;
    }

    if(($billing_account_id == 0)&&($bacc_check_flag)) {
	    return 0;
    }

    $re_bill_plan_id = re_get_bill_plan_id($user['re_bill_plan']);

    if($re_bill_plan_id == 0) {
	    return 0;
    }

    $sm_bill_plan_id = re_get_bill_plan_id($user['sm_bill_plan']);

    // Insert RatingAccount
    $racc_check_flag = 0;

racc_check:
    $rating_account_id = re_get_rating_account_id($user['rating_mode'],$user['rating_account']);

    if(($rating_account_id == 0)&&($racc_check_flag == 0)) {
	    if(re_get_rating_mode_id($user['rating_mode']) <= 4)
	        re_insert_rating_account($user['rating_mode'],$user['rating_account'],$billing_account_id,$re_bill_plan_id,$sm_bill_plan_id);
	
	    if(re_get_rating_mode_id($user['rating_mode']) >= 5)
            //re_insert_rating_account_2($user['rating_mode'],$user['rating_account'],$billing_account_id,$re_bill_plan_id,$clg_nadi,$cld_nadi);
            re_insert_rating_account_2($user['rating_mode'],$user['rating_account'],$billing_account_id,$re_bill_plan_id,3,3);
	
	    $racc_check_flag = 1;
	    goto racc_check;
    }

    if(($rating_account_id == 0)&&($racc_check_flag)) {
	    return 0;
    }

    // Insert PCard
    if($billing_account_id) {
	    if(empty($user['start_date'])) $user['start_date'] = date("Y-m-d");
	
	    if(!empty($user['pcard_status'])) $pcard_status_id = re_get_pcard_status_id($user['pcard_status']);
	    else $pcard_status_id = 2;
	
	    if(!empty($user['pcard_type'])) {
	        $pcard_type_id = re_get_pcard_type_id($user['pcard_type']);
	    } else $pcard_type_id = 2;
	
	    if($pcard_type_id) {
	        $pcard_id = re_get_pcard_id_2($billing_account_id);
	        if(($pcard_type_id == 2)AND($pcard_id > 0)) goto chk_call_number;
		
	        if((re_get_pcard_id($billing_account_id,$pcard_type_id,$pcard_status_id,$user['amount'],$user['start_date'],$user['end_date'])) == 0) {
		        re_insert_pcard($billing_account_id,$pcard_type_id,$pcard_status_id,$user['amount'],$user['start_date'],$user['end_date']);
	        } else {
            chk_call_number:
		        $bacc_num = re_get_bacc_num_clg($billing_account_id);
		
		        if($bacc_num > 1) re_update_pcard_call_number($pcard_id,$bacc_num);
	        }
	    }
    }

end_func:
    return array('re_bacc_id' => $billing_account_id , 're_racc_id' => $rating_account_id);
}
?>
