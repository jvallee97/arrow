use arrow::util::pretty;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use chrono::format::ParseError;

use std::process;
use std::thread;
use std::time::Duration;
use std::io::{self, Read};

use std::process::Command;

extern crate flame;

use std::fs::File;

use datafusion::datasource::csv::CsvReadOptions;
use datafusion::error::Result;
use datafusion::execution::context::ExecutionContext;


fn main() -> Result<()> {
    /*     SORT NOW     */

    let now = Utc::now();
    let part = 32;
    println!("\n\n\n\n\n starting sort");
    println!("\n\n\n\n\n\n My pid is {}", process::id());


    let mut date_str = String::new();
    let mut end = String::new();
    //let date_str = "2020-04-12 22:10:57 +00:00";
    println!("Enter start of busy");
    //let mut date_str = "2020-04-12 22:10:57 +00:00";
    io::stdin().read_line(&mut date_str)?;
    date_str = date_str.replace('\n', "");
    let start = DateTime::parse_from_str(&date_str, "%Y-%m-%d %H:%M:%S %z").unwrap();



    println!("Enter end of Busy");
    io::stdin().read_line(&mut end)?;
    //let mut end = "2020-04-12 22:10:57 +00:00";
    end = end.replace('\n', "");

    let end =  DateTime::parse_from_str(&end, "%Y-%m-%d %H:%M:%S %z").unwrap();

    if end > start {
        println!("working");
    }

    println!("enter query");
    //let mut query = "SELECT c1 FROM aggregate_test_10000 GROUP BY c1";
    let mut query = String::new();
    io::stdin().read_line(&mut query)?;
    let query = query.trim();

    println!("enter cpuRate");
    //let mut query = "SELECT c1 FROM aggregate_test_10000 GROUP BY c1";
    let mut cpuRate = String::new();
    io::stdin().read_line(&mut cpuRate)?;
    let cpuRate = cpuRate.trim();


    while true{
        println!("\n\nin loop");
        let now = Utc::now();
        if now < end && now > start{
            println!("within no work interval");
            println!("time is {}, can work at {}", now, end);
            std::thread::sleep(Duration::from_millis(5000));
        }
        else{
            println!("time is {}, can work at {}", now, end);
            println!("in else/can work \n\n");
            break;
        }
    }

    println!("after while");


    let mut cmd = process::Command::new("cpulimit")
        .arg("--limit")
        .arg(cpuRate.to_string())
        //.arg("3")
        .arg("-v")
        .arg("--pid")
        .arg(process::id().to_string())        // .arg("CPU_Plot.png")
        .spawn()
        .expect("Couldn't run 'cpulimit'");


    println!("about to sleep");
    //std::thread::sleep(Duration::from_millis(1000));
    println!("after to sleep");

    let mut ctx = ExecutionContext::new();
    let testdata = "/Users/jasonmichaelvallee/CS/data/Arrow/testing/data/csv/1000_data.csv";

    ctx.register_csv(
        "aggregate_test_10000",
        &format!("{}", testdata),
        CsvReadOptions::new(),
    )?;

    //let sql_sort = "SELECT c1 FROM aggregate_test_10000 GROUP BY c1";

    let sql_sort = query.to_string();//"SELECT c1 FROM aggregate_test_100000 GROUP BY c1";
    let plan2 = ctx.create_logical_plan(&sql_sort)?;
    let plan2 = ctx.optimize(&plan2)?;
    let plan2 = ctx.create_physical_plan(&plan2, part )?;//* part)?;
    let results = ctx.collect(plan2.as_ref())?;
    pretty::print_batches(&results)?;
    // std::thread::sleep(Duration::from_millis(10000));

    cmd.kill().expect("!kill");

    println!("end");

    Ok(())
}
