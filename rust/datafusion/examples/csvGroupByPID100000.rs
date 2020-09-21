use arrow::util::pretty;

use std::process;
use std::thread;
use std::time::Duration;

use std::process::Command;

extern crate flame;

use std::fs::File;

use datafusion::datasource::csv::CsvReadOptions;
use datafusion::error::Result;
use datafusion::execution::context::ExecutionContext;

fn main() -> Result<()> {
    /*     SORT NOW     */
    let part = 32;
    println!("\n\n\n\n\n starting sort 8");
    println!("\n\n\n\n\n\n My pid is {}", process::id());

    // let argument = process::id().to_string() + &" --log CPU_Data.txt";

    // cmd.arg(argument);

    let mut cmd = process::Command::new("psrecord")
        .arg(process::id().to_string())
        .arg("--log")
        .arg("CPU_Data.txt")
        // .arg("--plot")
        // .arg("CPU_Plot.png")
        .spawn()
        .expect("Couldn't run 'psrecord'");

    std::thread::sleep(Duration::from_millis(1000));

    let mut ctx = ExecutionContext::new();
    let testdata = "../../testing/data";

    ctx.register_csv(
        "aggregate_test_100000",
        &format!("{}/csv/100000_data.csv", testdata),
        CsvReadOptions::new(),
    )?;

    let sql_sort = "SELECT c1 FROM aggregate_test_100000 GROUP BY c1";
    let plan2 = ctx.create_logical_plan(sql_sort)?;
    let plan2 = ctx.optimize(&plan2)?;
    let plan2 = ctx.create_physical_plan(&plan2, part )?;//* part)?;
    let results = ctx.collect(plan2.as_ref())?;
    // pretty::print_batches(&results)?;
    // std::thread::sleep(Duration::from_millis(10000));

    cmd.kill().expect("!kill");


    flame::dump_html(&mut File::create("flame-graph.html").unwrap()).unwrap();

    // std::thread::sleep(Duration::from_millis(10000));


    Ok(())
}
