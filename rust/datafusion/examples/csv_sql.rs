// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
extern crate cpu_monitor;

use arrow::util::pretty;
//use systemstat::{System, Platform, saturating_sub_bytes};
use std::thread;
use std::time::Duration;

use datafusion::datasource::csv::CsvReadOptions;
use datafusion::error::Result;
use datafusion::execution::context::ExecutionContext;

use sysinfo::{NetworkExt, NetworksExt, ProcessExt, System, SystemExt};
use cpu_monitor::CpuInstant;
/// This example demonstrates executing a simple query against an Arrow data source (CSV) and
/// fetching results
fn main() -> Result<()> {
    // create local execution context
    let mut x = 0;
    let temp = CpuInstant::now()?;
    let mut avg = 0.0;


    while x < 10
    {
        x+=1;
        let mut ctx = ExecutionContext::new();
        let mut sysinfo = System::new_all();

        // Number of processors
        println!("NB processors: {}", sysinfo.get_processors().len());
        //let testdata = std::env::var("ARROW_TEST_DATA").expect("ARROW_TEST_DATA not Defined");
        //let testdata = "/Users/jasonmichaelvallee/CS/data/Arrow/testing/data";
        let testdata = "../../testing/data";
        let start = CpuInstant::now()?;

        // register csv file with the execution context
        ctx.register_csv(
            "aggregate_test_100",
            &format!("{}/csv/aggregate_test_100.csv", testdata),
            CsvReadOptions::new(),
        )?;

        let sql = "SELECT c1, MIN(c12), MAX(c12) FROM aggregate_test_100 WHERE c11 > 0.1 AND c11 < 0.9 GROUP BY c1";

        // create the query plan
        let plan = ctx.create_logical_plan(sql)?;
        let plan = ctx.optimize(&plan)?;
        let plan = ctx.create_physical_plan(&plan, 1024 * 1024)?;

        // execute the query
        let results = ctx.collect(plan.as_ref())?;
        // Number of processors
        // print the results
        //pretty::print_batches(&results)?;
        let end = CpuInstant::now()?;

        let duration = end - start;
        println!("PREV AVG: {}%", avg);
        println!("CPU usage for Query: {}%", duration.non_idle());

        avg += duration.non_idle();
        println!("AVG: {}%", avg);

        println!("CPU usage for Query: {}%", duration.non_idle());
        println!("CPU usage for Query: {:.0}%", duration.non_idle()*100.);


        let start = CpuInstant::now()?;
        std::thread::sleep(Duration::from_millis(100));
        let end = CpuInstant::now()?;
        let duration = end - start;

        println!("CPU usage for sleep thread: {:.0}%", duration.non_idle() * 100.);
    }

    println!("\n\nCPU Average usage: {:.0}%", avg * 10.);
    Ok(())
}
