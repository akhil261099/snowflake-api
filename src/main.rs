use anyhow::Result;
use snowflake_api::{QueryResult, SnowflakeApi};
use std::io::{self, Write};  // For user input
use std::path::Path;
use tokio::main;
use std::time::Instant;

#[tokio::main]  // Tokio runtime entry point
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Prompt the user to choose the operation
    println!("Choose the operation you want to perform:");
    println!("1. PUT Query");
    println!("2. SELECT Query");
    println!("3. INSERT Query");
    println!("4. UPDATE Query");
    println!("5. DELETE Query");

    // Read user choice
    let choice = read_user_input()?;

    match choice.as_str() {
        "1" => {
            let start = Instant::now();
            // PUT query (e.g., file upload)
            let file_path = Path::new(r"E:\rust\snowflake-api\iris_dataset.csv");
            let stage_name = "TRY_STAGE";
            run_query("CREATE STAGE IF NOT EXISTS TRY_STAGE").await?;
            let put_query = format!("PUT file://{} @{}", file_path.display(), stage_name);
            run_query("CREATE OR REPLACE TABLE iris_table (
                SepalLength NUMBER(3,2),
                SepalWidth NUMBER(3,2),
                PetalLength NUMBER(3,2),
                PetalWidth NUMBER(3,2),
                Species STRING
            );").await?;
            run_query("COPY INTO iris_table
                FROM @TRY_STAGE/iris_dataset.csv
                ;").await?;
            let duration = start.elapsed();
            println!("Time taken for uploading csv to table or PUT query is :{:?}", duration);
        }
        "2" => {
            // SELECT query
            println!("Enter your SELECT query:");
            let select_query = read_user_input()?;
            let start = Instant::now();
            if let Err(e) = run_select_query(&select_query).await {
                eprintln!("Error executing SELECT query: {}", e);
            }
            let duration = start.elapsed();
            println!("Time taken for Select query is :{:?}", duration);
        }
        "3" => {
            // INSERT query
            println!("Enter your INSERT query:");
            let insert_query = read_user_input()?;
            if let Err(e) = run_insert_query(&insert_query).await {
                eprintln!("Error executing INSERT query: {}", e);
            }
        }
        "4" => {
            // UPDATE query
            println!("Enter your UPDATE query:");
            let update_query = read_user_input()?;
            if let Err(e) = run_update_query(&update_query).await {
                eprintln!("Error executing UPDATE query: {}", e);
            }
        }
        "5" => {
            // DELETE query
            println!("Enter your DELETE query:");
            let delete_query = read_user_input()?;
            if let Err(e) = run_delete_query(&delete_query).await {
                eprintln!("Error executing DELETE query: {}", e);
            }
        }
        _ => {
            println!("Invalid choice. Please choose a number between 1 and 5.");
        }
    }
    Ok(())
}

// Function to read user input from the terminal
fn read_user_input() -> Result<String> {
    let mut input = String::new();
    io::stdout().flush()?;  // Ensure the prompt is printed before reading input
    io::stdin().read_line(&mut input)?;
    Ok(input.trim().to_string())
}

// Function to execute a generic query
async fn run_query(sql: &str) -> Result<QueryResult> {
    let api = SnowflakeApi::with_password_auth(
        "kcb57939.us-east-1",
        Some("DEMO_SQL_WH"),
        Some("TRAININGDB"),
        Some("SALES"),
        "akhil969",
        Some("ACCOUNTADMIN"),
        "zaq1ZAQ1@1",  // Ideally, credentials should be securely managed
    )?;
    let res = api.exec(sql).await?;
    Ok(res)
}

// Function to run a SELECT query
async fn run_select_query(sql: &str) -> Result<QueryResult> {
    println!("Executing SELECT query: {}", sql);
    run_query(sql).await
}

// Function to run an INSERT query
async fn run_insert_query(sql: &str) -> Result<QueryResult> {
    println!("Executing INSERT query: {}", sql);
    run_query(sql).await
}

// Function to run an UPDATE query
async fn run_update_query(sql: &str) -> Result<QueryResult> {
    println!("Executing UPDATE query: {}", sql);
    run_query(sql).await
}

// Function to run a DELETE query
async fn run_delete_query(sql: &str) -> Result<QueryResult> {
    println!("Executing DELETE query: {}", sql);
    run_query(sql).await
}










// // Function to run a PUT query
// async fn run_put_query(sql: &str) -> Result<QueryResult> {
//     // println!("Executing PUT query: {}", sql);
//     run_query(sql).await

// }