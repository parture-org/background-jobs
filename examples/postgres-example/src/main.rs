use background_jobs::postgres::Storage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage =
        Storage::connect("postgres://postgres:postgres@localhost:5432/db".parse()?).await?;
    println!("Hello, world!");
    Ok(())
}
