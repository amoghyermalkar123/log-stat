mod logstat;
use std::time::Duration;
use clap::Parser;
use logstat::logstat;
use duration_string::DurationString;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    pattern: String,

    #[arg(short, long)]
    server_to_watch: String,

    #[arg(short, long)]
    kafka_host_port: String,

    #[arg(short, long)]
    kafka_topic: String,

    #[arg(short, long)]
    session_duration: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let session_duration: Duration = DurationString::from_string(String::from(args.session_duration)).unwrap().into();

    logstat(
        args.kafka_host_port,
        args.kafka_topic,
        args.pattern,
        args.server_to_watch,
        session_duration
    )?;
    
    Ok(())
}
