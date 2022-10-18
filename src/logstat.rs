use csv::Writer;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use std::time::{Instant, Duration};
use std::{collections::HashMap};

pub fn logstat(
    kafka_host_port: String,
    kafka_topic: String,
    pattern: String,
    server_to_watch: String,
    session_duration: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut consumer = Consumer::from_hosts(vec![kafka_host_port.to_owned()])
        .with_topic_partitions(kafka_topic.to_owned(), &[0, 1])
        .with_fallback_offset(FetchOffset::Latest)
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .create()
        .unwrap();

    let mut latency_table: HashMap<String, (String, u64)> = HashMap::new();
    let now = Instant::now();

    while now.elapsed() <= session_duration {
        for ms in consumer.poll().unwrap().iter() {
            for m in ms.messages() {
                let log = String::from_utf8_lossy(m.value);
                if log.contains(pattern.as_str()) && log.contains(server_to_watch.as_str()) {
                    let mut split = log.split(" ");
                    let mut latency = "";
                    let mut uri = "";
                    if let Some(l) = split.nth(3) {
                        latency = l;
                    }
                    if let Some(u) = split.nth(8) {
                        uri = u;
                    }
                    let parts = uri.split(".");

                    let last_part = parts.last().unwrap();
                    let latency_encountered: u64 = latency.parse()?;


                    let default_count: u64 = 1;
                    let default_lat = String::from("0");
                    let tup = &(default_lat, default_count);

                    let (latency_till_now, count) = latency_table.get(last_part).unwrap_or(tup);
                    let latency_till_now : u64 = latency_till_now.parse().unwrap_or(0);

                    let average_latency = latency_till_now + latency_encountered / count;
                    consumer.commit_consumed().unwrap();
                    latency_table.insert(last_part.to_owned(), (average_latency.to_string(), count + 1));
                }
            }
        }
    }
    println!("Table {:?}", latency_table);
    write_to_csv(&mut latency_table)?;
    Ok(())
}

fn write_to_csv(
    latency_table: &mut HashMap<String, (String, u64)>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut wtr = Writer::from_path("foo.csv")?;
    wtr.write_record(&["Content Type", "Average Latency", "Logs encountered"])?;

    for (k, v) in latency_table {
        wtr.write_record([k.as_bytes(), v.0.as_bytes(), v.1.to_string().as_bytes()])?;
    }
    wtr.flush()?;

    Ok(())
}
